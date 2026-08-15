#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER_BIN="${SERVER_BIN:-${ROOT_DIR}/ayder}"
PORT="${PORT:-$((20000 + ($$ % 18000)))}"
TOKEN="${TOKEN:-rrv1-local-crash-token}"
RECORDS="${RECORDS:-100}"
PILOT_RPO_TARGET_RECORDS="${PILOT_RPO_TARGET_RECORDS:-0}"
PILOT_RTO_TARGET_MS="${PILOT_RTO_TARGET_MS:-5000}"
OUTPUT_DIR="${OUTPUT_DIR:-${ROOT_DIR}/bench/results/crash-$(date -u +%Y%m%dT%H%M%SZ)}"
BASE_URL="http://127.0.0.1:${PORT}"
TOPIC="rrv1_crash_$(date -u +%Y%m%dT%H%M%S)"
DATA_DIR="${OUTPUT_DIR}/data"

for tool in curl findmnt jq realpath setsid sha256sum sort comm; do
  command -v "$tool" >/dev/null 2>&1 || {
    printf 'missing tool: %s\n' "$tool" >&2
    exit 127
  }
done
SERVER_BIN="$(realpath -e -- "$SERVER_BIN")"
[[ -x "$SERVER_BIN" ]] || {
  printf 'SERVER_BIN is not executable: %s\n' "$SERVER_BIN" >&2
  exit 2
}
[[ "$RECORDS" =~ ^[1-9][0-9]*$ ]] || {
  printf 'RECORDS must be positive\n' >&2
  exit 2
}
[[ "$PILOT_RPO_TARGET_RECORDS" =~ ^[0-9]+$ ]] || {
  printf 'PILOT_RPO_TARGET_RECORDS must be a non-negative integer\n' >&2
  exit 2
}
[[ "$PILOT_RTO_TARGET_MS" =~ ^[1-9][0-9]*$ ]] || {
  printf 'PILOT_RTO_TARGET_MS must be a positive integer\n' >&2
  exit 2
}
[[ ! -e "$OUTPUT_DIR" ]] || {
  printf 'OUTPUT_DIR already exists: %s\n' "$OUTPUT_DIR" >&2
  exit 2
}

umask 077
mkdir -p "$DATA_DIR"
server_pid=""
start_count=0
last_start_ns=0
last_ready_ns=0

stop_server() {
  [[ -n "$server_pid" ]] || return 0
  if kill -0 "$server_pid" 2>/dev/null; then
    kill -TERM -- "-$server_pid" 2>/dev/null || true
    for _ in $(seq 1 100); do
      kill -0 "$server_pid" 2>/dev/null || break
      sleep 0.05
    done
    kill -KILL -- "-$server_pid" 2>/dev/null || true
  fi
  wait "$server_pid" 2>/dev/null || true
  server_pid=""
}

crash_server() {
  [[ -n "$server_pid" ]] || return 1
  kill -KILL -- "-$server_pid" 2>/dev/null || true
  wait "$server_pid" 2>/dev/null || true
  server_pid=""
}

cleanup() {
  stop_server
  rm -f -- /dev/shm/ayder_crash_storage_$$_* \
            /dev/shm/ayder_crash_metrics_$$_*
}
trap cleanup EXIT

start_server() {
  start_count=$((start_count + 1))
  last_start_ns="$(date +%s%N)"
  (
    cd "$DATA_DIR"
    exec setsid env \
      RF_BEARER_TOKENS="${TOKEN}@1000000:1000000:1000000000" \
      RF_HTTP_DISABLE_RL=1 \
      AYDER_STORAGE_SHM_NAME="/ayder_crash_storage_$$_${start_count}" \
      AYDER_METRICS_SHM_NAME="/ayder_crash_metrics_$$_${start_count}" \
      "$SERVER_BIN" --port "$PORT" --workers 1 --aof always
  ) >>"${OUTPUT_DIR}/server.log" 2>&1 &
  server_pid="$!"
  for _ in $(seq 1 200); do
    if curl -fsS --max-time 1 "${BASE_URL}/ready" 2>/dev/null |
       jq -e '.ready == true' >/dev/null 2>&1; then
      last_ready_ns="$(date +%s%N)"
      return 0
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
      break
    fi
    sleep 0.05
  done
  printf 'server did not become ready; see %s/server.log\n' "$OUTPUT_DIR" >&2
  return 1
}

auth=(-H "Authorization: Bearer ${TOKEN}")
start_server
curl -fsS -X POST "${BASE_URL}/broker/topics" "${auth[@]}" \
  -H 'Content-Type: application/json' \
  --data-binary "{\"name\":\"${TOPIC}\",\"partitions\":1}" |
  jq -e '.ok == true and .partitions == 1' >/dev/null

: >"${OUTPUT_DIR}/acknowledged.jsonl"
: >"${OUTPUT_DIR}/expected_ids.txt"
for i in $(seq 1 "$RECORDS"); do
  id="record-${i}"
  response="$(curl -fsS -X POST \
    "${BASE_URL}/broker/topics/${TOPIC}/produce?partition=0&timeout_ms=5000&idempotency_key=${id}" \
    "${auth[@]}" -H 'Content-Type: application/json' \
    --data-binary "{\"id\":\"${id}\"}")"
  jq -e '.ok == true and .durability_scope == "local" and
         .local_fsync == true and
         .quorum_committed == false and .failover_safe == false and
         .applied_all_workers == true and .duplicate == false' \
    >/dev/null <<<"$response"
  jq -c --arg id "$id" \
    '{id:$id,offset,batch_id,durability_scope,local_fsync,
      quorum_committed,failover_safe,applied_all_workers}' \
    <<<"$response" >>"${OUTPUT_DIR}/acknowledged.jsonl"
  printf '%s\n' "$id" >>"${OUTPUT_DIR}/expected_ids.txt"
done

fault_ns="$(date +%s%N)"
crash_server
start_server
restart_to_ready_ms=$(((last_ready_ns - last_start_ns) / 1000000))
fault_to_ready_ms=$(((last_ready_ns - fault_ns) / 1000000))

curl -fsS --get "${BASE_URL}/broker/consume/${TOPIC}/rrv1_verify/0" \
  "${auth[@]}" --data-urlencode 'offset=0' --data-urlencode "limit=${RECORDS}" \
  >"${OUTPUT_DIR}/recovered.json"
jq -r '.messages[].value | fromjson | .id' "${OUTPUT_DIR}/recovered.json" |
  sort >"${OUTPUT_DIR}/recovered_ids.txt"
sort "${OUTPUT_DIR}/expected_ids.txt" >"${OUTPUT_DIR}/expected_ids.sorted.txt"
comm -23 "${OUTPUT_DIR}/expected_ids.sorted.txt" \
  "${OUTPUT_DIR}/recovered_ids.txt" >"${OUTPUT_DIR}/lost_ids.txt"

lost="$(wc -l <"${OUTPUT_DIR}/lost_ids.txt")"
recovered="$(wc -l <"${OUTPUT_DIR}/recovered_ids.txt")"
unique="$(sort -u "${OUTPUT_DIR}/recovered_ids.txt" | wc -l)"
duplicates=$((recovered - unique))
fs_type="$(findmnt -n -o FSTYPE -T "$DATA_DIR" 2>/dev/null || printf unknown)"
fs_source="$(findmnt -n -o SOURCE -T "$DATA_DIR" 2>/dev/null || printf unknown)"

jq -n \
  --arg timestamp_utc "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg git_sha "$(git -C "$ROOT_DIR" rev-parse HEAD)" \
  --argjson worktree_dirty "$(git -C "$ROOT_DIR" status --porcelain --untracked-files=normal | grep -q . && printf true || printf false)" \
  --arg tracked_patch_sha256 "$(git -C "$ROOT_DIR" diff --binary HEAD | sha256sum | awk '{print $1}')" \
  --arg binary_path "$SERVER_BIN" \
  --arg binary_sha256 "$(sha256sum "$SERVER_BIN" | awk '{print $1}')" \
  --arg platform_label "$(uname -m) $(uname -r | grep -qi microsoft && printf 'WSL2 development' || printf 'native Linux')" \
  --arg filesystem_type "$fs_type" \
  --arg filesystem_source "$fs_source" \
  --arg topic "$TOPIC" \
  --argjson acknowledged "$RECORDS" \
  --argjson recovered "$recovered" \
  --argjson lost "$lost" \
  --argjson duplicates "$duplicates" \
  --argjson restart_to_ready_ms "$restart_to_ready_ms" \
  --argjson fault_to_ready_ms "$fault_to_ready_ms" \
  --argjson rpo_target_records "$PILOT_RPO_TARGET_RECORDS" \
  --argjson rto_target_ms "$PILOT_RTO_TARGET_MS" \
  '{schema_version:1,timestamp_utc:$timestamp_utc,git_sha:$git_sha,
    worktree_dirty:$worktree_dirty,tracked_patch_sha256:$tracked_patch_sha256,
    binary_path:$binary_path,binary_sha256:$binary_sha256,
    platform_label:$platform_label,filesystem_type:$filesystem_type,
    filesystem_source:$filesystem_source,topic:$topic,
    durability_contract:"local fsync + all local workers; no quorum/failover claim",
    acknowledged:$acknowledged,recovered:$recovered,
    observed_rpo_records:$lost,duplicates:$duplicates,
    restart_to_ready_ms:$restart_to_ready_ms,
    fault_to_ready_ms:$fault_to_ready_ms,
    pilot_target:{rpo_records:$rpo_target_records,rto_ms:$rto_target_ms},
    target_met:($lost <= $rpo_target_records and
                $fault_to_ready_ms <= $rto_target_ms),
    pass:($lost==0 and $duplicates==0 and $recovered==$acknowledged and
          $lost <= $rpo_target_records and $fault_to_ready_ms <= $rto_target_ms)}' \
  >"${OUTPUT_DIR}/result.json"

jq -e '.pass == true' "${OUTPUT_DIR}/result.json"
printf 'crash/restart result: %s\n' "$OUTPUT_DIR/result.json"
