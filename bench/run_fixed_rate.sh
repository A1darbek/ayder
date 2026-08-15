#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WRK2_BIN="${WRK2_BIN:-wrk2}"
WRK2_LUA_PATH="${WRK2_LUA_PATH:-}"
BASE_URL="${BASE_URL:-http://127.0.0.1:1109}"
TOKEN="${TOKEN:-dev}"
TOPIC="${TOPIC:-rrv1_$(date -u +%Y%m%dT%H%M%S)}"
PARTITION="${PARTITION:-0}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-128}"
RATE="${RATE:-10000}"
THREADS="${THREADS:-4}"
CONNECTIONS="${CONNECTIONS:-128}"
WARMUP_SECONDS="${WARMUP_SECONDS:-10}"
DURATION_SECONDS="${DURATION_SECONDS:-20}"
TRIALS="${TRIALS:-3}"
TIMEOUT_MS="${TIMEOUT_MS:-5000}"
CLIENT_CPUSET="${CLIENT_CPUSET:-}"
OUTPUT_DIR="${OUTPUT_DIR:-${ROOT_DIR}/bench/results/$(date -u +%Y%m%dT%H%M%SZ)}"
SERVER_GIT_SHA="${SERVER_GIT_SHA:-$(git -C "$ROOT_DIR" rev-parse HEAD)}"
WORKTREE_DIRTY="$(git -C "$ROOT_DIR" status --porcelain --untracked-files=normal | grep -q . && printf true || printf false)"
TRACKED_PATCH_SHA256="$(git -C "$ROOT_DIR" diff --binary HEAD | sha256sum | awk '{print $1}')"
UNTRACKED_MANIFEST_SHA256="$({
  git -C "$ROOT_DIR" ls-files --others --exclude-standard -z \
    | LC_ALL=C sort -z \
    | while IFS= read -r -d '' path; do
        printf '%s  %s\n' "$(sha256sum "$ROOT_DIR/$path" | awk '{print $1}')" "$path"
      done
} | sha256sum | awk '{print $1}')"
SERVER_COMMAND="${SERVER_COMMAND:-not-recorded}"
SERVER_BINARY_SHA256="${SERVER_BINARY_SHA256:-not-recorded}"
SERVER_CPUSET="${SERVER_CPUSET:-not-recorded}"
SERVER_PIDS="${SERVER_PIDS:-}"
SERVER_TOPOLOGY="${SERVER_TOPOLOGY:-single-node, client and server colocated}"
FSYNC_SEMANTICS="${FSYNC_SEMANTICS:-local group commit; successful sample requires explicit local_fsync=true}"
LUA_SCRIPT="${LUA_SCRIPT:-${ROOT_DIR}/bench/lua/produce_durable.lua}"
WORKLOAD_NAME="${WORKLOAD_NAME:-durable_produce}"
DURABILITY_CONTRACT="${DURABILITY_CONTRACT:-local scope: ok/durable/synced/local_fsync/applied_all_workers true; quorum_committed/failover_safe false}"
CREATE_TOPIC="${CREATE_TOPIC:-1}"

if [[ -z "${PLATFORM_LABEL:-}" ]]; then
  platform_arch="$(uname -m)"
  if uname -r | grep -qi microsoft; then
    PLATFORM_LABEL="${platform_arch} WSL2 development benchmark"
  else
    PLATFORM_LABEL="${platform_arch} native Linux development benchmark"
  fi
fi

for tool in curl jq python3 sha256sum; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    printf 'missing required tool: %s\n' "$tool" >&2
    exit 127
  fi
done

if [[ "$WRK2_BIN" == */* ]]; then
  if [[ ! -x "$WRK2_BIN" ]]; then
    printf 'WRK2_BIN is not executable: %s\n' "$WRK2_BIN" >&2
    exit 127
  fi
elif ! command -v "$WRK2_BIN" >/dev/null 2>&1; then
  printf 'missing required fixed-rate client: %s\n' "$WRK2_BIN" >&2
  exit 127
fi
wrk2_resolved="$(command -v "$WRK2_BIN" 2>/dev/null || printf '%s' "$WRK2_BIN")"
wrk2_sha256="$(sha256sum "$wrk2_resolved" | awk '{print $1}')"

if ! [[ "$TRIALS" =~ ^[1-9][0-9]*$ ]]; then
  printf 'TRIALS must be a positive integer\n' >&2
  exit 2
fi

mkdir -p "$OUTPUT_DIR"
if [[ ! -r "$LUA_SCRIPT" ]]; then
  printf 'Lua workload is not readable: %s\n' "$LUA_SCRIPT" >&2
  exit 2
fi

ready_body="$(curl -fsS --max-time 3 "${BASE_URL}/ready")" || {
  printf 'server readiness probe failed: %s/ready\n' "$BASE_URL" >&2
  exit 1
}
if ! jq -e '.ready == true' >/dev/null <<<"$ready_body"; then
  printf 'server is not persistence-ready: %s\n' "$ready_body" >&2
  exit 1
fi

auth_args=()
if [[ -n "$TOKEN" ]]; then
  auth_args=(-H "Authorization: Bearer ${TOKEN}")
fi

if [[ "$CREATE_TOPIC" == "1" ]]; then
  curl -fsS -X POST "${BASE_URL}/broker/topics" \
    "${auth_args[@]}" \
    -H 'Content-Type: application/json' \
    --data-binary "{\"name\":\"${TOPIC}\",\"partitions\":1}" \
    | jq -e '.ok == true' >/dev/null
fi

wrk_cmd=("$WRK2_BIN" -t"$THREADS" -c"$CONNECTIONS" -R"$RATE" --latency \
  -s "$LUA_SCRIPT")
if [[ -n "$CLIENT_CPUSET" ]]; then
  wrk_cmd=(taskset -c "$CLIENT_CPUSET" "${wrk_cmd[@]}")
fi

run_once() {
  local duration="$1"
  local output="$2"
  local sampler_pid=""
  local rc=0
  if [[ -n "$SERVER_PIDS" ]]; then
    "${ROOT_DIR}/bench/sample_system.sh" "$SERVER_PIDS" >"${output%.txt}.system.tsv" &
    sampler_pid="$!"
  fi
  set +e
  if [[ -n "$WRK2_LUA_PATH" ]]; then
    LUA_PATH="$WRK2_LUA_PATH" TOPIC="$TOPIC" PARTITION="$PARTITION" \
      PAYLOAD_BYTES="$PAYLOAD_BYTES" TIMEOUT_MS="$TIMEOUT_MS" TOKEN="$TOKEN" \
      "${wrk_cmd[@]}" -d"${duration}s" "${BASE_URL}" >"$output" 2>&1
  else
    TOPIC="$TOPIC" PARTITION="$PARTITION" PAYLOAD_BYTES="$PAYLOAD_BYTES" \
      TIMEOUT_MS="$TIMEOUT_MS" TOKEN="$TOKEN" \
      "${wrk_cmd[@]}" -d"${duration}s" "${BASE_URL}" >"$output" 2>&1
  fi
  rc=$?
  set -e
  if [[ -n "$sampler_pid" ]]; then
    kill "$sampler_pid" 2>/dev/null || true
    wait "$sampler_pid" 2>/dev/null || true
  fi
  return "$rc"
}

if (( WARMUP_SECONDS > 0 )); then
  run_once "$WARMUP_SECONDS" "${OUTPUT_DIR}/warmup.txt"
fi

for trial in $(seq 1 "$TRIALS"); do
  run_once "$DURATION_SECONDS" "${OUTPUT_DIR}/trial-${trial}.txt"
done

jq -n \
  --arg platform_label "$PLATFORM_LABEL" \
  --arg timestamp_utc "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg harness_git_sha "$(git -C "$ROOT_DIR" rev-parse HEAD)" \
  --arg server_git_sha "$SERVER_GIT_SHA" \
  --argjson worktree_dirty "$WORKTREE_DIRTY" \
  --arg tracked_patch_sha256 "$TRACKED_PATCH_SHA256" \
  --arg untracked_manifest_sha256 "$UNTRACKED_MANIFEST_SHA256" \
  --arg base_url "$BASE_URL" \
  --arg topic "$TOPIC" \
  --arg workload_name "$WORKLOAD_NAME" \
  --arg lua_script "$LUA_SCRIPT" \
  --arg durability_contract "$DURABILITY_CONTRACT" \
  --arg fsync_semantics "$FSYNC_SEMANTICS" \
  --arg client_cpuset "$CLIENT_CPUSET" \
  --arg server_cpuset "$SERVER_CPUSET" \
  --arg server_pids "$SERVER_PIDS" \
  --arg server_command "$SERVER_COMMAND" \
  --arg server_binary_sha256 "$SERVER_BINARY_SHA256" \
  --arg server_topology "$SERVER_TOPOLOGY" \
  --arg wrk2_bin "$WRK2_BIN" \
  --arg wrk2_sha256 "$wrk2_sha256" \
  --arg wrk2_lua_path "$WRK2_LUA_PATH" \
  --argjson payload_bytes "$PAYLOAD_BYTES" \
  --argjson offered_rps "$RATE" \
  --argjson threads "$THREADS" \
  --argjson connections "$CONNECTIONS" \
  --argjson warmup_seconds "$WARMUP_SECONDS" \
  --argjson trial_seconds "$DURATION_SECONDS" \
  --argjson trials "$TRIALS" \
  --argjson timeout_ms "$TIMEOUT_MS" \
  '{platform_label:$platform_label,timestamp_utc:$timestamp_utc,harness_git_sha:$harness_git_sha,
    server_git_sha:$server_git_sha,worktree_dirty:$worktree_dirty,tracked_patch_sha256:$tracked_patch_sha256,
    untracked_manifest_sha256:$untracked_manifest_sha256,base_url:$base_url,topic:$topic,workload_name:$workload_name,lua_script:$lua_script,payload_bytes:$payload_bytes,
    offered_rps:$offered_rps,threads:$threads,connections:$connections,warmup_seconds:$warmup_seconds,
    trial_seconds:$trial_seconds,trials:$trials,timeout_ms:$timeout_ms,
    durability_contract:$durability_contract,fsync_semantics:$fsync_semantics,
    client_cpuset:$client_cpuset,server_cpuset:$server_cpuset,server_pids:$server_pids,server_command:$server_command,
    server_binary_sha256:$server_binary_sha256,
    server_topology:$server_topology,wrk2_bin:$wrk2_bin,wrk2_sha256:$wrk2_sha256,
    wrk2_lua_path:$wrk2_lua_path}' >"${OUTPUT_DIR}/metadata.json"
"${ROOT_DIR}/bench/capture_environment.sh" >"${OUTPUT_DIR}/environment.txt" 2>&1
python3 "${ROOT_DIR}/bench/parse_wrk.py" "$OUTPUT_DIR"

printf 'results: %s\n' "$OUTPUT_DIR"
grep -H -E 'Requests/sec:|Latency Distribution|50\.000%|95\.000%|99\.000%|99\.900%|100\.000%|AYDER_VALIDATION|Socket errors|Non-2xx' \
  "${OUTPUT_DIR}"/trial-*.txt || true
