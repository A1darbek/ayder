#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OLD_BIN="${OLD_BIN:-${ROOT_DIR}/ayder}"
NEW_BIN="${NEW_BIN:-${ROOT_DIR}/ayder}"
OUTPUT_DIR="${OUTPUT_DIR:-${ROOT_DIR}/bench/results/upgrade-rollback-$(date -u +%Y%m%dT%H%M%SZ)}"
PORT="${PORT:-$((39000 + ($$ % 12000)))}"
TOKEN="${TOKEN:-ayder-upgrade-rollback-drill}"
BASE_URL="http://127.0.0.1:${PORT}"
TOPIC="upgrade_rollback_$(date -u +%Y%m%dT%H%M%S)"

for tool in curl jq realpath setsid sha256sum; do
  command -v "$tool" >/dev/null 2>&1 || {
    printf 'missing required tool: %s\n' "$tool" >&2
    exit 127
  }
done
OLD_BIN="$(realpath -e -- "$OLD_BIN")"
NEW_BIN="$(realpath -e -- "$NEW_BIN")"
[[ -x "$OLD_BIN" && -x "$NEW_BIN" ]] || {
  printf 'OLD_BIN and NEW_BIN must be executable\n' >&2
  exit 2
}
[[ ! -e "$OUTPUT_DIR" ]] || {
  printf 'OUTPUT_DIR already exists: %s\n' "$OUTPUT_DIR" >&2
  exit 2
}

umask 077
mkdir -p "$OUTPUT_DIR/original"
server_pid=""
start_count=0
last_ready_ns=0
last_start_ns=0

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

cleanup() {
  stop_server
  rm -f -- /dev/shm/ayder_upgrade_storage_$$_* \
            /dev/shm/ayder_upgrade_metrics_$$_*
}
trap cleanup EXIT

start_server() {
  local binary="$1"
  local data_dir="$2"
  local label="$3"
  start_count=$((start_count + 1))
  last_start_ns="$(date +%s%N)"
  (
    cd "$data_dir"
    exec setsid env \
      RF_BEARER_TOKENS="${TOKEN}@1000000:1000000:1000000000" \
      RF_HTTP_DISABLE_RL=1 \
      AYDER_STORAGE_SHM_NAME="/ayder_upgrade_storage_$$_${start_count}" \
      AYDER_METRICS_SHM_NAME="/ayder_upgrade_metrics_$$_${start_count}" \
      "$binary" --port "$PORT" --workers 0
  ) >>"$OUTPUT_DIR/${label}.log" 2>&1 &
  server_pid="$!"
  for _ in $(seq 1 300); do
    if curl -fsS --max-time 1 "$BASE_URL/ready" 2>/dev/null |
       jq -e '.ready == true' >/dev/null 2>&1; then
      last_ready_ns="$(date +%s%N)"
      return 0
    fi
    kill -0 "$server_pid" 2>/dev/null || break
    sleep 0.05
  done
  printf '%s server did not become ready\n' "$label" >&2
  tail -n 100 "$OUTPUT_DIR/${label}.log" >&2 || true
  return 1
}

auth=(-H "Authorization: Bearer ${TOKEN}")

start_server "$OLD_BIN" "$OUTPUT_DIR/original" old
old_ready_ms=$(((last_ready_ns - last_start_ns) / 1000000))
curl -fsS -X POST "$BASE_URL/broker/topics" "${auth[@]}" \
  -H 'Content-Type: application/json' \
  --data-binary "{\"name\":\"${TOPIC}\",\"partitions\":1}" |
  jq -e '.ok == true' >/dev/null
before="$(curl -fsS "$BASE_URL/broker/topics/${TOPIC}/produce?partition=0&timeout_ms=5000" \
  "${auth[@]}" -H 'Content-Type: text/plain' --data-binary before-upgrade)"
jq -e '.ok == true and .durable == true and .synced == true' \
  >/dev/null <<<"$before"
stop_server

backup_begin_ns="$(date +%s%N)"
"$ROOT_DIR/scripts/operations/cold_backup.sh" \
  "$OUTPUT_DIR/original" "$OUTPUT_DIR/backup" "$OLD_BIN" \
  >"$OUTPUT_DIR/backup.log"
backup_end_ns="$(date +%s%N)"

start_server "$NEW_BIN" "$OUTPUT_DIR/original" new
upgrade_ready_ms=$(((last_ready_ns - last_start_ns) / 1000000))
consume_before="$(curl -fsS \
  "$BASE_URL/broker/consume/${TOPIC}/verify/0?offset=0&limit=20&encoding=b64" \
  "${auth[@]}")"
jq -e '[.messages[]?.value_b64] | index("YmVmb3JlLXVwZ3JhZGU=") != null' \
  >/dev/null <<<"$consume_before"
after="$(curl -fsS "$BASE_URL/broker/topics/${TOPIC}/produce?partition=0&timeout_ms=5000" \
  "${auth[@]}" -H 'Content-Type: text/plain' --data-binary after-upgrade)"
jq -e '.ok == true and .durable == true and .synced == true' \
  >/dev/null <<<"$after"
stop_server

restore_begin_ns="$(date +%s%N)"
"$ROOT_DIR/scripts/operations/cold_restore.sh" \
  "$OUTPUT_DIR/backup" "$OUTPUT_DIR/rollback" \
  >"$OUTPUT_DIR/restore.json"
restore_end_ns="$(date +%s%N)"

start_server "$OLD_BIN" "$OUTPUT_DIR/rollback" rollback
rollback_ready_ms=$(((last_ready_ns - last_start_ns) / 1000000))
consume_rollback="$(curl -fsS \
  "$BASE_URL/broker/consume/${TOPIC}/verify/0?offset=0&limit=20&encoding=b64" \
  "${auth[@]}")"
jq -e '
  ([.messages[]?.value_b64] | index("YmVmb3JlLXVwZ3JhZGU=") != null) and
  ([.messages[]?.value_b64] | index("YWZ0ZXItdXBncmFkZQ==") == null)
' >/dev/null <<<"$consume_rollback"
stop_server

old_sha="$(sha256sum "$OLD_BIN" | awk '{print $1}')"
new_sha="$(sha256sum "$NEW_BIN" | awk '{print $1}')"
jq -n \
  --arg timestamp_utc "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg git_sha "$(git -C "$ROOT_DIR" rev-parse HEAD)" \
  --arg old_binary "$OLD_BIN" \
  --arg new_binary "$NEW_BIN" \
  --arg old_sha256 "$old_sha" \
  --arg new_sha256 "$new_sha" \
  --arg topic "$TOPIC" \
  --argjson old_ready_ms "$old_ready_ms" \
  --argjson upgrade_ready_ms "$upgrade_ready_ms" \
  --argjson rollback_ready_ms "$rollback_ready_ms" \
  --argjson backup_ms "$(((backup_end_ns - backup_begin_ns) / 1000000))" \
  --argjson restore_ms "$(((restore_end_ns - restore_begin_ns) / 1000000))" \
  '{schema_version:1,timestamp_utc:$timestamp_utc,git_sha:$git_sha,
    old_binary:$old_binary,new_binary:$new_binary,
    old_sha256:$old_sha256,new_sha256:$new_sha256,
    version_change:($old_sha256 != $new_sha256),topic:$topic,
    old_ready_ms:$old_ready_ms,upgrade_ready_ms:$upgrade_ready_ms,
    rollback_ready_ms:$rollback_ready_ms,backup_ms:$backup_ms,
    restore_ms:$restore_ms,
    rollback_method:"restore the pre-upgrade cold backup; no in-place downgrade",
    before_upgrade_present_after_upgrade:true,
    before_upgrade_present_after_rollback:true,
    post_upgrade_write_absent_after_rollback:true,pass:true}' \
  >"$OUTPUT_DIR/result.json"

jq -e '.pass == true' "$OUTPUT_DIR/result.json"
printf 'upgrade/rollback drill result: %s\n' "$OUTPUT_DIR/result.json"
