#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AYDER_BIN="${AYDER_BIN:-${ROOT_DIR}/ayder}"
PORT="${AYDER_IDEMP_PORT:-$((21000 + ($$ % 18000)))}"
TOKEN="idempotency-regression-token"
RUN_DIR="$(mktemp -d /tmp/ayder-idempotency-regression.XXXXXX)"
SERVER_PID=""
START_COUNT=0

stop_server() {
  local signal="${1:-TERM}"
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill -"${signal}" -- -"${SERVER_PID}" 2>/dev/null || true
    for _ in $(seq 1 50); do
      kill -0 "${SERVER_PID}" 2>/dev/null || break
      sleep 0.1
    done
    kill -KILL -- -"${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  SERVER_PID=""
}

cleanup() {
  stop_server TERM
  rm -f -- /dev/shm/ayder_idempotency_storage_$$_* \
            /dev/shm/ayder_idempotency_metrics_$$_*
  rm -rf -- "${RUN_DIR}"
}
trap cleanup EXIT

for tool in curl jq realpath setsid; do
  command -v "${tool}" >/dev/null 2>&1 || {
    printf 'missing required tool: %s\n' "${tool}" >&2
    exit 2
  }
done
[[ -x "${AYDER_BIN}" ]] || {
  printf 'Ayder binary not found: %s\n' "${AYDER_BIN}" >&2
  exit 2
}
AYDER_BIN="$(realpath -e -- "${AYDER_BIN}")"

start_server() {
  local fail_committed_store="${1:-0}"
  START_COUNT=$((START_COUNT + 1))
  local storage_name="/ayder_idempotency_storage_$$_${START_COUNT}"
  local metrics_name="/ayder_idempotency_metrics_$$_${START_COUNT}"
  local -a fault_env=()
  if [[ "${fail_committed_store}" == 1 ]]; then
    fault_env=(RF_TEST_FAULTS=1 RF_TEST_IDEMP_FAIL_COMMITTED_STORE=1)
  fi
  (
    cd "${RUN_DIR}"
    exec setsid env \
      RF_BEARER_TOKENS="${TOKEN}" \
      RF_HTTP_DISABLE_RL=1 \
      RF_SHARED_ENTRIES=65536 \
      AYDER_STORAGE_SHM_NAME="${storage_name}" \
      AYDER_METRICS_SHM_NAME="${metrics_name}" \
      "${fault_env[@]}" \
      "${AYDER_BIN}" --workers 4 --aof always --port "${PORT}"
  ) >"${RUN_DIR}/server.log" 2>&1 &
  SERVER_PID=$!

  for _ in $(seq 1 200); do
    local body
    body="$(curl -fsS --max-time 1 "http://127.0.0.1:${PORT}/ready" 2>/dev/null || true)"
    [[ "${body}" == '{"ready":true}' ]] && return 0
    sleep 0.05
  done
  tail -n 100 "${RUN_DIR}/server.log" >&2
  return 1
}

auth=(-H "Authorization: Bearer ${TOKEN}")
start_server

curl -fsS --max-time 10 "${auth[@]}" -H 'Content-Type: application/json' \
  -d '{"name":"idempotent","partitions":1}' \
  "http://127.0.0.1:${PORT}/broker/topics" | jq -e '.ok == true' >/dev/null

curl_pids=()
for i in $(seq 1 16); do
  curl -sS --max-time 15 "${auth[@]}" -H 'Content-Type: text/plain' \
    --data-binary 'payload-one' \
    "http://127.0.0.1:${PORT}/broker/topics/idempotent/produce?partition=0&idempotency_key=same-key&timeout_ms=10000" \
    >"${RUN_DIR}/response.${i}.json" &
  curl_pids+=("$!")
done
for pid in "${curl_pids[@]}"; do wait "${pid}"; done

jq -s -e '
  length == 16 and
  all(.[]; .ok == true and .local_fsync == true and
           .applied_all_workers == true) and
  ([.[].offset] | unique | length) == 1 and
  ([.[] | select(.duplicate == false)] | length) == 1
' "${RUN_DIR}"/response.*.json >/dev/null
OFFSET="$(jq -r '.offset' "${RUN_DIR}/response.1.json")"

conflict="$(curl -sS --max-time 10 "${auth[@]}" -H 'Content-Type: text/plain' \
  --data-binary 'different-payload' \
  "http://127.0.0.1:${PORT}/broker/topics/idempotent/produce?partition=0&idempotency_key=same-key&timeout_ms=5000")"
jq -e '.ok == false and .error == "idempotency_conflict"' <<<"${conflict}" >/dev/null

consume="$(curl -fsS --max-time 10 "${auth[@]}" \
  "http://127.0.0.1:${PORT}/broker/consume/idempotent/regression/0?offset=0&limit=100&encoding=b64")"
jq -e '[.messages[] | select(.value_b64 == "cGF5bG9hZC1vbmU=")] | length == 1' \
  <<<"${consume}" >/dev/null

stop_server KILL
start_server

retry="$(curl -fsS --max-time 15 "${auth[@]}" -H 'Content-Type: text/plain' \
  --data-binary 'payload-one' \
  "http://127.0.0.1:${PORT}/broker/topics/idempotent/produce?partition=0&idempotency_key=same-key&timeout_ms=10000")"
jq -e --argjson offset "${OFFSET}" \
  '.ok == true and .duplicate == true and .offset == $offset and
   .local_fsync == true and .applied_all_workers == true' \
  <<<"${retry}" >/dev/null

conflict_after="$(curl -sS --max-time 10 "${auth[@]}" -H 'Content-Type: text/plain' \
  --data-binary 'different-after-restart' \
  "http://127.0.0.1:${PORT}/broker/topics/idempotent/produce?partition=0&idempotency_key=same-key&timeout_ms=5000")"
jq -e '.ok == false and .error == "idempotency_conflict"' <<<"${conflict_after}" >/dev/null

# A durable append followed by failure to publish the final COMMITTED proof is
# an uncertain transaction. It must not be acknowledged, and replay must
# resolve a retry to the original message without appending a duplicate.
stop_server KILL
start_server 1

uncertain_code="$(curl -sS --max-time 15 -o "${RUN_DIR}/uncertain.json" \
  -w '%{http_code}' "${auth[@]}" -H 'Content-Type: text/plain' \
  --data-binary 'payload-two' \
  "http://127.0.0.1:${PORT}/broker/topics/idempotent/produce?partition=0&idempotency_key=uncertain-key&timeout_ms=10000")"
[[ "${uncertain_code}" == 503 ]]
jq -e '.ok == false and .local_fsync == true' \
  "${RUN_DIR}/uncertain.json" >/dev/null

stop_server KILL
start_server

resolved="$(curl -fsS --max-time 15 "${auth[@]}" -H 'Content-Type: text/plain' \
  --data-binary 'payload-two' \
  "http://127.0.0.1:${PORT}/broker/topics/idempotent/produce?partition=0&idempotency_key=uncertain-key&timeout_ms=10000")"
jq -e '.ok == true and .duplicate == true and .local_fsync == true and
       .applied_all_workers == true' <<<"${resolved}" >/dev/null

consume_after_uncertain="$(curl -fsS --max-time 10 "${auth[@]}" \
  "http://127.0.0.1:${PORT}/broker/consume/idempotent/regression/0?offset=0&limit=100&encoding=b64")"
jq -e '[.messages[] | select(.value_b64 == "cGF5bG9hZC10d28=")] | length == 1' \
  <<<"${consume_after_uncertain}" >/dev/null

printf 'PASS atomic payload-bound idempotency: concurrent retries, conflicts, uncertain COMMITTED-store recovery, and no duplicates\n'
