#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AYDER_BIN="${AYDER_BIN:-${ROOT_DIR}/ayder}"
PORT="${AYDER_BUS_FAILURE_PORT:-$((22000 + ($$ % 16000)))}"
TOKEN="cross-worker-apply-failure-token"
RUN_DIR="$(mktemp -d /tmp/ayder-cross-worker-apply-failure.XXXXXX)"
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
  rm -f -- /dev/shm/ayder_bus_failure_storage_$$_* \
            /dev/shm/ayder_bus_failure_metrics_$$_*
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
  local inject_failure="${1:-0}"
  START_COUNT=$((START_COUNT + 1))
  local storage_name="/ayder_bus_failure_storage_$$_${START_COUNT}"
  local metrics_name="/ayder_bus_failure_metrics_$$_${START_COUNT}"
  local -a fault_env=()
  if [[ "${inject_failure}" == 1 ]]; then
    fault_env=(RF_TEST_FAULTS=1 RF_TEST_BUS_APPLY_FAIL_REC_ID=0xB10C0001)
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
      "${AYDER_BIN}" --workers 2 --aof always --port "${PORT}"
  ) >"${RUN_DIR}/server.${START_COUNT}.log" 2>&1 &
  SERVER_PID=$!

  for _ in $(seq 1 200); do
    local body
    body="$(curl -fsS --max-time 1 "http://127.0.0.1:${PORT}/ready" 2>/dev/null || true)"
    [[ "${body}" == '{"ready":true}' ]] && return 0
    sleep 0.05
  done
  tail -n 100 "${RUN_DIR}/server.${START_COUNT}.log" >&2
  return 1
}

auth=(-H "Authorization: Bearer ${TOKEN}")
start_server 1

curl -fsS --max-time 10 "${auth[@]}" -H 'Content-Type: application/json' \
  -d '{"name":"apply-failure","partitions":1}' \
  "http://127.0.0.1:${PORT}/broker/topics" |
  jq -e '.ok == true' >/dev/null

http_code="$(curl -sS --max-time 15 -o "${RUN_DIR}/failed-produce.json" \
  -w '%{http_code}' "${auth[@]}" -H 'Content-Type: text/plain' \
  --data-binary 'durable-but-unacknowledged' \
  "http://127.0.0.1:${PORT}/broker/topics/apply-failure/produce?partition=0&timeout_ms=10000")"
[[ "${http_code}" == 503 ]]
jq -e '.ok == false and .local_fsync == true and
       .applied_all_workers == false' \
  "${RUN_DIR}/failed-produce.json" >/dev/null

stop_server KILL
start_server 0

missing=0
for _ in $(seq 1 100); do
  consume="$(curl -fsS --max-time 5 "${auth[@]}" \
    "http://127.0.0.1:${PORT}/broker/consume/apply-failure/regression/0?offset=0&limit=20&encoding=b64")"
  if ! jq -e '[.messages[]?.value_b64] |
              index("ZHVyYWJsZS1idXQtdW5hY2tub3dsZWRnZWQ=") != null' \
      <<<"${consume}" >/dev/null; then
    missing=$((missing + 1))
  fi
done
[[ "${missing}" -eq 0 ]]

recovered="$(curl -fsS --max-time 15 "${auth[@]}" \
  -H 'Content-Type: text/plain' --data-binary 'post-recovery' \
  "http://127.0.0.1:${PORT}/broker/topics/apply-failure/produce?partition=0&timeout_ms=10000")"
jq -e '.ok == true and .local_fsync == true and
       .applied_all_workers == true' <<<"${recovered}" >/dev/null

printf 'PASS cross-worker apply failures fail acknowledgment; restart replays the durable record to every worker\n'
