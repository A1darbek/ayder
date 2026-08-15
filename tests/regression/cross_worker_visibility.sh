#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AYDER_BIN="${AYDER_BIN:-${ROOT_DIR}/ayder}"
PORT="${AYDER_CROSS_WORKER_PORT:-$((18000 + ($$ % 20000)))}"
REQUESTS="${AYDER_CROSS_WORKER_REQUESTS:-400}"
TOKEN="cross-worker-regression-token"
RUN_DIR="$(mktemp -d /tmp/ayder-cross-worker-regression.XXXXXX)"
METRICS_NAME="/ayder_cross_worker_metrics_$$"
SERVER_PID=""

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill -TERM "${SERVER_PID}" 2>/dev/null || true
    for _ in $(seq 1 50); do
      kill -0 "${SERVER_PID}" 2>/dev/null || break
      sleep 0.1
    done
    kill -KILL "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -rf -- "${RUN_DIR}"
}
trap cleanup EXIT

for tool in curl jq; do
  command -v "${tool}" >/dev/null 2>&1 || {
    printf 'missing required tool: %s\n' "${tool}" >&2
    exit 2
  }
done
[[ -x "${AYDER_BIN}" ]] || {
  printf 'Ayder binary not found: %s (run make first)\n' "${AYDER_BIN}" >&2
  exit 2
}

(
  cd "${RUN_DIR}"
  RF_BEARER_TOKENS="${TOKEN}" \
  RF_HTTP_DISABLE_RL=1 \
  RF_SHARED_ENTRIES=65536 \
  AYDER_METRICS_SHM_NAME="${METRICS_NAME}" \
    "${AYDER_BIN}" --workers 4 --aof always --port "${PORT}"
) >"${RUN_DIR}/server.log" 2>&1 &
SERVER_PID=$!

ready=0
for _ in $(seq 1 200); do
  body="$(curl -fsS --max-time 1 "http://127.0.0.1:${PORT}/ready" 2>/dev/null || true)"
  if [[ "${body}" == '{"ready":true}' ]]; then
    ready=1
    break
  fi
  sleep 0.05
done
if [[ "${ready}" -ne 1 ]]; then
  tail -n 100 "${RUN_DIR}/server.log" >&2
  exit 1
fi

auth=(-H "Authorization: Bearer ${TOKEN}")
curl -fsS --max-time 10 "${auth[@]}" -H 'Content-Type: application/json' \
  -d '{"name":"visibility","partitions":1}' \
  "http://127.0.0.1:${PORT}/broker/topics" |
  jq -e '.ok == true and .partitions == 1' >/dev/null

produce="$(curl -fsS --max-time 10 "${auth[@]}" -H 'Content-Type: text/plain' \
  --data-binary 'cross-worker-record' \
  "http://127.0.0.1:${PORT}/broker/topics/visibility/produce?partition=0&timeout_ms=5000")"
jq -e '.ok == true and .durable == true and .synced == true' <<<"${produce}" >/dev/null

missing=0
for _ in $(seq 1 "${REQUESTS}"); do
  consume="$(curl -fsS --max-time 3 "${auth[@]}" \
    "http://127.0.0.1:${PORT}/broker/consume/visibility/regression/0?offset=0&limit=10&encoding=b64")"
  if ! jq -e '[.messages[]?.value_b64] | index("Y3Jvc3Mtd29ya2VyLXJlY29yZA==") != null' \
      <<<"${consume}" >/dev/null; then
    missing=$((missing + 1))
  fi
done

if [[ "${missing}" -ne 0 ]]; then
  printf 'FAIL cross-worker visibility: missing=%d/%d\n' "${missing}" "${REQUESTS}" >&2
  tail -n 100 "${RUN_DIR}/server.log" >&2
  exit 1
fi

kill -TERM "${SERVER_PID}"
wait "${SERVER_PID}"
SERVER_PID=""
printf 'PASS cross-worker topic/message visibility: %d/%d responses contained the durable record\n' \
  "${REQUESTS}" "${REQUESTS}"
