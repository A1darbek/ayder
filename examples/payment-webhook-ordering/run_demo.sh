#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

AYDER_PORT="${AYDER_PORT:-1132}"
export AYDER_BASE="${AYDER_BASE:-http://127.0.0.1:${AYDER_PORT}}"

# shellcheck source=examples/payment-webhook-ordering/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd bash
require_cmd curl
require_cmd jq
require_cmd "$DOCKER_BIN"

RUN_ID="payment_webhook_ordering_$(date -u +%Y%m%dT%H%M%SZ)"
ARTIFACT_DIR="${ARTIFACT_DIR:-${REPO_ROOT}/artifacts/payment_webhook_ordering/${RUN_ID}}"
mkdir -p "$ARTIFACT_DIR"
export ARTIFACT_DIR OUT_DIR="${ARTIFACT_DIR}/receipt"

START_AYDER="${START_AYDER:-1}"
AYDER_BIN="${AYDER_BIN:-${REPO_ROOT}/ayder}"
AYDER_WORKERS="${AYDER_WORKERS:-2}"
AYDER_AOF="${AYDER_AOF-}"
AYDER_PID_FILE="${ARTIFACT_DIR}/ayder.pid"
AYDER_STDOUT_LOG="${ARTIFACT_DIR}/ayder.log"
AYDER_STOP_FILE="${ARTIFACT_DIR}/ayder.stop"

cleanup() {
  set +e
  : >"$AYDER_STOP_FILE" 2>/dev/null || true
  if [[ -f "$AYDER_PID_FILE" ]]; then
    pid="$(cat "$AYDER_PID_FILE" 2>/dev/null || true)"
    [[ -n "$pid" ]] && kill "$pid" >/dev/null 2>&1 || true
  fi
  if [[ -n "${AYDER_MGR_PID:-}" ]]; then
    kill "$AYDER_MGR_PID" >/dev/null 2>&1 || true
    wait "$AYDER_MGR_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_ayder() {
  local i
  for ((i = 1; i <= 120; i++)); do
    curl -fsS "${AYDER_BASE}/health" >/dev/null 2>&1 && return 0
    sleep 0.25
  done
  return 1
}

start_ayder_manager() {
  [[ -x "$AYDER_BIN" ]] || { echo "ayder binary not executable: $AYDER_BIN" >&2; exit 1; }
  local ayder_dir="${ARTIFACT_DIR}/ayder_data"
  mkdir -p "$ayder_dir"
  (
    while true; do
      [[ -f "$AYDER_STOP_FILE" ]] && exit 0
      (
        cd "$ayder_dir" || exit 1
        export RF_BEARER_TOKENS="${RF_BEARER_TOKENS:-dev@55555555555555:11111111111111111:111111111111111111111}"
        export RF_HTTP_DISABLE_RL="${RF_HTTP_DISABLE_RL:-1}"
        args=(--port "$AYDER_PORT" --workers "$AYDER_WORKERS")
        [[ -n "$AYDER_AOF" ]] && args+=(--aof "$AYDER_AOF")
        "$AYDER_BIN" "${args[@]}" >>"$AYDER_STDOUT_LOG" 2>&1
      ) &
      child=$!
      echo "$child" >"$AYDER_PID_FILE"
      wait "$child"
      [[ -f "$AYDER_STOP_FILE" ]] && exit 0
      sleep 1
    done
  ) &
  AYDER_MGR_PID=$!
}

validate_receipt() {
  local receipt="${OUT_DIR}/receipt.json"
  [[ -s "$receipt" ]] || return 1
  [[ "$(jq -r '.verdict' "$receipt")" == "GREEN" ]] || return 1
  [[ "$(jq -r '.ayder_evidence.topic' "$receipt")" == "payment_webhooks" ]] || return 1
  [[ "$(jq -r '.ayder_evidence.consumer_group' "$receipt")" == "webhook_worker" ]] || return 1
  [[ "$(jq -r '.ayder_evidence.events_consumed' "$receipt")" == "3" ]] || return 1
  [[ "$(jq -r '.ayder_evidence.offsets_committed' "$receipt")" == "true" ]] || return 1
  [[ "$(jq -r '.ayder_evidence.deterministic_replay_available' "$receipt")" == "true" ]] || return 1
  [[ "$(jq -r '.payment_state.final_state' "$receipt")" == "SUCCESS" ]] || return 1
  [[ "$(jq -r '.webhook_handling[0].decision' "$receipt")" == "APPLIED" ]] || return 1
  [[ "$(jq -r '.webhook_handling[1].decision' "$receipt")" == "IGNORED_STALE_TRANSITION" ]] || return 1
  [[ "$(jq -r '.webhook_handling[2].decision' "$receipt")" == "SUPPRESSED_DUPLICATE" ]] || return 1
  [[ "$(jq -r '[.rules[] | select(.status!="PASS")] | length' "$receipt")" == "0" ]] || return 1
}

log "payment webhook ordering example artifact_dir=${ARTIFACT_DIR}"

if [[ "$START_AYDER" == "1" ]]; then
  start_ayder_manager
  wait_ayder || { echo "ayder did not become healthy at ${AYDER_BASE}" >&2; exit 1; }
fi

"${SCRIPT_DIR}/init.sh" >"${ARTIFACT_DIR}/init.log" 2>&1
"${SCRIPT_DIR}/producer.sh" >"${ARTIFACT_DIR}/producer.log" 2>&1
"${SCRIPT_DIR}/worker.sh" >"${ARTIFACT_DIR}/worker.log" 2>&1
"${SCRIPT_DIR}/recovery_receipt.sh" >"${ARTIFACT_DIR}/receipt.log" 2>&1

validate_receipt

cat >"${ARTIFACT_DIR}/summary.txt" <<EOF
run_id=${RUN_ID}
artifact_dir=${ARTIFACT_DIR}
topic=${TOPIC}
consumer_group=${GROUP}
ayder_base=${AYDER_BASE}
receipt_json=${OUT_DIR}/receipt.json
status=pass
EOF

cat "${OUT_DIR}/receipt.txt"
log "payment webhook ordering example success"
log "artifacts: ${ARTIFACT_DIR}"
