#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

AYDER_PORT="${AYDER_PORT:-1130}"
export AYDER_BASE="${AYDER_BASE:-http://127.0.0.1:${AYDER_PORT}}"

# shellcheck source=examples/payment-ambiguity-paypal/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd bash
require_cmd curl
require_cmd jq
require_cmd "$DOCKER_BIN"

SCENARIO="${SCENARIO:-timeout_unknown}"
case "$SCENARIO" in
  happy_path|timeout_then_success|timeout_unknown|duplicate_initiation|redis_stale) ;;
  *) echo "invalid SCENARIO=${SCENARIO}" >&2; exit 2 ;;
esac

RUN_ID="paypal_${SCENARIO}_$(date -u +%Y%m%dT%H%M%SZ)"
ARTIFACT_DIR="${ARTIFACT_DIR:-${REPO_ROOT}/artifacts/payment_ambiguity_paypal/${RUN_ID}}"
mkdir -p "$ARTIFACT_DIR"
export SCENARIO ARTIFACT_DIR OUT_DIR="${ARTIFACT_DIR}/receipt"

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

validate_final_receipt() {
  local receipt="${OUT_DIR}/receipt.json"
  [[ -s "$receipt" ]] || return 1
  [[ "$(jq -r '.ayder_evidence.event_durable' "$receipt")" == "true" ]] || return 1
  [[ "$(jq -r '.ayder_evidence.event_sealed' "$receipt")" == "true" ]] || return 1
  [[ "$(jq -r '.ayder_evidence.offset_committed_after_audit_write' "$receipt")" == "true" ]] || return 1
  [[ "$(jq -r '.ayder_evidence.deterministic_replay_available' "$receipt")" == "true" ]] || return 1
  [[ "$(jq -r '.source_of_truth.conflict_resolution' "$receipt")" == "database_wins" ]] || return 1
  [[ "$(jq -r '.source_of_truth.cache_backfilled' "$receipt")" == "true" ]] || return 1

  case "$SCENARIO" in
    timeout_unknown)
      [[ "$(jq -r '.verdict' "$receipt")" == "YELLOW" ]] || return 1
      [[ "$(jq -r '.states.provider_commitment' "$receipt")" == "UNKNOWN" ]] || return 1
      [[ "$(jq -r '.states.safe_to_close' "$receipt")" == "false" ]] || return 1
      [[ "$(jq -r '.verification_evidence.attempts' "$receipt")" == "3" ]] || return 1
      ;;
    duplicate_initiation)
      [[ "$(jq -r '.verdict' "$receipt")" == "GREEN" ]] || return 1
      [[ "$(jq -r '.idempotency_evidence.produce_requests' "$receipt")" == "2" ]] || return 1
      [[ "$(jq -r '.idempotency_evidence.logical_events' "$receipt")" == "1" ]] || return 1
      [[ "$(jq -r '.idempotency_evidence.duplicate_requests_suppressed' "$receipt")" == "1" ]] || return 1
      [[ "$(jq -r '.idempotency_evidence.wallet_credit_effects' "$receipt")" == "1" ]] || return 1
      ;;
    *)
      [[ "$(jq -r '.verdict' "$receipt")" == "GREEN" ]] || return 1
      [[ "$(jq -r '.states.provider_commitment' "$receipt")" == "SUCCESS" ]] || return 1
      [[ "$(jq -r '.states.safe_to_close' "$receipt")" == "true" ]] || return 1
      ;;
  esac
}

log "paypal ambiguity demo scenario=${SCENARIO} artifact_dir=${ARTIFACT_DIR}"

if [[ "$START_AYDER" == "1" ]]; then
  start_ayder_manager
  wait_ayder || { echo "ayder did not become healthy at ${AYDER_BASE}" >&2; exit 1; }
fi

"${SCRIPT_DIR}/init.sh" >"${ARTIFACT_DIR}/init.log" 2>&1
"${SCRIPT_DIR}/producer.sh" >"${ARTIFACT_DIR}/producer.log" 2>&1
"${SCRIPT_DIR}/worker.sh" >"${ARTIFACT_DIR}/worker.log" 2>&1

if [[ "$SCENARIO" == "timeout_then_success" ]]; then
  "${SCRIPT_DIR}/reconcile_cache.sh" >"${ARTIFACT_DIR}/cache_before.log" 2>&1
  set +e
  RECEIPT_NAME=receipt.before "${SCRIPT_DIR}/recovery_receipt.sh" >"${ARTIFACT_DIR}/receipt_before.log" 2>&1
  before_rc=$?
  set -e
  [[ "$before_rc" == "10" ]] || { echo "expected pre-verification YELLOW rc=10, got ${before_rc}" >&2; exit 1; }
fi

"${SCRIPT_DIR}/verify_provider.sh" >"${ARTIFACT_DIR}/verification.log" 2>&1
"${SCRIPT_DIR}/reconcile_cache.sh" >"${ARTIFACT_DIR}/cache.log" 2>&1

set +e
RECEIPT_NAME=receipt "${SCRIPT_DIR}/recovery_receipt.sh" >"${ARTIFACT_DIR}/receipt.log" 2>&1
receipt_rc=$?
set -e

if [[ "$SCENARIO" == "timeout_unknown" ]]; then
  [[ "$receipt_rc" == "10" ]] || { echo "expected YELLOW rc=10, got ${receipt_rc}" >&2; exit 1; }
else
  [[ "$receipt_rc" == "0" ]] || { echo "expected GREEN rc=0, got ${receipt_rc}" >&2; exit 1; }
fi

validate_final_receipt

cat >"${ARTIFACT_DIR}/summary.txt" <<EOF
run_id=${RUN_ID}
scenario=${SCENARIO}
artifact_dir=${ARTIFACT_DIR}
topic=${TOPIC}
group=${GROUP}
ayder_base=${AYDER_BASE}
receipt_json=${OUT_DIR}/receipt.json
status=pass
EOF

cat "${OUT_DIR}/receipt.txt"
log "paypal ambiguity demo success"
log "artifacts: ${ARTIFACT_DIR}"
