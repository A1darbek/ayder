#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

AYDER_PORT="${AYDER_PORT:-1133}"
export AYDER_BASE="${AYDER_BASE:-http://127.0.0.1:${AYDER_PORT}}"

# shellcheck source=examples/data-pipeline-recovery/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd bash
require_cmd curl
require_cmd jq
require_cmd "$DOCKER_BIN"

SCENARIO="${SCENARIO:-partial_download}"
case "$SCENARIO" in
  full_success|partial_download|hard_failure|partial_usable|replay_missing_partition) ;;
  *) echo "invalid SCENARIO=${SCENARIO}" >&2; exit 2 ;;
esac

RUN_ID="data_pipeline_${SCENARIO}_$(date -u +%Y%m%dT%H%M%SZ)"
ARTIFACT_DIR="${ARTIFACT_DIR:-${REPO_ROOT}/artifacts/data_pipeline_recovery/${RUN_ID}}"
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

validate_common() {
  local receipt="$1"
  [[ -s "$receipt" ]] || return 1
  [[ "$(jq -r '.ayder_evidence.topic' "$receipt")" == "data_downloads" ]] || return 1
  [[ "$(jq -r '.ayder_evidence.event_durable' "$receipt")" == "true" ]] || return 1
  [[ "$(jq -r '.ayder_evidence.offset_committed_after_audit_write' "$receipt")" == "true" ]] || return 1
  [[ "$(jq -r '.ayder_evidence.deterministic_replay_available' "$receipt")" == "true" ]] || return 1
  [[ "$(jq -r '.pipeline_context.job_run_id' "$receipt")" == "job_run_001" ]] || return 1
}

validate_final() {
  local receipt="${OUT_DIR}/receipt.json"
  validate_common "$receipt"
  case "$SCENARIO" in
    full_success)
      [[ "$(jq -r '.verdict' "$receipt")" == "PASS" ]] || return 1
      [[ "$(jq -r '.download_result.confirmed_pages' "$receipt")" == "10" ]] || return 1
      [[ "$(jq -r '.business_trust.fully_trusted' "$receipt")" == "true" ]] || return 1
      ;;
    partial_download)
      [[ "$(jq -r '.verdict' "$receipt")" == "WARN" ]] || return 1
      [[ "$(jq -r '.download_result.confirmed_pages' "$receipt")" == "8" ]] || return 1
      [[ "$(jq -c '.download_result.missing_pages' "$receipt")" == "[9,10]" ]] || return 1
      [[ "$(jq -r '.business_trust.manual_review_required' "$receipt")" == "true" ]] || return 1
      ;;
    hard_failure)
      [[ "$(jq -r '.verdict' "$receipt")" == "FAIL" ]] || return 1
      [[ "$(jq -r '.business_trust.downstream_should_not_continue' "$receipt")" == "true" ]] || return 1
      ;;
    partial_usable)
      [[ "$(jq -r '.verdict' "$receipt")" == "WARN" ]] || return 1
      [[ "$(jq -r '.business_trust.can_continue_partially' "$receipt")" == "true" ]] || return 1
      ;;
    replay_missing_partition)
      [[ "$(jq -r '.verdict' "$receipt")" == "PASS" ]] || return 1
      [[ "$(jq -r '.download_result.confirmed_pages' "$receipt")" == "10" ]] || return 1
      [[ "$(jq -r '.ayder_evidence.replay_count' "$receipt")" == "1" ]] || return 1
      [[ "$(jq -c '.replay_history[0].pages_recovered' "$receipt")" == "[9,10]" ]] || return 1
      ;;
  esac
}

log "data pipeline recovery scenario=${SCENARIO} artifact_dir=${ARTIFACT_DIR}"

if [[ "$START_AYDER" == "1" ]]; then
  start_ayder_manager
  wait_ayder || { echo "ayder did not become healthy at ${AYDER_BASE}" >&2; exit 1; }
fi

"${SCRIPT_DIR}/init.sh" >"${ARTIFACT_DIR}/init.log" 2>&1
"${SCRIPT_DIR}/producer.sh" >"${ARTIFACT_DIR}/producer.log" 2>&1
REPLAY_NUMBER=0 "${SCRIPT_DIR}/worker.sh" >"${ARTIFACT_DIR}/worker.log" 2>&1

if [[ "$SCENARIO" == "replay_missing_partition" ]]; then
  set +e
  RECEIPT_NAME=receipt.before "${SCRIPT_DIR}/recovery_receipt.sh" >"${ARTIFACT_DIR}/receipt_before.log" 2>&1
  before_rc=$?
  set -e
  [[ "$before_rc" == "10" ]] || { echo "expected pre-replay WARN rc=10, got ${before_rc}" >&2; exit 1; }
  validate_common "${OUT_DIR}/receipt.before.json"
  [[ "$(jq -r '.verdict' "${OUT_DIR}/receipt.before.json")" == "WARN" ]] || exit 1

  REPLAY_NUMBER=1 "${SCRIPT_DIR}/worker.sh" >"${ARTIFACT_DIR}/replay_worker.log" 2>&1
fi

set +e
RECEIPT_NAME=receipt "${SCRIPT_DIR}/recovery_receipt.sh" >"${ARTIFACT_DIR}/receipt.log" 2>&1
receipt_rc=$?
set -e

case "$SCENARIO" in
  full_success|replay_missing_partition)
    [[ "$receipt_rc" == "0" ]] || { echo "expected PASS rc=0, got ${receipt_rc}" >&2; exit 1; }
    ;;
  partial_download|partial_usable)
    [[ "$receipt_rc" == "10" ]] || { echo "expected WARN rc=10, got ${receipt_rc}" >&2; exit 1; }
    ;;
  hard_failure)
    [[ "$receipt_rc" == "20" ]] || { echo "expected FAIL rc=20, got ${receipt_rc}" >&2; exit 1; }
    ;;
esac

validate_final

cat >"${ARTIFACT_DIR}/summary.txt" <<EOF
run_id=${RUN_ID}
scenario=${SCENARIO}
artifact_dir=${ARTIFACT_DIR}
topic=${TOPIC}
consumer_group=${GROUP}
ayder_base=${AYDER_BASE}
receipt_json=${OUT_DIR}/receipt.json
status=pass
EOF

cat "${OUT_DIR}/receipt.txt"
log "data pipeline recovery example success"
log "artifacts: ${ARTIFACT_DIR}"
