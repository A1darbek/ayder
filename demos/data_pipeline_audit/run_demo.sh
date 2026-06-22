#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

AYDER_PORT="${AYDER_PORT:-1124}"
export AYDER_BASE="${AYDER_BASE:-http://127.0.0.1:${AYDER_PORT}}"

# shellcheck source=demos/data_pipeline_audit/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd bash
require_cmd curl
require_cmd jq
require_cmd "$DOCKER_BIN"

RUN_ID="data_pipeline_audit_$(date -u +%Y%m%dT%H%M%SZ)"
ARTIFACT_DIR="${ARTIFACT_DIR:-${REPO_ROOT}/artifacts/data_pipeline_audit/${RUN_ID}}"
mkdir -p "$ARTIFACT_DIR"

START_AYDER="${START_AYDER:-1}"
AYDER_BIN="${AYDER_BIN:-${REPO_ROOT}/ayder}"
AYDER_WORKERS="${AYDER_WORKERS:-2}"
AYDER_AOF="${AYDER_AOF-}"
AYDER_PID_FILE="${ARTIFACT_DIR}/ayder.pid"
AYDER_STDOUT_LOG="${ARTIFACT_DIR}/ayder.log"
AYDER_STOP_FILE="${ARTIFACT_DIR}/ayder.stop"
export ARTIFACT_DIR OUT_DIR="${ARTIFACT_DIR}/receipt"

cleanup() {
  set +e
  : >"$AYDER_STOP_FILE" 2>/dev/null || true
  if [[ -n "${AYDER_MGR_PID:-}" ]]; then kill "$AYDER_MGR_PID" >/dev/null 2>&1 || true; fi
  if [[ -f "$AYDER_PID_FILE" ]]; then
    pid="$(cat "$AYDER_PID_FILE" 2>/dev/null || true)"
    if [[ -n "$pid" ]]; then kill "$pid" >/dev/null 2>&1 || true; fi
  fi
}
trap cleanup EXIT

wait_ayder() {
  local retries="${1:-120}"
  local i
  for ((i = 1; i <= retries; i++)); do
    if curl -fsS "${AYDER_BASE}/health" >/dev/null 2>&1; then
      return 0
    fi
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
  [[ -s "$receipt" ]] || { echo "receipt missing: $receipt" >&2; return 1; }
  [[ "$(jq -r '.verdict' "$receipt")" == "WARN" ]] || return 1
  [[ "$(jq -r '.retry_summary.api_attempts' "$receipt")" == "3" ]] || return 1
  [[ "$(jq -r '.retry_summary.final_error' "$receipt")" == "timeout" ]] || return 1
  [[ "$(jq -r '.retry_summary.source_status' "$receipt")" == "PARTIAL" ]] || return 1
  [[ "$(jq -r '.data_quality.input_rows' "$receipt")" == "10000" ]] || return 1
  [[ "$(jq -r '.data_quality.output_rows' "$receipt")" == "8420" ]] || return 1
  [[ "$(jq -r '.data_quality.valid_rows' "$receipt")" == "8300" ]] || return 1
  [[ "$(jq -r '.data_quality.invalid_rows' "$receipt")" == "120" ]] || return 1
  [[ "$(jq -r '.data_quality.missing_rows' "$receipt")" == "1580" ]] || return 1
}

log "data-pipeline-audit demo run_id=${RUN_ID} artifact_dir=${ARTIFACT_DIR}"

if [[ "$START_AYDER" == "1" ]]; then
  start_ayder_manager
  wait_ayder 120 || { echo "ayder did not become healthy at ${AYDER_BASE}" >&2; exit 1; }
fi

"${SCRIPT_DIR}/init.sh" >"${ARTIFACT_DIR}/init.log" 2>&1
"${SCRIPT_DIR}/producer.sh" >"${ARTIFACT_DIR}/producer.log" 2>&1
"${SCRIPT_DIR}/pipeline_runner.sh" >"${ARTIFACT_DIR}/pipeline_runner.log" 2>&1

set +e
"${SCRIPT_DIR}/recovery_receipt.sh" >"${ARTIFACT_DIR}/recovery_receipt.log" 2>&1
receipt_rc=$?
set -e
[[ "$receipt_rc" == "10" ]] || { echo "expected WARN receipt rc=10, got ${receipt_rc}" >&2; exit 1; }

validate_receipt

cat >"${ARTIFACT_DIR}/summary.txt" <<EOF
run_id=${RUN_ID}
artifact_dir=${ARTIFACT_DIR}
topic=${TOPIC}
group=${GROUP}
partition=${PARTITION}
ayder_base=${AYDER_BASE}
receipt_json=${OUT_DIR}/receipt.json
receipt_txt=${OUT_DIR}/receipt.txt
status=pass
EOF

cat "${OUT_DIR}/receipt.txt"
log "data-pipeline-audit demo success"
log "artifacts: ${ARTIFACT_DIR}"
