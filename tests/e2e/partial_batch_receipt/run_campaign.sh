#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

AYDER_PORT="${AYDER_PORT:-1109}"
export AYDER_BASE="${AYDER_BASE:-http://127.0.0.1:${AYDER_PORT}}"

# shellcheck source=tests/e2e/partial_batch_receipt/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd bash
require_cmd curl
require_cmd jq
require_cmd "$DOCKER_BIN"

RUN_ID="pbr_$(date -u +%Y%m%dT%H%M%SZ)"
ARTIFACT_DIR="${ARTIFACT_DIR:-${REPO_ROOT}/artifacts/partial_batch_receipt/${RUN_ID}}"
mkdir -p "$ARTIFACT_DIR"

TOTAL_EVENTS="${TOTAL_EVENTS:-10}"
BATCH_SIZE="${BATCH_SIZE:-10}"
POISON_TAIL="${POISON_TAIL:-2}"
MODE="${MODE:-silent_skip}"            # silent_skip | dlq | correct
START_AYDER="${START_AYDER:-1}"
AYDER_BIN="${AYDER_BIN:-${REPO_ROOT}/ayder}"
AYDER_WORKERS="${AYDER_WORKERS:-2}"
AYDER_AOF="${AYDER_AOF-}"              # empty uses the binary default
AYDER_PID_FILE="${ARTIFACT_DIR}/ayder.pid"
AYDER_STDOUT_LOG="${ARTIFACT_DIR}/ayder.log"
AYDER_STOP_FILE="${ARTIFACT_DIR}/ayder.stop"

export TOTAL_EVENTS BATCH_SIZE POISON_TAIL MODE ARTIFACT_DIR
export AYDER_PORT AYDER_BIN AYDER_WORKERS AYDER_AOF AYDER_PID_FILE
export OUT_DIR="${ARTIFACT_DIR}/receipt"

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
  if [[ ! -x "$AYDER_BIN" ]]; then
    echo "ayder binary not executable: $AYDER_BIN" >&2
    exit 1
  fi

  local ayder_dir="${ARTIFACT_DIR}/ayder_data"
  mkdir -p "$ayder_dir"

  (
    while true; do
      if [[ -f "$AYDER_STOP_FILE" ]]; then
        exit 0
      fi

      (
        cd "$ayder_dir" || exit 1
        export RF_BEARER_TOKENS="${RF_BEARER_TOKENS:-dev@55555555555555:11111111111111111:111111111111111111111}"
        export RF_HTTP_DISABLE_RL="${RF_HTTP_DISABLE_RL:-1}"
        args=(--port "$AYDER_PORT" --workers "$AYDER_WORKERS")
        if [[ -n "${AYDER_AOF}" ]]; then
          args+=(--aof "$AYDER_AOF")
        fi
        "$AYDER_BIN" "${args[@]}" >>"$AYDER_STDOUT_LOG" 2>&1
      ) &

      child=$!
      echo "$child" >"$AYDER_PID_FILE"
      wait "$child"
      rc=$?

      if [[ -f "$AYDER_STOP_FILE" ]]; then
        exit 0
      fi

      log "ayder manager restart after exit rc=${rc}"
      sleep 1
    done
  ) &

  AYDER_MGR_PID=$!
}

validate_receipt() {
  local receipt="${OUT_DIR}/receipt.json"
  if [[ ! -s "$receipt" ]]; then
    echo "receipt missing: $receipt" >&2
    return 1
  fi

  local verdict expected confirmed missing partial unresolved dlq
  local effect_confirmed effect_missing effect_suppressed
  verdict="$(jq -r '.verdict' "$receipt")"
  expected="$(jq -r '.totals.expected' "$receipt")"
  confirmed="$(jq -r '.totals.confirmed' "$receipt")"
  missing="$(jq -r '.totals.missing' "$receipt")"
  partial="$(jq -r '.totals.partial' "$receipt")"
  unresolved="$(jq -r '.totals.unresolved' "$receipt")"
  dlq="$(jq -r '.totals.dlq' "$receipt")"
  effect_confirmed="$(jq -r '.effect_status_counts.confirmed' "$receipt")"
  effect_missing="$(jq -r '.effect_status_counts.missing' "$receipt")"
  effect_suppressed="$(jq -r '.effect_status_counts.suppressed' "$receipt")"

  case "$MODE" in
    silent_skip)
      [[ "$verdict" == "RED" ]] || { echo "expected RED verdict, got ${verdict}" >&2; return 1; }
      [[ "$expected" == "$TOTAL_EVENTS" ]] || { echo "expected ${TOTAL_EVENTS} events, got ${expected}" >&2; return 1; }
      [[ "$confirmed" == "$((TOTAL_EVENTS - POISON_TAIL))" ]] || { echo "expected $((TOTAL_EVENTS - POISON_TAIL)) confirmed, got ${confirmed}" >&2; return 1; }
      [[ "$missing" == "1" ]] || { echo "expected 1 missing, got ${missing}" >&2; return 1; }
      [[ "$partial" == "1" ]] || { echo "expected 1 partial, got ${partial}" >&2; return 1; }
      [[ "$dlq" == "0" ]] || { echo "expected 0 dlq, got ${dlq}" >&2; return 1; }
      [[ "$effect_confirmed" == "$(( (TOTAL_EVENTS - POISON_TAIL) * 3 + 2 ))" ]] || { echo "expected 26 confirmed effects, got ${effect_confirmed}" >&2; return 1; }
      [[ "$effect_missing" == "$(( (POISON_TAIL - 1) * 3 + 1 ))" ]] || { echo "expected 4 missing effects, got ${effect_missing}" >&2; return 1; }
      ;;
    dlq)
      [[ "$verdict" == "AMBER" ]] || { echo "expected AMBER verdict, got ${verdict}" >&2; return 1; }
      [[ "$confirmed" == "$((TOTAL_EVENTS - POISON_TAIL))" ]] || { echo "expected $((TOTAL_EVENTS - POISON_TAIL)) confirmed, got ${confirmed}" >&2; return 1; }
      [[ "$missing" == "0" ]] || { echo "expected 0 missing, got ${missing}" >&2; return 1; }
      [[ "$partial" == "0" ]] || { echo "expected 0 partial, got ${partial}" >&2; return 1; }
      [[ "$unresolved" == "$POISON_TAIL" ]] || { echo "expected ${POISON_TAIL} unresolved, got ${unresolved}" >&2; return 1; }
      [[ "$dlq" == "$POISON_TAIL" ]] || { echo "expected ${POISON_TAIL} dlq, got ${dlq}" >&2; return 1; }
      [[ "$effect_missing" == "0" ]] || { echo "expected 0 missing effects, got ${effect_missing}" >&2; return 1; }
      [[ "$effect_suppressed" == "$((POISON_TAIL * 3))" ]] || { echo "expected $((POISON_TAIL * 3)) suppressed effects, got ${effect_suppressed}" >&2; return 1; }
      ;;
    correct)
      [[ "$verdict" == "GREEN" ]] || { echo "expected GREEN verdict, got ${verdict}" >&2; return 1; }
      [[ "$confirmed" == "$TOTAL_EVENTS" ]] || { echo "expected ${TOTAL_EVENTS} confirmed, got ${confirmed}" >&2; return 1; }
      [[ "$missing" == "0" ]] || { echo "expected 0 missing, got ${missing}" >&2; return 1; }
      [[ "$partial" == "0" ]] || { echo "expected 0 partial, got ${partial}" >&2; return 1; }
      [[ "$dlq" == "0" ]] || { echo "expected 0 dlq, got ${dlq}" >&2; return 1; }
      [[ "$effect_confirmed" == "$((TOTAL_EVENTS * 3))" ]] || { echo "expected $((TOTAL_EVENTS * 3)) confirmed effects, got ${effect_confirmed}" >&2; return 1; }
      [[ "$effect_missing" == "0" ]] || { echo "expected 0 missing effects, got ${effect_missing}" >&2; return 1; }
      ;;
    *)
      echo "invalid MODE='${MODE}' (want silent_skip|dlq|correct)" >&2
      return 2
      ;;
  esac
}

log "partial-batch campaign run_id=${RUN_ID} artifact_dir=${ARTIFACT_DIR}"
log "mode=${MODE} total_events=${TOTAL_EVENTS} batch_size=${BATCH_SIZE} poison_tail=${POISON_TAIL}"

if [[ "$START_AYDER" == "1" ]]; then
  start_ayder_manager
  if ! wait_ayder 120; then
    echo "ayder did not become healthy at ${AYDER_BASE}" >&2
    exit 1
  fi
fi

"${SCRIPT_DIR}/init.sh" >"${ARTIFACT_DIR}/init.log" 2>&1
ensure_topic

"${SCRIPT_DIR}/producer.sh" >"${ARTIFACT_DIR}/producer.log" 2>&1
"${SCRIPT_DIR}/batch_consumer.sh" >"${ARTIFACT_DIR}/consumer.log" 2>&1

set +e
"${SCRIPT_DIR}/recovery_receipt.sh" >"${ARTIFACT_DIR}/recovery_receipt.log" 2>&1
receipt_rc=$?
set -e

case "$MODE" in
  silent_skip)
    [[ "$receipt_rc" == "20" ]] || { echo "expected recovery_receipt.sh rc=20 for RED, got ${receipt_rc}" >&2; exit 1; }
    ;;
  dlq)
    [[ "$receipt_rc" == "10" ]] || { echo "expected recovery_receipt.sh rc=10 for AMBER, got ${receipt_rc}" >&2; exit 1; }
    ;;
  correct)
    [[ "$receipt_rc" == "0" ]] || { echo "expected recovery_receipt.sh rc=0 for GREEN, got ${receipt_rc}" >&2; exit 1; }
    ;;
esac

validate_receipt

cat >"${ARTIFACT_DIR}/summary.txt" <<EOF
run_id=${RUN_ID}
artifact_dir=${ARTIFACT_DIR}
topic=${TOPIC}
group=${GROUP}
partition=${PARTITION}
mode=${MODE}
total_events=${TOTAL_EVENTS}
batch_size=${BATCH_SIZE}
poison_tail=${POISON_TAIL}
start_ayder=${START_AYDER}
ayder_base=${AYDER_BASE}
ayder_aof=${AYDER_AOF}
receipt_json=${OUT_DIR}/receipt.json
receipt_txt=${OUT_DIR}/receipt.txt
status=pass
EOF

cat "${OUT_DIR}/receipt.txt"
log "partial-batch campaign success"
log "artifacts: ${ARTIFACT_DIR}"
