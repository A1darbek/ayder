#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/e2e/exactly_once/lib.sh
source "${SCRIPT_DIR}/lib.sh"

require_cmd bash
require_cmd curl
require_cmd "$DOCKER_BIN"
require_cmd jq

RUN_ID="e2e_eos_$(date -u +%Y%m%dT%H%M%SZ)"
ARTIFACT_DIR="${ARTIFACT_DIR:-${REPO_ROOT}/artifacts/e2e_exactly_once/${RUN_ID}}"
mkdir -p "$ARTIFACT_DIR"

TOTAL_EVENTS="${TOTAL_EVENTS:-200}"
DUP_EVERY="${DUP_EVERY:-10}"
START_AYDER="${START_AYDER:-1}"
AYDER_BIN="${AYDER_BIN:-${REPO_ROOT}/ayder}"
AYDER_PORT="${AYDER_PORT:-1109}"
AYDER_WORKERS="${AYDER_WORKERS:-2}"
AYDER_PID_FILE="${ARTIFACT_DIR}/ayder.pid"
AYDER_STDOUT_LOG="${ARTIFACT_DIR}/ayder.log"
AYDER_STOP_FILE="${ARTIFACT_DIR}/ayder.stop"

ENABLE_PARTITION="${ENABLE_PARTITION:-1}"
ENABLE_AYDER_KILL="${ENABLE_AYDER_KILL:-1}"
if [[ "$START_AYDER" != "1" ]]; then
  ENABLE_AYDER_KILL=0
fi

DURATION_SEC="${DURATION_SEC:-120}"
PARTITION_EVERY_SEC="${PARTITION_EVERY_SEC:-25}"
PARTITION_HOLD_SEC="${PARTITION_HOLD_SEC:-6}"
AYDER_KILL_EVERY_SEC="${AYDER_KILL_EVERY_SEC:-30}"

FAULT_CRASH_AFTER_CONSUME_PCT="${FAULT_CRASH_AFTER_CONSUME_PCT:-8}"
FAULT_CRASH_BEFORE_DB_COMMIT_PCT="${FAULT_CRASH_BEFORE_DB_COMMIT_PCT:-8}"
FAULT_CRASH_AFTER_DB_COMMIT_PCT="${FAULT_CRASH_AFTER_DB_COMMIT_PCT:-8}"

export TOTAL_EVENTS DUP_EVERY START_AYDER AYDER_BIN AYDER_PORT AYDER_WORKERS
export ENABLE_PARTITION ENABLE_AYDER_KILL DURATION_SEC PARTITION_EVERY_SEC PARTITION_HOLD_SEC AYDER_KILL_EVERY_SEC
export FAULT_CRASH_AFTER_CONSUME_PCT FAULT_CRASH_BEFORE_DB_COMMIT_PCT FAULT_CRASH_AFTER_DB_COMMIT_PCT
export AYDER_PID_FILE ARTIFACT_DIR

cleanup() {
  set +e
  touch "$AYDER_STOP_FILE" >/dev/null 2>&1 || true
  if [[ -n "${FAULT_PID:-}" ]]; then kill "$FAULT_PID" >/dev/null 2>&1 || true; fi
  if [[ -n "${CONSUMER_PID:-}" ]]; then kill "$CONSUMER_PID" >/dev/null 2>&1 || true; fi
  if [[ -n "${AYDER_MGR_PID:-}" ]]; then kill "$AYDER_MGR_PID" >/dev/null 2>&1 || true; fi
  if [[ -f "$AYDER_PID_FILE" ]]; then
    pid="$(cat "$AYDER_PID_FILE" 2>/dev/null || true)"
    if [[ -n "$pid" ]]; then kill "$pid" >/dev/null 2>&1 || true; fi
  fi
}
trap cleanup EXIT

wait_ayder() {
  local retries="${1:-80}"
  local i
  for ((i=1; i<=retries; i++)); do
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
  rm -f "$AYDER_STOP_FILE"

  (
    while true; do
      if [[ -f "$AYDER_STOP_FILE" ]]; then
        exit 0
      fi

      (
        cd "$ayder_dir" || exit 1
        export RF_BEARER_TOKENS="${RF_BEARER_TOKENS:-dev@55555555555555:11111111111111111:111111111111111111111}"
        export RF_HTTP_DISABLE_RL="${RF_HTTP_DISABLE_RL:-1}"
        "$AYDER_BIN" --port "$AYDER_PORT" --workers "$AYDER_WORKERS" >>"$AYDER_STDOUT_LOG" 2>&1
      ) &

      child=$!
      echo "$child" > "$AYDER_PID_FILE"
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

log "campaign run_id=${RUN_ID} artifact_dir=${ARTIFACT_DIR}"

"${SCRIPT_DIR}/init.sh" >"${ARTIFACT_DIR}/init.log" 2>&1

if [[ "$START_AYDER" == "1" ]]; then
  start_ayder_manager
  if ! wait_ayder 120; then
    echo "ayder did not become healthy" >&2
    exit 1
  fi
fi

ensure_topic

"${SCRIPT_DIR}/consumer_supervisor.sh" >"${ARTIFACT_DIR}/consumer.log" 2>&1 &
CONSUMER_PID=$!

export AYDER_PORT
"${SCRIPT_DIR}/fault_inject.sh" >"${ARTIFACT_DIR}/faults.log" 2>&1 &
FAULT_PID=$!

"${SCRIPT_DIR}/producer.sh" >"${ARTIFACT_DIR}/producer.log" 2>&1

wait "$CONSUMER_PID"
wait "$FAULT_PID" || true

"${SCRIPT_DIR}/verify_invariants.sh" >"${ARTIFACT_DIR}/invariants.log" 2>&1

cat >"${ARTIFACT_DIR}/summary.txt" <<EOF
run_id=${RUN_ID}
artifact_dir=${ARTIFACT_DIR}
total_events=${TOTAL_EVENTS}
dup_every=${DUP_EVERY}
start_ayder=${START_AYDER}
enable_partition=${ENABLE_PARTITION}
enable_ayder_kill=${ENABLE_AYDER_KILL}
fault_crash_after_consume_pct=${FAULT_CRASH_AFTER_CONSUME_PCT}
fault_crash_before_db_commit_pct=${FAULT_CRASH_BEFORE_DB_COMMIT_PCT}
fault_crash_after_db_commit_pct=${FAULT_CRASH_AFTER_DB_COMMIT_PCT}
status=pass
EOF

log "campaign success"
log "artifacts: ${ARTIFACT_DIR}"
