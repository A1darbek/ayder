#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/e2e/exactly_once/lib.sh
source "${SCRIPT_DIR}/lib.sh"

TOTAL_EVENTS="${TOTAL_EVENTS:-200}"
MAX_IDLE_STREAK="${MAX_IDLE_STREAK:-50}"
IDLE_SLEEP_SEC="${IDLE_SLEEP_SEC:-0.20}"

processed_target="$TOTAL_EVENTS"
idle_streak=0
step=0

log "consumer supervisor start target_events=${processed_target}"

while true; do
  step=$((step + 1))

  set +e
  "${SCRIPT_DIR}/consumer_step.sh"
  rc=$?
  set -e

  case "$rc" in
    0)
      idle_streak=0
      ;;
    10)
      idle_streak=$((idle_streak + 1))
      sleep "$IDLE_SLEEP_SEC"
      ;;
    20|21)
      sleep "$IDLE_SLEEP_SEC"
      ;;
    86)
      log "consumer crash fault injected; restarting step loop"
      sleep "$IDLE_SLEEP_SEC"
      ;;
    *)
      echo "consumer step failed rc=${rc}" >&2
      exit "$rc"
      ;;
  esac

  processed="$(pg_query_scalar "SELECT COUNT(*) FROM processed_events WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION};")"
  processed="${processed:-0}"

  if (( processed >= processed_target )); then
    log "consumer supervisor complete processed=${processed}"
    exit 0
  fi

  if (( idle_streak >= MAX_IDLE_STREAK )); then
    log "consumer idle timeout streak=${idle_streak} processed=${processed}"
    exit 3
  fi

done
