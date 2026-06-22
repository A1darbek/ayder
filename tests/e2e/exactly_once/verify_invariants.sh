#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/e2e/exactly_once/lib.sh
source "${SCRIPT_DIR}/lib.sh"

failures=0

check_eq() {
  local label="$1"
  local got="$2"
  local want="$3"
  if [[ "$got" == "$want" ]]; then
    printf 'PASS %-42s got=%s\n' "$label" "$got"
  else
    printf 'FAIL %-42s got=%s want=%s\n' "$label" "$got" "$want"
    failures=$((failures + 1))
  fi
}

check_zero() {
  local label="$1"
  local got="$2"
  check_eq "$label" "$got" "0"
}

expected_count="$(pg_query_scalar "SELECT COUNT(*) FROM expected_events;")"
processed_count="$(pg_query_scalar "SELECT COUNT(*) FROM processed_events WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION};")"
processed_distinct="$(pg_query_scalar "SELECT COUNT(DISTINCT event_id) FROM processed_events WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION};")"
missing_expected="$(pg_query_scalar "SELECT COUNT(*) FROM expected_events e LEFT JOIN processed_events p ON p.event_id=e.event_id AND p.topic='${TOPIC}' AND p.group_id='${GROUP}' AND p.partition_id=${PARTITION} WHERE p.event_id IS NULL;")"
state_last_offset="$(pg_query_scalar "SELECT COALESCE((SELECT last_offset FROM consumer_state WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION}), -1);")"
max_processed_offset="$(pg_query_scalar "SELECT COALESCE(MAX(msg_offset), -1) FROM processed_events WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION};")"
min_processed_offset="$(pg_query_scalar "SELECT COALESCE(MIN(msg_offset), -1) FROM processed_events WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION};")"
non_monotonic_rows="$(pg_query_scalar "SELECT COUNT(*) FROM consumer_state_history WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION} AND new_offset < prev_offset;")"
duplicate_effect_ids="$(pg_query_scalar "SELECT COUNT(*) FROM (SELECT event_id FROM processed_events WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION} GROUP BY event_id HAVING COUNT(*) > 1) d;")"

printf 'INFO expected_count=%s processed_count=%s state_last_offset=%s\n' "$expected_count" "$processed_count" "$state_last_offset"

check_eq "no duplicate business effects" "$processed_count" "$processed_distinct"
check_zero "duplicate event ids" "$duplicate_effect_ids"
check_zero "missing effects for expected events" "$missing_expected"
check_zero "monotonic offset progression violations" "$non_monotonic_rows"
check_eq "consumer state tracks max processed offset" "$state_last_offset" "$max_processed_offset"

if [[ "$processed_count" != "0" ]]; then
  check_eq "first processed offset is zero" "$min_processed_offset" "0"
fi

expected_last=$((expected_count - 1))
check_eq "state offset equals expected_last" "$state_last_offset" "$expected_last"

sum_expected="$(pg_query_scalar "SELECT COALESCE(SUM(delta),0) FROM expected_events;")"
sum_balances="$(pg_query_scalar "SELECT COALESCE(SUM(balance),0) FROM account_balances;")"
check_eq "replay safety (no double-apply on balances)" "$sum_balances" "$sum_expected"

commit_failures="$(pg_query_scalar "SELECT COUNT(*) FROM commit_log WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION} AND commit_ok=false;")"
printf 'INFO broker commit transient failures observed=%s (allowed; DB state remains source of truth)\n' "$commit_failures"

if (( failures > 0 )); then
  echo "invariants failed: ${failures}" >&2
  exit 1
fi

echo "all invariants passed"
