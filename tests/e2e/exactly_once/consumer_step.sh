#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/e2e/exactly_once/lib.sh
source "${SCRIPT_DIR}/lib.sh"

CONSUME_LIMIT="${CONSUME_LIMIT:-1}"
CONSUME_TIMEOUT_SEC="${CONSUME_TIMEOUT_SEC:-1.2}"
COMMIT_RETRIES="${COMMIT_RETRIES:-5}"
COMMIT_SLEEP_MS="${COMMIT_SLEEP_MS:-120}"

FAULT_CRASH_AFTER_CONSUME_PCT="${FAULT_CRASH_AFTER_CONSUME_PCT:-0}"
FAULT_CRASH_BEFORE_DB_COMMIT_PCT="${FAULT_CRASH_BEFORE_DB_COMMIT_PCT:-0}"
FAULT_CRASH_AFTER_DB_COMMIT_PCT="${FAULT_CRASH_AFTER_DB_COMMIT_PCT:-0}"

require_cmd jq
require_cmd curl
require_cmd "$DOCKER_BIN"
require_cmd base64

maybe_crash() {
  local pct="$1"
  local label="$2"
  if (( pct <= 0 )); then
    return 0
  fi
  if (( RANDOM % 100 < pct )); then
    log "fault injected: crash at ${label}"
    exit 86
  fi
}

next_offset="$(pg_query_scalar "SELECT COALESCE((SELECT last_offset FROM consumer_state WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION}), -1) + 1;")"
if [[ -z "$next_offset" ]]; then
  next_offset=0
fi

consume_url="${AYDER_BASE}/broker/consume/${TOPIC}/${GROUP}/${PARTITION}?offset=${next_offset}&limit=${CONSUME_LIMIT}&encoding=b64"

set +e
raw="$(curl -sS --max-time "$CONSUME_TIMEOUT_SEC" -w "\n%{http_code}" -H "$AUTH_HEADER" "$consume_url")"
rc=$?
set -e

if [[ $rc -ne 0 ]]; then
  log "transient consume error rc=${rc} offset=${next_offset}"
  exit 20
fi

http_code="${raw##*$'\n'}"
body="${raw%$'\n'*}"

if [[ "$http_code" != "200" ]]; then
  err="$(echo "$body" | jq -r '.error // "http_error"' 2>/dev/null || echo "http_error")"
  log "consume non-200 code=${http_code} err=${err}"
  exit 21
fi

count="$(echo "$body" | jq -r '.count // 0')"
if [[ "$count" == "0" ]]; then
  exit 10
fi

msg_offset="$(echo "$body" | jq -r '.messages[0].offset')"
value_b64="$(echo "$body" | jq -r '.messages[0].value_b64')"
payload_json="$(printf '%s' "$value_b64" | base64 -d)"
event_id="$(printf '%s' "$payload_json" | jq -r '.event_id')"
account_id="$(printf '%s' "$payload_json" | jq -r '.account_id')"
delta="$(printf '%s' "$payload_json" | jq -r '.delta')"

if [[ -z "$event_id" || "$event_id" == "null" ]]; then
  echo "missing event_id in payload: ${payload_json}" >&2
  exit 2
fi
if ! [[ "$event_id" =~ ^[A-Za-z0-9._:-]+$ ]]; then
  echo "invalid event_id format: ${event_id}" >&2
  exit 2
fi
if ! [[ "$account_id" =~ ^[A-Za-z0-9._:-]+$ ]]; then
  echo "invalid account_id format: ${account_id}" >&2
  exit 2
fi

maybe_crash "$FAULT_CRASH_AFTER_CONSUME_PCT" "after_consume"
maybe_crash "$FAULT_CRASH_BEFORE_DB_COMMIT_PCT" "before_db_commit"

payload_b64="$(printf '%s' "$payload_json" | base64 -w0)"
prev_offset="$(pg_query_scalar "SELECT COALESCE((SELECT last_offset FROM consumer_state WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION}), -1);")"
new_offset="$msg_offset"
if (( prev_offset > msg_offset )); then
  new_offset="$prev_offset"
fi

pg_exec_sql "
BEGIN;
WITH ins AS (
  INSERT INTO processed_events(event_id, topic, group_id, partition_id, msg_offset, account_id, delta, payload)
  VALUES ('${event_id}', '${TOPIC}', '${GROUP}', ${PARTITION}, ${msg_offset}, '${account_id}', ${delta},
          convert_from(decode('${payload_b64}', 'base64'), 'UTF8')::jsonb)
  ON CONFLICT (event_id) DO NOTHING
  RETURNING account_id, delta
)
INSERT INTO account_balances(account_id, balance, updated_at)
SELECT account_id, delta, now() FROM ins
ON CONFLICT (account_id) DO UPDATE
SET balance = account_balances.balance + EXCLUDED.balance,
    updated_at = now();

INSERT INTO consumer_state(topic, group_id, partition_id, last_offset, updated_at)
VALUES ('${TOPIC}', '${GROUP}', ${PARTITION}, ${new_offset}, now())
ON CONFLICT (topic, group_id, partition_id) DO UPDATE
SET last_offset = GREATEST(consumer_state.last_offset, EXCLUDED.last_offset),
    updated_at = now();

INSERT INTO consumer_state_history(topic, group_id, partition_id, prev_offset, new_offset)
VALUES ('${TOPIC}', '${GROUP}', ${PARTITION}, ${prev_offset}, ${new_offset});
COMMIT;
"

maybe_crash "$FAULT_CRASH_AFTER_DB_COMMIT_PCT" "after_db_commit_before_offset_commit"

commit_payload="$(jq -cn --arg topic "$TOPIC" --arg group "$GROUP" --argjson partition "$PARTITION" --argjson offset "$msg_offset" '{topic:$topic,group:$group,partition:$partition,offset:$offset}')"
commit_ok=0
commit_detail="ok"

for attempt in $(seq 1 "$COMMIT_RETRIES"); do
  set +e
  cbody="$(curl -sS --max-time "$CONSUME_TIMEOUT_SEC" -X POST "${AYDER_BASE}/broker/commit" -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$commit_payload")"
  crc=$?
  set -e
  if [[ $crc -eq 0 ]] && echo "$cbody" | jq -e '.ok==true' >/dev/null 2>&1; then
    commit_ok=1
    break
  fi
  commit_detail="$(echo "$cbody" | jq -r '.error // "curl_or_http_error"' 2>/dev/null || echo "curl_or_http_error")"
  sleep "$(awk "BEGIN{printf \"%.3f\", ${COMMIT_SLEEP_MS} / 1000.0}")"
done

if (( commit_ok == 1 )); then
  pg_exec_sql "INSERT INTO commit_log(topic, group_id, partition_id, msg_offset, commit_ok, detail) VALUES ('${TOPIC}', '${GROUP}', ${PARTITION}, ${msg_offset}, true, 'ok');"
else
  commit_detail_safe="$(printf '%s' "$commit_detail" | tr -c 'A-Za-z0-9._:-' '_')"
  pg_exec_sql "INSERT INTO commit_log(topic, group_id, partition_id, msg_offset, commit_ok, detail) VALUES ('${TOPIC}', '${GROUP}', ${PARTITION}, ${msg_offset}, false, '${commit_detail_safe}');"
fi

log "processed event_id=${event_id} offset=${msg_offset} commit_ok=${commit_ok}"
exit 0
