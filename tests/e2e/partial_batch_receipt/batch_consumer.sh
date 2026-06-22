#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/e2e/partial_batch_receipt/env.sh
source "${SCRIPT_DIR}/env.sh"

# ---------------------------------------------------------------------------
# Batch consumer that reproduces the classic partial-batch / offset-commit bug.
#
#   Consumer receives a batch of BATCH_SIZE messages.
#   Processes the first (BATCH_SIZE - POISON_TAIL) successfully.
#   The last POISON_TAIL messages are NOT applied.
#   The offset is still advanced past the WHOLE batch and committed.
#     -> broker looks fully consumed, dashboards green
#     -> those business effects never happened
#
# MODE controls what happens to the un-applied tail messages:
#   silent_skip (default) : dropped on the floor          -> receipt verdict RED
#   dlq                   : parked in the dlq table        -> receipt verdict AMBER
#   correct               : whole batch applied (no bug)   -> receipt verdict GREEN
#
# The bug is deterministic so the demo is reproducible: with TOTAL_EVENTS=10,
# BATCH_SIZE=10, POISON_TAIL=2 you always get "8 confirmed, 2 missing".
# ---------------------------------------------------------------------------
BATCH_SIZE="${BATCH_SIZE:-10}"
POISON_TAIL="${POISON_TAIL:-2}"
MODE="${MODE:-silent_skip}"            # silent_skip | dlq | correct
CONSUME_TIMEOUT_SEC="${CONSUME_TIMEOUT_SEC:-2.0}"
COMMIT_RETRIES="${COMMIT_RETRIES:-5}"
COMMIT_SLEEP_MS="${COMMIT_SLEEP_MS:-120}"
MAX_EMPTY_POLLS="${MAX_EMPTY_POLLS:-3}"

require_cmd jq
require_cmd curl
require_cmd base64
require_cmd "$DOCKER_BIN"

case "$MODE" in
  silent_skip|dlq|correct) ;;
  *) echo "invalid MODE='${MODE}' (want silent_skip|dlq|correct)" >&2; exit 2 ;;
esac

log "batch consumer start mode=${MODE} batch_size=${BATCH_SIZE} poison_tail=${POISON_TAIL}"

observe_effect_sql() {
  local event_id="$1" effect_type="$2" account_id="$3" msg_offset="$4" observed_value="$5"
  printf "INSERT INTO observed_effects(event_id, effect_type, topic, group_id, partition_id, msg_offset, status, observed_value)
VALUES ('%s', '%s', '%s', '%s', %s, %s, 'confirmed', '%s'::jsonb)
ON CONFLICT (event_id, effect_type, topic, group_id, partition_id)
DO UPDATE SET status='confirmed', msg_offset=EXCLUDED.msg_offset, observed_value=EXCLUDED.observed_value, observed_at=now();
" "$event_id" "$effect_type" "$TOPIC" "$GROUP" "$PARTITION" "$msg_offset" "$observed_value"
}

# Apply one event's business effect inside a single Postgres transaction,
# using the same exactly-once discipline as the reference consumer:
# dedupe insert + idempotent balance apply + monotonic offset state + history.
apply_effect() {
  local event_id="$1" account_id="$2" delta="$3" msg_offset="$4" payload_b64="$5"
  local prev_offset new_offset
  prev_offset="$(pg_query_scalar "SELECT COALESCE((SELECT last_offset FROM consumer_state WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION}), -1);")"
  new_offset="$msg_offset"
  if (( prev_offset > msg_offset )); then new_offset="$prev_offset"; fi

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

$(observe_effect_sql "$event_id" "ledger_update" "$account_id" "$msg_offset" "{\"delta\":${delta}}")
$(observe_effect_sql "$event_id" "order_state_updated" "$account_id" "$msg_offset" "{\"state\":\"applied\"}")
$(observe_effect_sql "$event_id" "notification_sent" "$account_id" "$msg_offset" "{\"channel\":\"email\"}")
COMMIT;
"
}

# Simulate a partial downstream outcome: some side effects happened before the
# consumer crashed, but the event was never fully recorded as processed and the
# notification effect is absent. This is the Demo 1.1 operator scenario.
apply_partial_effects() {
  local event_id="$1" account_id="$2" delta="$3" msg_offset="$4"
  pg_exec_sql "
BEGIN;
INSERT INTO account_balances(account_id, balance, updated_at)
VALUES ('${account_id}', ${delta}, now())
ON CONFLICT (account_id) DO UPDATE
SET balance = account_balances.balance + EXCLUDED.balance,
    updated_at = now();

$(observe_effect_sql "$event_id" "ledger_update" "$account_id" "$msg_offset" "{\"delta\":${delta}}")
$(observe_effect_sql "$event_id" "order_state_updated" "$account_id" "$msg_offset" "{\"state\":\"applied\"}")
COMMIT;
"
}

# Park an un-applied message in the DLQ (recoverable backlog).
park_dlq() {
  local event_id="$1" account_id="$2" delta="$3" msg_offset="$4" reason="$5"
  pg_exec_sql "
INSERT INTO dlq(event_id, topic, group_id, partition_id, msg_offset, account_id, delta, reason)
VALUES ('${event_id}', '${TOPIC}', '${GROUP}', ${PARTITION}, ${msg_offset}, '${account_id}', ${delta}, '${reason}')
ON CONFLICT (event_id, topic, group_id, partition_id)
DO UPDATE SET attempts = dlq.attempts + 1, parked_at = now();
"
}

# Advance the broker-committed offset to ${1}. This is the buggy commit: it runs
# after the loop regardless of whether the tail messages were applied.
commit_offset() {
  local off="$1"
  local payload commit_ok=0 detail="ok"
  payload="$(jq -cn --arg topic "$TOPIC" --arg group "$GROUP" \
    --argjson partition "$PARTITION" --argjson offset "$off" \
    '{topic:$topic,group:$group,partition:$partition,offset:$offset}')"
  for _ in $(seq 1 "$COMMIT_RETRIES"); do
    set +e
    cbody="$(curl -sS --max-time "$CONSUME_TIMEOUT_SEC" -X POST "${AYDER_BASE}/broker/commit" \
      -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$payload")"
    crc=$?
    set -e
    if [[ $crc -eq 0 ]] && echo "$cbody" | jq -e '.ok==true' >/dev/null 2>&1; then
      commit_ok=1; break
    fi
    detail="$(echo "$cbody" | jq -r '.error // "curl_or_http_error"' 2>/dev/null || echo err)"
    sleep "$(awk "BEGIN{printf \"%.3f\", ${COMMIT_SLEEP_MS} / 1000.0}")"
  done
  local detail_safe; detail_safe="$(printf '%s' "$detail" | tr -c 'A-Za-z0-9._:-' '_')"
  pg_exec_sql "INSERT INTO commit_log(topic, group_id, partition_id, msg_offset, commit_ok, detail)
               VALUES ('${TOPIC}', '${GROUP}', ${PARTITION}, ${off}, $( ((commit_ok==1)) && echo true || echo false ), '${detail_safe}');"
}

next_offset="$(pg_query_scalar "SELECT COALESCE((SELECT last_offset FROM consumer_state WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION}), -1) + 1;")"
[[ -z "$next_offset" ]] && next_offset=0

empty_polls=0
batches=0
applied_total=0
skipped_total=0

while true; do
  consume_url="${AYDER_BASE}/broker/consume/${TOPIC}/${GROUP}/${PARTITION}?offset=${next_offset}&limit=${BATCH_SIZE}&encoding=b64"
  set +e
  raw="$(curl -sS --max-time "$CONSUME_TIMEOUT_SEC" -w $'\n%{http_code}' -H "$AUTH_HEADER" "$consume_url")"
  rc=$?
  set -e
  if [[ $rc -ne 0 ]]; then
    log "transient consume error rc=${rc} offset=${next_offset}"
    sleep 0.3; continue
  fi
  http_code="${raw##*$'\n'}"
  body="${raw%$'\n'*}"
  if [[ "$http_code" != "200" ]]; then
    log "consume non-200 code=${http_code}"; sleep 0.3; continue
  fi

  count="$(echo "$body" | jq -r '.count // 0')"
  if [[ "$count" == "0" || -z "$count" ]]; then
    empty_polls=$((empty_polls + 1))
    if (( empty_polls >= MAX_EMPTY_POLLS )); then break; fi
    sleep 0.3; continue
  fi
  empty_polls=0
  batches=$((batches + 1))

  # How many of this batch do we actually apply?
  apply_upto="$count"
  if [[ "$MODE" != "correct" ]] && (( POISON_TAIL > 0 )); then
    apply_upto=$(( count - POISON_TAIL ))
    (( apply_upto < 0 )) && apply_upto=0
  fi

  batch_max_offset=-1
  for idx in $(seq 0 $((count - 1))); do
    msg="$(echo "$body" | jq -c ".messages[${idx}]")"
    msg_offset="$(echo "$msg" | jq -r '.offset')"
    value_b64="$(echo "$msg" | jq -r '.value_b64')"
    payload_json="$(printf '%s' "$value_b64" | base64 -d)"
    event_id="$(printf '%s' "$payload_json" | jq -r '.event_id')"
    account_id="$(printf '%s' "$payload_json" | jq -r '.account_id')"
    delta="$(printf '%s' "$payload_json" | jq -r '.delta')"
    payload_b64="$(printf '%s' "$payload_json" | base64 -w0)"
    (( msg_offset > batch_max_offset )) && batch_max_offset="$msg_offset"

    if (( idx < apply_upto )); then
      apply_effect "$event_id" "$account_id" "$delta" "$msg_offset" "$payload_b64"
      applied_total=$((applied_total + 1))
    else
      # Tail message of the batch: the bug leaves it un-applied.
      skipped_total=$((skipped_total + 1))
      if [[ "$MODE" == "dlq" ]]; then
        park_dlq "$event_id" "$account_id" "$delta" "$msg_offset" "partial_batch_skip"
        log "PARKED  event_id=${event_id} offset=${msg_offset} -> dlq"
      elif (( idx == count - 1 )); then
        apply_partial_effects "$event_id" "$account_id" "$delta" "$msg_offset"
        log "PARTIAL event_id=${event_id} offset=${msg_offset} (ledger/order observed, notification missing)"
      else
        log "SKIPPED event_id=${event_id} offset=${msg_offset} (silently dropped, effect lost)"
      fi
    fi
  done

  # THE BUG: commit past the whole batch even though the tail was never applied.
  # In silent_skip/dlq the offset state also jumps to batch_max, so the skipped
  # messages will never be redelivered by normal consumption.
  if [[ "$MODE" != "correct" ]] && (( apply_upto < count )); then
    pg_exec_sql "
INSERT INTO consumer_state(topic, group_id, partition_id, last_offset, updated_at)
VALUES ('${TOPIC}', '${GROUP}', ${PARTITION}, ${batch_max_offset}, now())
ON CONFLICT (topic, group_id, partition_id) DO UPDATE
SET last_offset = GREATEST(consumer_state.last_offset, EXCLUDED.last_offset), updated_at = now();
INSERT INTO consumer_state_history(topic, group_id, partition_id, prev_offset, new_offset)
VALUES ('${TOPIC}', '${GROUP}', ${PARTITION},
        (SELECT COALESCE(MAX(msg_offset),-1) FROM processed_events WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION}),
        ${batch_max_offset});
"
  fi
  commit_offset "$batch_max_offset"

  next_offset=$(( batch_max_offset + 1 ))
  log "batch=${batches} applied=${applied_total} skipped=${skipped_total} committed_offset=${batch_max_offset}"
done

log "batch consumer done batches=${batches} applied=${applied_total} skipped=${skipped_total} mode=${MODE}"
