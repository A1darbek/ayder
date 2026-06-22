#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/e2e/partial_batch_receipt/env.sh
source "${SCRIPT_DIR}/env.sh"

# Each produced event is one business operation worth +1 to an account balance.
# expected_events is the source of truth for "what should have happened" — the
# recovery receipt reconciles observed effects against exactly this set.
TOTAL_EVENTS="${TOTAL_EVENTS:-10}"
ACCOUNTS="${ACCOUNTS:-5}"
PRODUCE_TIMEOUT_MS="${PRODUCE_TIMEOUT_MS:-5000}"
MAX_RETRIES="${MAX_RETRIES:-8}"
RETRY_SLEEP_MS="${RETRY_SLEEP_MS:-120}"

require_cmd jq
require_cmd curl
require_cmd "$DOCKER_BIN"

ensure_topic

log "producer start events=${TOTAL_EVENTS} accounts=${ACCOUNTS} topic=${TOPIC}"

for i in $(seq 1 "$TOTAL_EVENTS"); do
  event_id="evt-pbr-$(printf '%06d' "$i")"
  account_id="acct-pbr-$(printf '%02d' $(( (i % ACCOUNTS) + 1 )))"
  delta=1
  payload="$(jq -cn \
    --arg event_id "$event_id" \
    --arg account_id "$account_id" \
    --argjson delta "$delta" \
    --argjson seq "$i" \
    '{event_id:$event_id,account_id:$account_id,delta:$delta,seq:$seq}')"

  pg_exec_sql "INSERT INTO expected_events(event_id, account_id, delta, seq)
               VALUES ('${event_id}', '${account_id}', ${delta}, ${i})
               ON CONFLICT (event_id) DO NOTHING;"

  pg_exec_sql "
INSERT INTO expected_effects(event_id, effect_type, account_id, expected_value)
VALUES
  ('${event_id}', 'ledger_update', '${account_id}', '{\"delta\":${delta}}'::jsonb),
  ('${event_id}', 'order_state_updated', '${account_id}', '{\"state\":\"applied\"}'::jsonb),
  ('${event_id}', 'notification_sent', '${account_id}', '{\"channel\":\"email\"}'::jsonb)
ON CONFLICT (event_id, effect_type) DO NOTHING;
"

  attempt=1
  while true; do
    set +e
    resp="$(curl -sS -X POST \
      "${AYDER_BASE}/broker/topics/${TOPIC}/produce?partition=${PARTITION}&timeout_ms=${PRODUCE_TIMEOUT_MS}&idempotency_key=${event_id}" \
      -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$payload")"
    rc=$?
    set -e

    if [[ $rc -eq 0 ]] && echo "$resp" | jq -e '.ok==true' >/dev/null 2>&1; then
      break
    fi

    if (( attempt >= MAX_RETRIES )); then
      echo "producer failed for ${event_id} after ${MAX_RETRIES} retries: ${resp}" >&2
      exit 1
    fi

    sleep "$(awk "BEGIN{printf \"%.3f\", ${RETRY_SLEEP_MS} / 1000.0}")"
    attempt=$((attempt + 1))
  done
done

log "producer complete: ${TOTAL_EVENTS} events into ${TOPIC}/${PARTITION}"
