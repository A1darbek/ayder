#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/partial_effect_recovery/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd curl
require_cmd base64

EVENT_FILE="${SCRIPT_DIR}/events/order_confirmed.json"
EVENT_ID="$(jq -r '.event_id' "$EVENT_FILE")"
IDEMPOTENCY_KEY="${EVENT_ID}"

ensure_topic

resp=""
for attempt in $(seq 1 30); do
  ensure_topic
  set +e
  resp="$(curl -fsS -X POST \
    "${AYDER_BASE}/broker/topics/${TOPIC}/produce?partition=${PARTITION}&timeout_ms=5000&idempotency_key=${IDEMPOTENCY_KEY}" \
    -H "$AUTH_HEADER" \
    -H 'Content-Type: application/json' \
    --data-binary @"${EVENT_FILE}" 2>/dev/null)"
  rc=$?
  set -e
  if [[ "$rc" == "0" ]] && echo "$resp" | jq -e '.ok == true' >/dev/null 2>&1; then
    break
  fi
  if [[ "$attempt" == "30" ]]; then
    echo "produce failed after ${attempt} attempts: ${resp}" >&2
    exit 1
  fi
  sleep 0.25
done

echo "$resp" | jq -e '.ok == true' >/dev/null

event_b64="$(base64 -w0 "$EVENT_FILE")"
offset="$(echo "$resp" | jq -r '.offset')"
batch_id="$(echo "$resp" | jq -r '.batch_id // 0')"
durable="$(echo "$resp" | jq -r '.durable // (.sealed and .synced) // false')"
sealed="$(echo "$resp" | jq -r '.sealed // false')"
synced="$(echo "$resp" | jq -r '.synced // false')"

if [[ "$batch_id" != "0" && "$batch_id" != "null" ]]; then
  for _ in $(seq 1 40); do
    status_resp="$(curl -fsS "${AYDER_BASE}/admin/sealed/status" -H "$AUTH_HEADER" 2>/dev/null || true)"
    last_synced="$(printf '%s' "$status_resp" | jq -r '.last_synced_batch_id // 0' 2>/dev/null || echo 0)"
    if (( last_synced >= batch_id )); then
      durable="true"
      synced="true"
      break
    fi
    sleep 0.25
  done
fi

event_type="$(jq -r '.event_type' "$EVENT_FILE")"
order_id="$(jq -r '.order_id' "$EVENT_FILE")"
customer_id="$(jq -r '.customer_id' "$EVENT_FILE")"
amount="$(jq -r '.amount' "$EVENT_FILE")"
currency="$(jq -r '.currency' "$EVENT_FILE")"

pg_exec_sql "
BEGIN;
INSERT INTO partial_effect_events(
  event_id, event_type, order_id, customer_id, amount, currency,
  topic, consumer_group, partition, event_offset, idempotency_key,
  event_durable, event_sealed, event_synced, raw_event)
VALUES (
  '${EVENT_ID}', '${event_type}', '${order_id}', '${customer_id}', ${amount}, '${currency}',
  '${TOPIC}', '${GROUP}', ${PARTITION}, ${offset}, '${IDEMPOTENCY_KEY}',
  ${durable}, ${sealed}, ${synced},
  convert_from(decode('${event_b64}', 'base64'), 'UTF8')::jsonb)
ON CONFLICT (event_id) DO UPDATE SET
  event_offset=EXCLUDED.event_offset,
  event_durable=EXCLUDED.event_durable,
  event_sealed=EXCLUDED.event_sealed,
  event_synced=EXCLUDED.event_synced,
  raw_event=EXCLUDED.raw_event,
  updated_at=now();
INSERT INTO partial_effect_expected_effects(event_id, effect_name, ordinal)
VALUES
  ('${EVENT_ID}', 'order_state_updated', 1),
  ('${EVENT_ID}', 'ledger_entry_created', 2),
  ('${EVENT_ID}', 'customer_notification_sent', 3)
ON CONFLICT DO NOTHING;
COMMIT;
"

log "produced ${EVENT_ID} to ${TOPIC} offset=${offset}"
