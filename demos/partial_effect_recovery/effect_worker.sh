#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/partial_effect_recovery/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd curl
require_cmd base64

SCENARIO="${SCENARIO:-partial_missing_notification}"
EVENT_ID="${EVENT_ID:-evt_order_001}"

if [[ "$SCENARIO" == "fully_missing_event" ]]; then
  log "scenario=${SCENARIO}: leaving event unconsumed with no effect audit"
  exit 0
fi

consume_url="${AYDER_BASE}/broker/consume/${TOPIC}/${GROUP}/${PARTITION}?offset=0&limit=1&encoding=b64"
body=""
count="0"
for attempt in $(seq 1 30); do
  raw="$(curl -sS -w $'\n%{http_code}' -H "$AUTH_HEADER" "$consume_url")"
  http_code="${raw##*$'\n'}"
  body="${raw%$'\n'*}"
  [[ "$http_code" == "200" ]] || { echo "consume failed http=${http_code}: ${body}" >&2; exit 1; }

  count="$(echo "$body" | jq -r '.count // 0')"
  if [[ "$count" == "1" ]]; then
    break
  fi
  if [[ "$attempt" == "30" ]]; then
    echo "expected 1 order event from Ayder, got ${count}" >&2
    exit 1
  fi
  sleep 0.25
done

value_b64="$(echo "$body" | jq -r '.messages[0].value_b64')"
payload="$(printf '%s' "$value_b64" | base64 -d)"
actual_event_id="$(printf '%s' "$payload" | jq -r '.event_id')"
[[ "$actual_event_id" == "$EVENT_ID" ]] || { echo "unexpected event_id=${actual_event_id}" >&2; exit 1; }

insert_audit_sql=""
case "$SCENARIO" in
  all_confirmed)
    insert_audit_sql="
INSERT INTO partial_effect_audit(event_id, effect_name, status, attempt_no, detail) VALUES
  ('${EVENT_ID}', 'order_state_updated', 'CONFIRMED', 1, 'order moved to CONFIRMED'),
  ('${EVENT_ID}', 'ledger_entry_created', 'CONFIRMED', 1, 'ledger entry recorded'),
  ('${EVENT_ID}', 'customer_notification_sent', 'CONFIRMED', 1, 'notification delivered');"
    ;;
  partial_missing_notification)
    insert_audit_sql="
INSERT INTO partial_effect_audit(event_id, effect_name, status, attempt_no, detail) VALUES
  ('${EVENT_ID}', 'order_state_updated', 'CONFIRMED', 1, 'order moved to CONFIRMED'),
  ('${EVENT_ID}', 'ledger_entry_created', 'CONFIRMED', 1, 'ledger entry recorded'),
  ('${EVENT_ID}', 'customer_notification_sent', 'MISSING', 1, 'notification side effect absent');"
    ;;
  ambiguous_ledger)
    insert_audit_sql="
INSERT INTO partial_effect_audit(event_id, effect_name, status, attempt_no, detail) VALUES
  ('${EVENT_ID}', 'order_state_updated', 'CONFIRMED', 1, 'order moved to CONFIRMED'),
  ('${EVENT_ID}', 'ledger_entry_created', 'UNKNOWN', 1, 'ledger write acknowledgement lost'),
  ('${EVENT_ID}', 'customer_notification_sent', 'MISSING', 1, 'notification side effect absent');"
    ;;
  duplicate_risk)
    insert_audit_sql="
INSERT INTO partial_effect_audit(event_id, effect_name, status, attempt_no, detail) VALUES
  ('${EVENT_ID}', 'order_state_updated', 'CONFIRMED', 1, 'order moved to CONFIRMED'),
  ('${EVENT_ID}', 'order_state_updated', 'CONFIRMED', 2, 'second confirmed order-state write observed'),
  ('${EVENT_ID}', 'ledger_entry_created', 'CONFIRMED', 1, 'ledger entry recorded'),
  ('${EVENT_ID}', 'customer_notification_sent', 'MISSING', 1, 'notification side effect absent');"
    ;;
  *)
    echo "unknown SCENARIO=${SCENARIO}" >&2
    exit 2
    ;;
esac

pg_exec_sql "
BEGIN;
DELETE FROM partial_effect_audit WHERE event_id='${EVENT_ID}';
${insert_audit_sql}
UPDATE partial_effect_events
SET consumed=true, updated_at=now()
WHERE event_id='${EVENT_ID}';
COMMIT;
"

payload_commit="$(jq -cn --arg topic "$TOPIC" --arg group "$GROUP" --argjson partition "$PARTITION" --argjson offset 0 \
  '{topic:$topic,group:$group,partition:$partition,offset:$offset}')"
curl -fsS -X POST "${AYDER_BASE}/broker/commit" \
  -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$payload_commit" >/dev/null

pg_exec_sql "
UPDATE partial_effect_events
SET offset_committed=true, updated_at=now()
WHERE event_id='${EVENT_ID}';
"

log "scenario=${SCENARIO}: effect audit recorded and Ayder offset committed"
