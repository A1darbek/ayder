#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/webhook_ordering/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd curl
require_cmd base64

process_webhook() {
  local payload_json="$1"
  local event_id payment_id incoming_state state_rank received_order current_state current_rank
  event_id="$(printf '%s' "$payload_json" | jq -r '.event_id')"
  payment_id="$(printf '%s' "$payload_json" | jq -r '.payment_id')"
  incoming_state="$(printf '%s' "$payload_json" | jq -r '.incoming_state')"
  state_rank="$(printf '%s' "$payload_json" | jq -r '.state_rank')"
  received_order="$(printf '%s' "$payload_json" | jq -r '.received_order')"

  current_state="$(pg_query_scalar "SELECT state FROM webhook_payments WHERE payment_id='${payment_id}';")"
  current_rank="$(pg_query_scalar "SELECT state_rank FROM webhook_payments WHERE payment_id='${payment_id}';")"

  if [[ -z "$current_state" ]]; then
    pg_exec_sql "
BEGIN;
INSERT INTO webhook_payments(payment_id, state, state_rank, success_effect_count)
VALUES ('${payment_id}', '${incoming_state}', ${state_rank}, CASE WHEN '${incoming_state}'='SUCCESS' THEN 1 ELSE 0 END);
INSERT INTO webhook_events(event_id, payment_id, incoming_state, state_rank, received_order, action, reason)
VALUES ('${event_id}', '${payment_id}', '${incoming_state}', ${state_rank}, ${received_order}, 'APPLIED', NULL);
COMMIT;
"
    return
  fi

  if (( state_rank < current_rank )); then
    pg_exec_sql "
BEGIN;
INSERT INTO webhook_events(event_id, payment_id, incoming_state, state_rank, received_order, action, reason)
VALUES ('${event_id}', '${payment_id}', '${incoming_state}', ${state_rank}, ${received_order}, 'IGNORED', 'stale_state_transition')
ON CONFLICT DO NOTHING;
INSERT INTO webhook_suppressed_attempts(event_id, payment_id, reason, incoming_state, current_state, action)
VALUES ('${event_id}', '${payment_id}', 'stale_state_transition', '${incoming_state}', '${current_state}', 'IGNORED')
ON CONFLICT DO NOTHING;
COMMIT;
"
    return
  fi

  if [[ "$incoming_state" == "$current_state" ]]; then
    pg_exec_sql "
BEGIN;
INSERT INTO webhook_events(event_id, payment_id, incoming_state, state_rank, received_order, action, reason)
VALUES ('${event_id}', '${payment_id}', '${incoming_state}', ${state_rank}, ${received_order}, 'SUPPRESSED', 'duplicate_webhook')
ON CONFLICT DO NOTHING;
INSERT INTO webhook_suppressed_attempts(event_id, payment_id, reason, incoming_state, current_state, action)
VALUES ('${event_id}', '${payment_id}', 'duplicate_webhook', '${incoming_state}', '${current_state}', 'SUPPRESSED')
ON CONFLICT DO NOTHING;
COMMIT;
"
    return
  fi

  pg_exec_sql "
BEGIN;
UPDATE webhook_payments
SET state='${incoming_state}',
    state_rank=${state_rank},
    success_effect_count=success_effect_count + CASE WHEN '${incoming_state}'='SUCCESS' THEN 1 ELSE 0 END,
    updated_at=now()
WHERE payment_id='${payment_id}';
INSERT INTO webhook_events(event_id, payment_id, incoming_state, state_rank, received_order, action, reason)
VALUES ('${event_id}', '${payment_id}', '${incoming_state}', ${state_rank}, ${received_order}, 'APPLIED', NULL)
ON CONFLICT DO NOTHING;
COMMIT;
"
}

consume_url="${AYDER_BASE}/broker/consume/${TOPIC}/${GROUP}/${PARTITION}?offset=0&limit=10&encoding=b64"
raw="$(curl -sS -w $'\n%{http_code}' -H "$AUTH_HEADER" "$consume_url")"
http_code="${raw##*$'\n'}"
body="${raw%$'\n'*}"
[[ "$http_code" == "200" ]] || { echo "consume failed http=${http_code}: ${body}" >&2; exit 1; }

count="$(echo "$body" | jq -r '.count // 0')"
[[ "$count" == "3" ]] || { echo "expected 3 webhooks from Ayder, got ${count}" >&2; exit 1; }

for idx in $(seq 0 $((count - 1))); do
  value_b64="$(echo "$body" | jq -r ".messages[${idx}].value_b64")"
  process_webhook "$(printf '%s' "$value_b64" | base64 -d)"
done

payload="$(jq -cn --arg topic "$TOPIC" --arg group "$GROUP" --argjson partition "$PARTITION" --argjson offset 2 \
  '{topic:$topic,group:$group,partition:$partition,offset:$offset}')"
curl -fsS -X POST "${AYDER_BASE}/broker/commit" \
  -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$payload" >/dev/null

log "processed webhook sequence and recorded suppressions"
