#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=examples/payment-webhook-ordering/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd curl
require_cmd base64

is_terminal() {
  [[ "$1" == "SUCCESS" || "$1" == "FAILED" ]]
}

process_webhook() {
  local payload_json="$1" offset="$2"
  local ingestion_event_id provider_event_id payment_id incoming_state
  local current_state duplicate_count decision effect=false

  ingestion_event_id="$(printf '%s' "$payload_json" | jq -r '.ingestion_event_id')"
  provider_event_id="$(printf '%s' "$payload_json" | jq -r '.provider_event_id')"
  payment_id="$(printf '%s' "$payload_json" | jq -r '.payment_id')"
  incoming_state="$(printf '%s' "$payload_json" | jq -r '.state')"

  current_state="$(pg_query_scalar "SELECT state FROM ordering_payments WHERE payment_id='${payment_id}';")"
  duplicate_count="$(pg_query_scalar "
SELECT COUNT(*) FROM ordering_provider_event_dedupe WHERE provider_event_id='${provider_event_id}';")"

  if (( duplicate_count > 0 )); then
    decision="SUPPRESSED_DUPLICATE"
    pg_exec_sql "
INSERT INTO ordering_webhook_handling(ingestion_event_id, provider_event_id, payment_id,
  incoming_state, current_state_before, decision, business_effect_applied, event_offset)
VALUES ('${ingestion_event_id}', '${provider_event_id}', '${payment_id}',
  '${incoming_state}', '${current_state}', '${decision}', false, ${offset});
"
  elif [[ -z "$current_state" ]]; then
    decision="APPLIED"
    effect=true
    pg_exec_sql "
BEGIN;
INSERT INTO ordering_provider_event_dedupe(provider_event_id, first_ingestion_event_id, payment_id)
VALUES ('${provider_event_id}', '${ingestion_event_id}', '${payment_id}');
INSERT INTO ordering_payments(payment_id, state, terminal, success_effect_count)
VALUES ('${payment_id}', '${incoming_state}',
  CASE WHEN '${incoming_state}' IN ('SUCCESS','FAILED') THEN true ELSE false END,
  CASE WHEN '${incoming_state}'='SUCCESS' THEN 1 ELSE 0 END);
INSERT INTO ordering_business_effects(effect_id, payment_id, effect_type, provider_event_id)
SELECT 'success_effect_${payment_id}', '${payment_id}', 'payment_success', '${provider_event_id}'
WHERE '${incoming_state}'='SUCCESS'
ON CONFLICT (payment_id, effect_type) DO NOTHING;
INSERT INTO ordering_webhook_handling(ingestion_event_id, provider_event_id, payment_id,
  incoming_state, current_state_before, decision, business_effect_applied, event_offset)
VALUES ('${ingestion_event_id}', '${provider_event_id}', '${payment_id}',
  '${incoming_state}', NULL, '${decision}', ${effect}, ${offset});
COMMIT;
"
  elif is_terminal "$current_state"; then
    if [[ "$incoming_state" == "$current_state" ]]; then
      decision="SUPPRESSED_DUPLICATE"
    elif [[ "$incoming_state" == "PROCESSING" ]]; then
      decision="IGNORED_STALE_TRANSITION"
    else
      # SUCCESS <-> FAILED is not legal for the same payment_id.
      decision="REJECTED_TERMINAL_CONFLICT_NEW_PAYMENT_ID_REQUIRED"
    fi

    pg_exec_sql "
BEGIN;
INSERT INTO ordering_provider_event_dedupe(provider_event_id, first_ingestion_event_id, payment_id)
VALUES ('${provider_event_id}', '${ingestion_event_id}', '${payment_id}')
ON CONFLICT DO NOTHING;
INSERT INTO ordering_webhook_handling(ingestion_event_id, provider_event_id, payment_id,
  incoming_state, current_state_before, decision, business_effect_applied, event_offset)
VALUES ('${ingestion_event_id}', '${provider_event_id}', '${payment_id}',
  '${incoming_state}', '${current_state}', '${decision}', false, ${offset});
COMMIT;
"
  else
    decision="APPLIED"
    effect=true
    pg_exec_sql "
BEGIN;
INSERT INTO ordering_provider_event_dedupe(provider_event_id, first_ingestion_event_id, payment_id)
VALUES ('${provider_event_id}', '${ingestion_event_id}', '${payment_id}');
UPDATE ordering_payments
SET state='${incoming_state}',
    terminal=CASE WHEN '${incoming_state}' IN ('SUCCESS','FAILED') THEN true ELSE false END,
    success_effect_count=success_effect_count + CASE WHEN '${incoming_state}'='SUCCESS' THEN 1 ELSE 0 END,
    updated_at=now()
WHERE payment_id='${payment_id}';
INSERT INTO ordering_business_effects(effect_id, payment_id, effect_type, provider_event_id)
SELECT 'success_effect_${payment_id}', '${payment_id}', 'payment_success', '${provider_event_id}'
WHERE '${incoming_state}'='SUCCESS'
ON CONFLICT (payment_id, effect_type) DO NOTHING;
INSERT INTO ordering_webhook_handling(ingestion_event_id, provider_event_id, payment_id,
  incoming_state, current_state_before, decision, business_effect_applied, event_offset)
VALUES ('${ingestion_event_id}', '${provider_event_id}', '${payment_id}',
  '${incoming_state}', '${current_state}', '${decision}', ${effect}, ${offset});
COMMIT;
"
  fi

  commit_payload="$(jq -cn --arg topic "$TOPIC" --arg group "$GROUP" \
    --argjson partition "$PARTITION" --argjson offset "$offset" \
    '{topic:$topic,group:$group,partition:$partition,offset:$offset}')"
  curl -fsS -X POST "${AYDER_BASE}/broker/commit" \
    -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$commit_payload" >/dev/null

  pg_exec_sql "
UPDATE ordering_webhook_handling
SET offset_committed_at=now()
WHERE ingestion_event_id='${ingestion_event_id}';
"

  log "${ingestion_event_id} incoming=${incoming_state} decision=${decision} offset=${offset}"
}

consume_url="${AYDER_BASE}/broker/consume/${TOPIC}/${GROUP}/${PARTITION}?offset=0&limit=10&encoding=b64"
raw="$(curl -sS -w $'\n%{http_code}' -H "$AUTH_HEADER" "$consume_url")"
http_code="${raw##*$'\n'}"
body="${raw%$'\n'*}"
[[ "$http_code" == "200" ]] || { echo "consume failed http=${http_code}: ${body}" >&2; exit 1; }

count="$(echo "$body" | jq -r '.count // 0')"
[[ "$count" == "3" ]] || { echo "expected 3 webhooks from Ayder, got ${count}" >&2; exit 1; }

for idx in $(seq 0 $((count - 1))); do
  offset="$(echo "$body" | jq -r ".messages[${idx}].offset")"
  value_b64="$(echo "$body" | jq -r ".messages[${idx}].value_b64")"
  process_webhook "$(printf '%s' "$value_b64" | base64 -d)" "$offset"
done

log "processed and committed all webhook events"
