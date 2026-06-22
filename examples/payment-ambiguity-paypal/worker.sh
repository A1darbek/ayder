#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=examples/payment-ambiguity-paypal/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd curl
require_cmd base64

SCENARIO="${SCENARIO:-timeout_unknown}"

consume_url="${AYDER_BASE}/broker/consume/${TOPIC}/${GROUP}/${PARTITION}?offset=0&limit=10&encoding=b64"
raw="$(curl -sS -w $'\n%{http_code}' -H "$AUTH_HEADER" "$consume_url")"
http_code="${raw##*$'\n'}"
body="${raw%$'\n'*}"
[[ "$http_code" == "200" ]] || { echo "consume failed http=${http_code}: ${body}" >&2; exit 1; }

count="$(echo "$body" | jq -r '.count // 0')"
[[ "$count" == "1" ]] || { echo "expected exactly 1 logical Ayder event, got ${count}" >&2; exit 1; }

offset="$(echo "$body" | jq -r '.messages[0].offset')"
value_b64="$(echo "$body" | jq -r '.messages[0].value_b64')"
payload_json="$(printf '%s' "$value_b64" | base64 -d)"
attempt_id="$(printf '%s' "$payload_json" | jq -r '.attempt_id')"
idem="$(printf '%s' "$payload_json" | jq -r '.idempotency_key')"
amount="$(printf '%s' "$payload_json" | jq -r '.amount')"
currency="$(printf '%s' "$payload_json" | jq -r '.currency')"
provider_order_id="paypal_order_001"

initial_outcome="TIMEOUT"
if [[ "$SCENARIO" == "happy_path" || "$SCENARIO" == "duplicate_initiation" || "$SCENARIO" == "redis_stale" ]]; then
  initial_outcome="SUCCESS"
fi

pg_exec_sql "
BEGIN;
INSERT INTO paypal_payment_attempts(attempt_id, idempotency_key, provider, provider_order_id,
  amount, currency, internal_state, customer_visible_state, provider_commitment,
  safe_to_close, manual_reconciliation_required)
VALUES ('${attempt_id}', '${idem}', 'paypal', '${provider_order_id}', ${amount}, '${currency}',
  'PENDING', 'PENDING', 'UNKNOWN', false, false);

INSERT INTO paypal_provider_calls(call_id, attempt_id, call_type, outcome, provider_order_id)
VALUES ('provider_create_${attempt_id}', '${attempt_id}', 'CREATE_ORDER', '${initial_outcome}', '${provider_order_id}');

INSERT INTO paypal_processing_audit(audit_id, attempt_id, event_offset, detail)
VALUES ('audit_${attempt_id}', '${attempt_id}', ${offset},
  '{\"db_transaction\":\"committed\",\"state_written\":\"PENDING\",\"provider\":\"paypal\"}'::jsonb);

UPDATE paypal_payment_attempts
SET internal_state=CASE WHEN '${initial_outcome}'='SUCCESS' THEN 'SUCCESS' ELSE 'TIMEOUT' END,
    customer_visible_state=CASE WHEN '${initial_outcome}'='SUCCESS' THEN 'SUCCESS' ELSE 'PENDING' END,
    provider_commitment=CASE WHEN '${initial_outcome}'='SUCCESS' THEN 'SUCCESS' ELSE 'UNKNOWN' END,
    safe_to_close=CASE WHEN '${initial_outcome}'='SUCCESS' THEN true ELSE false END,
    updated_at=now()
WHERE attempt_id='${attempt_id}';

INSERT INTO paypal_wallet_effects(effect_id, attempt_id, effect_type, amount, currency)
SELECT 'wallet_credit_${attempt_id}', '${attempt_id}', 'payment_success_credit', ${amount}, '${currency}'
WHERE '${initial_outcome}'='SUCCESS'
ON CONFLICT (attempt_id, effect_type) DO NOTHING;
COMMIT;
"

commit_payload="$(jq -cn --arg topic "$TOPIC" --arg group "$GROUP" \
  --argjson partition "$PARTITION" --argjson offset "$offset" \
  '{topic:$topic,group:$group,partition:$partition,offset:$offset}')"
curl -fsS -X POST "${AYDER_BASE}/broker/commit" \
  -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$commit_payload" >/dev/null

pg_exec_sql "
UPDATE paypal_processing_audit
SET offset_committed_at=now(),
    detail=detail || '{\"offset_commit\":\"after_audit_write\"}'::jsonb
WHERE audit_id='audit_${attempt_id}';
"

log "worker wrote DB/audit then committed Ayder offset=${offset}"
