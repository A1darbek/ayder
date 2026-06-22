#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/webhook_ordering/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq

OUT_DIR="${OUT_DIR:-${REPO_ROOT}/artifacts/webhook_ordering/adhoc}"
mkdir -p "$OUT_DIR"
RECEIPT_ID="${RECEIPT_ID:-webhook_rcpt_$(date -u +%Y%m%dT%H%M%SZ)_$$}"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
PAYMENT_ID="${PAYMENT_ID:-pay_123}"

pg_json() {
  dc exec -T "$PG_SERVICE" psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -X -A -t -c "$1" \
    | tr -d '\r' | sed '/^$/d'
}

final_state="$(pg_query_scalar "SELECT state FROM webhook_payments WHERE payment_id='${PAYMENT_ID}';")"
success_effect_count="$(pg_query_scalar "SELECT success_effect_count FROM webhook_payments WHERE payment_id='${PAYMENT_ID}';")"
stale_suppressed="$(pg_query_scalar "SELECT COUNT(*) FROM webhook_suppressed_attempts WHERE payment_id='${PAYMENT_ID}' AND reason='stale_state_transition';")"
duplicate_suppressed="$(pg_query_scalar "SELECT COUNT(*) FROM webhook_suppressed_attempts WHERE payment_id='${PAYMENT_ID}' AND reason='duplicate_webhook';")"
downgrade_applied="$(pg_query_scalar "
SELECT COUNT(*) FROM webhook_events
WHERE payment_id='${PAYMENT_ID}'
  AND incoming_state='PROCESSING'
  AND action='APPLIED';")"

suppressed_json="$(pg_json "
SELECT COALESCE(json_agg(json_build_object(
  'event_id', event_id,
  'reason', reason,
  'incoming_state', incoming_state,
  'current_state', current_state,
  'action', action
) ORDER BY event_id)::text, '[]')
FROM webhook_suppressed_attempts
WHERE payment_id='${PAYMENT_ID}';")"
[[ -z "$suppressed_json" ]] && suppressed_json='[]'

w1="PASS"
w2="PASS"
w3="PASS"
[[ "$final_state" == "SUCCESS" && "$downgrade_applied" == "0" ]] || w1="FAIL"
[[ "$success_effect_count" == "1" ]] || w2="FAIL"
[[ "$stale_suppressed" == "1" && "$duplicate_suppressed" == "1" ]] || w3="FAIL"

verdict="GREEN"
operator_answer="Payment state is correct. Duplicate and stale webhook attempts were safely suppressed."
receipt_rc=0
if [[ "$w1" != "PASS" || "$w2" != "PASS" || "$w3" != "PASS" ]]; then
  verdict="RED"
  operator_answer="Do not close webhook incident. State-machine or suppression rule failed."
  receipt_rc=20
fi

rule() {
  jq -cn --arg id "$1" --arg name "$2" --arg status "$3" \
    '{id:$id,name:$name,status:$status}'
}

business_rules_json="$(jq -cs '.' <<EOF
$(rule W1 "payment state-machine monotonicity" "$w1")
$(rule W2 "no duplicate payment effect" "$w2")
$(rule W3 "duplicate webhooks safely suppressed" "$w3")
EOF
)"

receipt_json="$(jq -n \
  --arg receipt_id "$RECEIPT_ID" \
  --arg generated_at "$GENERATED_AT" \
  --arg verdict "$verdict" \
  --arg operator_answer "$operator_answer" \
  --arg payment_id "$PAYMENT_ID" \
  --arg final_state "$final_state" \
  --argjson suppressed_attempts "$suppressed_json" \
  --argjson business_rules "$business_rules_json" \
  '{
    receipt_id:$receipt_id,
    generated_at:$generated_at,
    verdict:$verdict,
    operator_answer:$operator_answer,
    payment_id:$payment_id,
    final_state:$final_state,
    suppressed_attempts:$suppressed_attempts,
    business_rules:$business_rules
  }')"

echo "$receipt_json" > "${OUT_DIR}/receipt.json"

suppressed_b64="$(printf '%s' "$suppressed_json" | base64 -w0)"
rules_b64="$(printf '%s' "$business_rules_json" | base64 -w0)"
operator_safe="$(printf '%s' "$operator_answer" | sed "s/'/''/g")"
pg_exec_sql "
INSERT INTO webhook_receipts(receipt_id, verdict, operator_answer, payment_id, final_state,
  suppressed_attempts, business_rules)
VALUES ('${RECEIPT_ID}', '${verdict}', '${operator_safe}', '${PAYMENT_ID}', '${final_state}',
  convert_from(decode('${suppressed_b64}', 'base64'), 'UTF8')::jsonb,
  convert_from(decode('${rules_b64}', 'base64'), 'UTF8')::jsonb)
ON CONFLICT DO NOTHING;
"

{
  echo "============================================================"
  echo " AYDER WEBHOOK ORDERING RECEIPT"
  echo "============================================================"
  echo " receipt_id      : ${RECEIPT_ID}"
  echo " generated       : ${GENERATED_AT}"
  echo " verdict         : ${verdict}"
  echo " operator_answer : ${operator_answer}"
  echo " payment_id      : ${PAYMENT_ID}"
  echo " final_state     : ${final_state}"
  echo "------------------------------------------------------------"
  echo " SUPPRESSED ATTEMPTS:"
  echo "$suppressed_json" | jq -r '.[] |
    "   - " + .event_id + "\n" +
    "     reason: " + .reason + "\n" +
    "     incoming_state: " + .incoming_state + "\n" +
    "     current_state: " + .current_state + "\n" +
    "     action: " + .action'
  echo "------------------------------------------------------------"
  echo " BUSINESS RULES:"
  echo "$business_rules_json" | jq -r '.[] | "   [\(.status)] \(.id) \(.name)"'
  echo "============================================================"
  echo " json: ${OUT_DIR}/receipt.json"
} | tee "${OUT_DIR}/receipt.txt"

exit "$receipt_rc"
