#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=examples/payment-webhook-ordering/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd base64

OUT_DIR="${OUT_DIR:-${REPO_ROOT}/artifacts/payment_webhook_ordering/adhoc/receipt}"
mkdir -p "$OUT_DIR"
RECEIPT_ID="webhook_ordering_rcpt_$(date -u +%Y%m%dT%H%M%SZ)_$$"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
PAYMENT_ID="${PAYMENT_ID:-pay_123}"

events_consumed="$(pg_query_scalar "SELECT COUNT(*) FROM ordering_webhook_handling WHERE payment_id='${PAYMENT_ID}';")"
committed_count="$(pg_query_scalar "
SELECT COUNT(*) FROM ordering_webhook_handling
WHERE payment_id='${PAYMENT_ID}' AND offset_committed_at IS NOT NULL;")"
all_durable="$(pg_query_scalar "
SELECT bool_and(event_durable AND event_sealed AND synced)
FROM ordering_ayder_evidence;")"
offset_sequence="$(pg_query_scalar "
SELECT COUNT(*) FROM (
  SELECT msg_offset, row_number() OVER (ORDER BY msg_offset)-1 AS expected_offset
  FROM ordering_ayder_evidence
) x WHERE msg_offset <> expected_offset;")"

final_state="$(pg_query_scalar "SELECT state FROM ordering_payments WHERE payment_id='${PAYMENT_ID}';")"
success_effect_count="$(pg_query_scalar "
SELECT COUNT(*) FROM ordering_business_effects
WHERE payment_id='${PAYMENT_ID}' AND effect_type='payment_success';")"
stale_downgrades="$(pg_query_scalar "
SELECT COUNT(*) FROM ordering_webhook_handling
WHERE payment_id='${PAYMENT_ID}'
  AND incoming_state='PROCESSING'
  AND decision='APPLIED'
  AND current_state_before IN ('SUCCESS','FAILED');")"
illegal_terminal_mutations="$(pg_query_scalar "
SELECT COUNT(*) FROM ordering_webhook_handling
WHERE payment_id='${PAYMENT_ID}'
  AND current_state_before='FAILED'
  AND incoming_state='SUCCESS'
  AND decision='APPLIED';")"

handling_json="$(pg_json "
SELECT json_agg(json_build_object(
  'event', incoming_state,
  'decision', decision
) ORDER BY event_offset)::text
FROM ordering_webhook_handling
WHERE payment_id='${PAYMENT_ID}';")"

offsets_committed=false
[[ "$events_consumed" == "3" && "$committed_count" == "3" ]] && offsets_committed=true
deterministic_replay=false
[[ "$all_durable" == "t" && "$offset_sequence" == "0" ]] && deterministic_replay=true

safe_to_close=false
[[ "$final_state" == "SUCCESS" || "$final_state" == "FAILED" ]] && safe_to_close=true

w1="PASS"
w2="PASS"
w3="PASS"
w4="PASS"
[[ "$safe_to_close" == "true" ]] || w1="FAIL"
[[ "$stale_downgrades" == "0" ]] || w2="FAIL"
[[ "$success_effect_count" == "1" ]] || w3="FAIL"
[[ "$illegal_terminal_mutations" == "0" ]] || w4="FAIL"

verdict="GREEN"
operator_answer="Payment incident can be closed. Payment reached terminal state and duplicate/stale webhooks were safely handled."
receipt_rc=0
if [[ "$w1" != "PASS" || "$w2" != "PASS" || "$w3" != "PASS" || "$w4" != "PASS" \
  || "$offsets_committed" != "true" || "$deterministic_replay" != "true" ]]; then
  verdict="RED"
  operator_answer="Do not close payment incident. Webhook state or replay evidence failed."
  receipt_rc=20
fi

rule() {
  jq -cn --arg id "$1" --arg name "$2" --arg status "$3" \
    '{id:$id,name:$name,status:$status}'
}

rules_json="$(jq -cs '.' <<EOF
$(rule W1 "payment reached terminal state" "$w1")
$(rule W2 "stale state did not downgrade terminal state" "$w2")
$(rule W3 "duplicate webhook did not duplicate business effect" "$w3")
$(rule W4 "failed-to-success requires new payment_id" "$w4")
EOF
)"

receipt_json="$(jq -n \
  --arg receipt_id "$RECEIPT_ID" \
  --arg generated_at "$GENERATED_AT" \
  --arg verdict "$verdict" \
  --arg operator_answer "$operator_answer" \
  --arg topic "$TOPIC" \
  --arg consumer_group "$GROUP" \
  --arg payment_id "$PAYMENT_ID" \
  --arg final_state "$final_state" \
  --argjson events_consumed "$events_consumed" \
  --argjson offsets_committed "$offsets_committed" \
  --argjson deterministic_replay_available "$deterministic_replay" \
  --argjson safe_to_close "$safe_to_close" \
  --argjson webhook_handling "$handling_json" \
  --argjson rules "$rules_json" \
  '{
    receipt_id:$receipt_id,
    generated_at:$generated_at,
    verdict:$verdict,
    operator_answer:$operator_answer,
    ayder_evidence:{
      topic:$topic,
      consumer_group:$consumer_group,
      events_consumed:$events_consumed,
      offsets_committed:$offsets_committed,
      deterministic_replay_available:$deterministic_replay_available
    },
    payment_state:{
      payment_id:$payment_id,
      final_state:$final_state,
      terminal_states:["SUCCESS","FAILED"],
      safe_to_close:$safe_to_close
    },
    webhook_handling:$webhook_handling,
    rules:$rules
  }')"

echo "$receipt_json" >"${OUT_DIR}/receipt.json"

receipt_b64="$(printf '%s' "$receipt_json" | base64 -w0)"
operator_safe="$(printf '%s' "$operator_answer" | sed "s/'/''/g")"
pg_exec_sql "
INSERT INTO ordering_receipts(receipt_id, verdict, operator_answer, receipt)
VALUES ('${RECEIPT_ID}', '${verdict}', '${operator_safe}',
  convert_from(decode('${receipt_b64}', 'base64'), 'UTF8')::jsonb);
"

{
  echo "============================================================"
  echo " AYDER PAYMENT WEBHOOK ORDERING RECEIPT"
  echo "============================================================"
  echo " verdict         : ${verdict}"
  echo " operator_answer : ${operator_answer}"
  echo " payment_id      : ${PAYMENT_ID}"
  echo " final_state     : ${final_state}"
  echo "------------------------------------------------------------"
  echo " AYDER EVIDENCE:"
  echo "   topic: ${TOPIC}"
  echo "   consumer_group: ${GROUP}"
  echo "   events_consumed: ${events_consumed}"
  echo "   offsets_committed: ${offsets_committed}"
  echo "   deterministic_replay_available: ${deterministic_replay}"
  echo "------------------------------------------------------------"
  echo " WEBHOOK HANDLING:"
  echo "$handling_json" | jq -r '.[] | "   - \(.event): \(.decision)"'
  echo "------------------------------------------------------------"
  echo " RULES:"
  echo "$rules_json" | jq -r '.[] | "   [\(.status)] \(.id) \(.name)"'
  echo "============================================================"
  echo " json: ${OUT_DIR}/receipt.json"
} | tee "${OUT_DIR}/receipt.txt"

exit "$receipt_rc"
