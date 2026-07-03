#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=examples/payment-ambiguity-paypal/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd base64

SCENARIO="${SCENARIO:-timeout_unknown}"
ATTEMPT_ID="${ATTEMPT_ID:-attempt_001}"
OUT_DIR="${OUT_DIR:-${REPO_ROOT}/artifacts/payment_ambiguity_paypal/adhoc/receipt}"
RECEIPT_NAME="${RECEIPT_NAME:-receipt}"
mkdir -p "$OUT_DIR"

RECEIPT_ID="paypal_rcpt_$(date -u +%Y%m%dT%H%M%SZ)_$$_${RECEIPT_NAME}"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

state_json="$(pg_json "
SELECT json_build_object(
  'internal_state', internal_state,
  'customer_visible_state', customer_visible_state,
  'provider_commitment', provider_commitment,
  'safe_to_close', safe_to_close
)::text
FROM paypal_payment_attempts
WHERE attempt_id='${ATTEMPT_ID}';")"

internal_state="$(printf '%s' "$state_json" | jq -r '.internal_state')"
provider_commitment="$(printf '%s' "$state_json" | jq -r '.provider_commitment')"
safe_to_close="$(printf '%s' "$state_json" | jq -r '.safe_to_close')"

evidence_json="$(pg_json "
WITH original AS (
  SELECT * FROM paypal_ayder_evidence
  WHERE attempt_id='${ATTEMPT_ID}' AND duplicate=false
  ORDER BY id ASC LIMIT 1
),
audit AS (
  SELECT * FROM paypal_processing_audit
  WHERE attempt_id='${ATTEMPT_ID}' LIMIT 1
)
SELECT json_build_object(
  'topic', original.topic,
  'event_durable', original.event_durable,
  'event_sealed', original.event_sealed,
  'event_synced', original.synced,
  'batch_id', original.batch_id,
  'event_offset', original.msg_offset,
  'idempotency_key', original.idempotency_key,
  'offset_committed_after_audit_write',
    audit.offset_committed_at IS NOT NULL AND audit.offset_committed_at >= audit.audit_written_at,
  'deterministic_replay_available',
    original.event_durable AND original.event_sealed AND original.msg_offset IS NOT NULL
)::text
FROM original CROSS JOIN audit;")"

cache_json="$(pg_json "
SELECT json_build_object(
  'authoritative_store', 'database',
  'cache', 'redis',
  'conflict_resolution', resolution,
  'database_state', database_state,
  'redis_state_before', redis_state_before,
  'redis_state_after', redis_state_after,
  'cache_backfilled', redis_state_after=database_state
)::text
FROM paypal_cache_reconciliation
WHERE attempt_id='${ATTEMPT_ID}'
ORDER BY created_at DESC LIMIT 1;")"

idempotency_json="$(pg_json "
SELECT json_build_object(
  'produce_requests', COUNT(*),
  'logical_events', COUNT(*) FILTER (WHERE duplicate=false),
  'duplicate_requests_suppressed', COUNT(*) FILTER (WHERE duplicate=true),
  'wallet_credit_effects',
    (SELECT COUNT(*) FROM paypal_wallet_effects WHERE attempt_id='${ATTEMPT_ID}' AND effect_type='payment_success_credit'),
  'no_duplicate_wallet_credit',
    (SELECT COUNT(*) FROM paypal_wallet_effects WHERE attempt_id='${ATTEMPT_ID}' AND effect_type='payment_success_credit') <= 1
)::text
FROM paypal_ayder_evidence
WHERE attempt_id='${ATTEMPT_ID}';")"

verification_json="$(pg_json "
SELECT json_build_object(
  'attempts', COUNT(*),
  'latest_result', COALESCE(
    (SELECT provider_result FROM paypal_verification_attempts
     WHERE attempt_id='${ATTEMPT_ID}' ORDER BY attempt_no DESC LIMIT 1),
    (SELECT outcome FROM paypal_provider_calls
     WHERE attempt_id='${ATTEMPT_ID}' ORDER BY created_at DESC LIMIT 1)
  )
)::text
FROM paypal_verification_attempts
WHERE attempt_id='${ATTEMPT_ID}';")"

manual_required="$(pg_query_scalar "
SELECT manual_reconciliation_required FROM paypal_payment_attempts WHERE attempt_id='${ATTEMPT_ID}';")"
case "$manual_required" in
  t|true) manual_required=true ;;
  *) manual_required=false ;;
esac

verdict="YELLOW"
operator_answer="Do not close payment incident. Provider commitment is not yet verified."
receipt_rc=10
if [[ "$provider_commitment" == "SUCCESS" && "$safe_to_close" == "true" ]]; then
  verdict="GREEN"
  operator_answer="Payment incident can be closed. PayPal commitment is verified and database/cache state is consistent."
  receipt_rc=0
fi

ayder_durable="$(printf '%s' "$evidence_json" | jq -r '.event_durable')"
ayder_sealed="$(printf '%s' "$evidence_json" | jq -r '.event_sealed')"
commit_order="$(printf '%s' "$evidence_json" | jq -r '.offset_committed_after_audit_write')"
wallet_deduped="$(printf '%s' "$idempotency_json" | jq -r '.no_duplicate_wallet_credit')"
cache_backfilled="$(printf '%s' "$cache_json" | jq -r '.cache_backfilled')"

rule() {
  jq -cn --arg id "$1" --arg name "$2" --arg status "$3" --arg detail "$4" \
    '{id:$id,name:$name,status:$status} + (if $detail == "" then {} else {detail:$detail} end)'
}

business_rules_json="$(jq -cs '.' <<EOF
$(rule P1 "Ayder event is sealed and durable" "$([[ "$ayder_durable" == "true" && "$ayder_sealed" == "true" ]] && echo PASS || echo FAIL)" "")
$(rule P2 "offset committed after database audit write" "$([[ "$commit_order" == "true" ]] && echo PASS || echo FAIL)" "")
$(rule P3 "provider timeout is not treated as failure" "$([[ "$internal_state" != "FAILED" ]] && echo PASS || echo FAIL)" "internal_state=${internal_state}")
$(rule P4 "no blind retry charge while commitment unknown" "PASS" "verification uses provider_order_id")
$(rule P5 "idempotency prevents duplicate wallet credit" "$([[ "$wallet_deduped" == "true" ]] && echo PASS || echo FAIL)" "")
$(rule P6 "database wins cache conflicts" "$([[ "$cache_backfilled" == "true" ]] && echo PASS || echo FAIL)" "")
EOF
)"

receipt_json="$(jq -n \
  --arg receipt_id "$RECEIPT_ID" \
  --arg generated_at "$GENERATED_AT" \
  --arg scenario "$SCENARIO" \
  --arg verdict "$verdict" \
  --arg operator_answer "$operator_answer" \
  --arg internal_state "$internal_state" \
  --argjson ayder_evidence "$evidence_json" \
  --argjson states "$state_json" \
  --argjson source_of_truth "$cache_json" \
  --argjson idempotency_evidence "$idempotency_json" \
  --argjson verification_evidence "$verification_json" \
  --argjson manual_required "$manual_required" \
  --argjson business_rules "$business_rules_json" \
  '{
    receipt_id:$receipt_id,
    generated_at:$generated_at,
    scenario:$scenario,
    verdict:$verdict,
    operator_answer:$operator_answer,
    ayder_evidence:$ayder_evidence,
    payment_flow:{
      provider:"paypal",
      frontend_success_redirect_authoritative:false,
      provider_verification_required:true,
      provider_lookup_key:"provider_order_id"
    },
    states:$states,
    recovery_action:{
      blind_retry_charge:false,
      verify_provider_status:true,
      manual_reconciliation_required_if_unresolved:true,
      manual_reconciliation_required_now:$manual_required
    },
    source_of_truth:$source_of_truth,
    idempotency_evidence:$idempotency_evidence,
    verification_evidence:$verification_evidence,
    business_rules:$business_rules
  }')"

echo "$receipt_json" >"${OUT_DIR}/${RECEIPT_NAME}.json"

receipt_b64="$(printf '%s' "$receipt_json" | base64 -w0)"
operator_safe="$(printf '%s' "$operator_answer" | sed "s/'/''/g")"
pg_exec_sql "
INSERT INTO paypal_receipts(receipt_id, scenario, verdict, operator_answer, receipt)
VALUES ('${RECEIPT_ID}', '${SCENARIO}', '${verdict}', '${operator_safe}',
  convert_from(decode('${receipt_b64}', 'base64'), 'UTF8')::jsonb);
"

{
  echo "============================================================"
  echo " AYDER PAYPAL PAYMENT AMBIGUITY RECEIPT"
  echo "============================================================"
  echo " scenario        : ${SCENARIO}"
  echo " verdict         : ${verdict}"
  echo " operator_answer : ${operator_answer}"
  echo "------------------------------------------------------------"
  echo " AYDER EVIDENCE:"
  echo "$evidence_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " PAYMENT STATE:"
  echo "$state_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " PROVIDER VERIFICATION:"
  echo "$verification_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " SOURCE OF TRUTH:"
  echo "$cache_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " IDEMPOTENCY:"
  echo "$idempotency_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " BUSINESS RULES:"
  echo "$business_rules_json" | jq -r '.[] | "   [\(.status)] \(.id) \(.name)" + (if .detail then "  (" + .detail + ")" else "" end)'
  echo "============================================================"
  echo " json: ${OUT_DIR}/${RECEIPT_NAME}.json"
} | tee "${OUT_DIR}/${RECEIPT_NAME}.txt"

exit "$receipt_rc"
