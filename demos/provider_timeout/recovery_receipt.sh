#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/provider_timeout/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq

OUT_DIR="${OUT_DIR:-${REPO_ROOT}/artifacts/provider_timeout/adhoc}"
mkdir -p "$OUT_DIR"
RECEIPT_ID="${RECEIPT_ID:-pay_rcpt_$(date -u +%Y%m%dT%H%M%SZ)_$$}"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

pg_json() {
  dc exec -T "$PG_SERVICE" psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -X -A -t -c "$1" \
    | tr -d '\r' | sed '/^$/d'
}

summary_json="$(pg_json "
WITH logical_success AS (
  SELECT idempotency_key, COUNT(*) AS success_count
  FROM payment_attempts
  WHERE attempt_id LIKE 'pay_attempt_%' AND internal_state='SUCCESS_CONFIRMED'
  GROUP BY idempotency_key
),
duplicate_charges AS (
  SELECT COALESCE(SUM(GREATEST(success_count - 1, 0)),0) AS n FROM logical_success
)
SELECT json_build_object(
  'payment_attempts', COUNT(*),
  'confirmed_success', COUNT(*) FILTER (WHERE internal_state='SUCCESS_CONFIRMED'),
  'confirmed_failed', COUNT(*) FILTER (WHERE internal_state='FAILED_CONFIRMED'),
  'ambiguous', COUNT(*) FILTER (WHERE internal_state='AMBIGUOUS'),
  'suppressed_duplicates', COUNT(*) FILTER (WHERE internal_state='SUPPRESSED_DUPLICATE'),
  'duplicate_charges', (SELECT n FROM duplicate_charges),
  'financially_indeterminate_attempts', COUNT(*) FILTER (WHERE internal_state='AMBIGUOUS' AND provider_transaction_id IS NULL)
)::text
FROM payment_attempts
WHERE attempt_id LIKE 'pay_attempt_%';")"

indeterminate_json="$(pg_json "
SELECT COALESCE(json_agg(json_build_object(
  'attempt_id', attempt_id,
  'idempotency_key', idempotency_key,
  'state', internal_state,
  'reason', ambiguity_reason,
  'provider_commitment', 'UNKNOWN',
  'customer_state', customer_state,
  'recommended_action', 'Retry provider verification. Do not retry charge blindly.'
) ORDER BY attempt_id)::text, '[]')
FROM payment_attempts
WHERE attempt_id LIKE 'pay_attempt_%'
  AND internal_state='AMBIGUOUS'
  AND provider_transaction_id IS NULL;")"

attempts_json="$(pg_json "
WITH effect_status AS (
  SELECT
    e.attempt_id,
    json_agg(json_build_object(
      'effect_type', e.effect_type,
      'status', COALESCE(o.status, 'MISSING')
    ) ORDER BY e.effect_type) AS expected_effects
  FROM payment_expected_effects e
  LEFT JOIN payment_observed_effects o
    ON o.attempt_id=e.attempt_id AND o.effect_type=e.effect_type
  WHERE e.attempt_id LIKE 'pay_attempt_%'
  GROUP BY e.attempt_id
)
SELECT COALESCE(json_agg(json_build_object(
  'attempt_id', p.attempt_id,
  'merchant_attempt_id', p.merchant_attempt_id,
  'idempotency_key', p.idempotency_key,
  'amount', p.amount,
  'currency', p.currency,
  'internal_state', p.internal_state,
  'customer_state', p.customer_state,
  'ambiguity_reason', p.ambiguity_reason,
  'provider_transaction_id', p.provider_transaction_id,
  'expected_effects', COALESCE(es.expected_effects, '[]'::json)
) ORDER BY p.attempt_id)::text, '[]')
FROM payment_attempts p
LEFT JOIN effect_status es ON es.attempt_id=p.attempt_id
WHERE p.attempt_id LIKE 'pay_attempt_%';")"

duplicate_charges="$(printf '%s' "$summary_json" | jq -r '.duplicate_charges')"
indeterminate_count="$(printf '%s' "$summary_json" | jq -r '.financially_indeterminate_attempts')"
ambiguous_final_customers="$(pg_query_scalar "
SELECT COUNT(*) FROM payment_attempts
WHERE attempt_id LIKE 'pay_attempt_%'
  AND internal_state='AMBIGUOUS'
  AND customer_state IN ('PAID','FAILED');")"
blind_retries="$(pg_query_scalar "
SELECT COUNT(*) FROM provider_calls c
JOIN payment_attempts p ON p.attempt_id=c.attempt_id
WHERE p.attempt_id LIKE 'pay_attempt_%'
  AND p.ambiguity_reason='provider_timeout'
  AND c.request_type='CHARGE'
  AND c.call_id NOT LIKE 'call_%_charge';")"
bad_idem_keys="$(pg_query_scalar "
WITH live AS (
  SELECT idempotency_key, COUNT(*) AS live_count
  FROM payment_attempts
  WHERE attempt_id LIKE 'pay_attempt_%' AND internal_state <> 'SUPPRESSED_DUPLICATE'
  GROUP BY idempotency_key
)
SELECT COUNT(*) FROM live WHERE live_count > 1;")"

rule() {
  jq -cn --arg id "$1" --arg name "$2" --arg status "$3" --arg detail "$4" \
    '{id:$id,name:$name,status:$status} + (if $detail == "" then {} else {detail:$detail} end)'
}

business_rules_json="$(jq -cs '.' <<EOF
$(rule P1 "no duplicate charge" "$([[ "$duplicate_charges" == "0" ]] && echo PASS || echo FAIL)" "duplicate_charges=${duplicate_charges}")
$(rule P2 "idempotency key maps to one logical payment attempt" "$([[ "$bad_idem_keys" == "0" ]] && echo PASS || echo FAIL)" "")
$(rule P3 "provider commitment known for every payment" "$([[ "$indeterminate_count" == "0" ]] && echo PASS || echo FAIL)" "${indeterminate_count} attempt has UNKNOWN provider commitment")
$(rule P4 "customer state must not be final while provider commitment is unknown" "$([[ "$ambiguous_final_customers" == "0" ]] && echo PASS || echo FAIL)" "ambiguous attempt is customer_state=PENDING_CONFIRMATION")
$(rule P5 "no blind retry after provider timeout" "$([[ "$blind_retries" == "0" ]] && echo PASS || echo FAIL)" "")
EOF
)"

verdict="GREEN"
financial_determinism="PASS"
operator_answer="Payment incident can be closed: every provider commitment is known."
receipt_rc=0
if (( indeterminate_count > 0 )); then
  verdict="YELLOW"
  financial_determinism="FAIL"
  operator_answer="Do not close payment incident. One payment attempt remains financially indeterminate."
  receipt_rc=10
fi
if (( duplicate_charges > 0 )); then
  verdict="RED"
  financial_determinism="FAIL"
  operator_answer="Do not close payment incident. Duplicate charge risk detected."
  receipt_rc=20
fi

receipt_json="$(jq -n \
  --arg receipt_id "$RECEIPT_ID" \
  --arg generated_at "$GENERATED_AT" \
  --arg verdict "$verdict" \
  --arg financial_determinism "$financial_determinism" \
  --arg operator_answer "$operator_answer" \
  --argjson summary "$summary_json" \
  --argjson indeterminate_attempts "$indeterminate_json" \
  --argjson business_rules "$business_rules_json" \
  --argjson attempts "$attempts_json" \
  '{
    receipt_id:$receipt_id,
    generated_at:$generated_at,
    verdict:$verdict,
    financial_determinism:$financial_determinism,
    operator_answer:$operator_answer,
    summary:$summary,
    indeterminate_attempts:$indeterminate_attempts,
    business_rules:$business_rules,
    attempts:$attempts
  }')"

echo "$receipt_json" > "${OUT_DIR}/receipt.json"

summary_b64="$(printf '%s' "$summary_json" | base64 -w0)"
indeterminate_b64="$(printf '%s' "$indeterminate_json" | base64 -w0)"
rules_b64="$(printf '%s' "$business_rules_json" | base64 -w0)"
operator_safe="$(printf '%s' "$operator_answer" | sed "s/'/''/g")"
pg_exec_sql "
INSERT INTO provider_timeout_receipts(receipt_id, verdict, financial_determinism, operator_answer,
  summary, indeterminate_attempts, business_rules)
VALUES ('${RECEIPT_ID}', '${verdict}', '${financial_determinism}', '${operator_safe}',
  convert_from(decode('${summary_b64}', 'base64'), 'UTF8')::jsonb,
  convert_from(decode('${indeterminate_b64}', 'base64'), 'UTF8')::jsonb,
  convert_from(decode('${rules_b64}', 'base64'), 'UTF8')::jsonb)
ON CONFLICT DO NOTHING;
"

{
  echo "============================================================"
  echo " AYDER PAYMENT RECOVERY RECEIPT"
  echo "============================================================"
  echo " receipt_id              : ${RECEIPT_ID}"
  echo " generated               : ${GENERATED_AT}"
  echo " verdict                 : ${verdict}"
  echo " financial_determinism   : ${financial_determinism}"
  echo " operator_answer         : ${operator_answer}"
  echo "------------------------------------------------------------"
  echo " SUMMARY:"
  echo "$summary_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " INDETERMINATE ATTEMPTS:"
  if [[ "$(printf '%s' "$indeterminate_json" | jq 'length')" == "0" ]]; then
    echo "   none"
  else
    echo "$indeterminate_json" | jq -r '.[] |
      "   - " + .attempt_id + "\n" +
      "     state: " + .state + "\n" +
      "     provider_commitment: " + .provider_commitment + "\n" +
      "     customer_state: " + .customer_state + "\n" +
      "     action: " + .recommended_action'
  fi
  echo "------------------------------------------------------------"
  echo " BUSINESS RULES:"
  echo "$business_rules_json" | jq -r '.[] | "   [\(.status)] \(.id) \(.name)" + (if .detail then "  (" + .detail + ")" else "" end)'
  echo "============================================================"
  echo " json: ${OUT_DIR}/receipt.json"
} | tee "${OUT_DIR}/receipt.txt"

exit "$receipt_rc"
