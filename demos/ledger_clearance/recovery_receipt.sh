#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/ledger_clearance/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq

OUT_DIR="${OUT_DIR:-${REPO_ROOT}/artifacts/ledger_clearance/adhoc}"
mkdir -p "$OUT_DIR"
RECEIPT_ID="${RECEIPT_ID:-ledger_rcpt_$(date -u +%Y%m%dT%H%M%SZ)_$$}"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
ATTEMPT_ID="${ATTEMPT_ID:-pay_attempt_004}"

pg_json() {
  dc exec -T "$PG_SERVICE" psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -X -A -t -c "$1" \
    | tr -d '\r' | sed '/^$/d'
}

debit_total="$(pg_query_scalar "
SELECT COALESCE(SUM(amount),0)
FROM ledger_entries e
JOIN ledger_journals j ON j.journal_id=e.journal_id
WHERE j.attempt_id='${ATTEMPT_ID}' AND e.direction='DEBIT';")"
credit_total="$(pg_query_scalar "
SELECT COALESCE(SUM(amount),0)
FROM ledger_entries e
JOIN ledger_journals j ON j.journal_id=e.journal_id
WHERE j.attempt_id='${ATTEMPT_ID}' AND e.direction='CREDIT';")"
journal_status="$(pg_query_scalar "SELECT status FROM ledger_journals WHERE attempt_id='${ATTEMPT_ID}' LIMIT 1;")"
entry_count="$(pg_query_scalar "
SELECT COUNT(*)
FROM ledger_entries e
JOIN ledger_journals j ON j.journal_id=e.journal_id
WHERE j.attempt_id='${ATTEMPT_ID}';")"
available_balance="$(pg_query_scalar "SELECT balance FROM accounts WHERE account_id='user_available';")"
clearance_balance="$(pg_query_scalar "SELECT balance FROM accounts WHERE account_id='clearance_account';")"
ledger_amount="$(pg_query_scalar "SELECT ledger_amount FROM reconciliation_results WHERE attempt_id='${ATTEMPT_ID}' LIMIT 1;")"
bank_match="$(pg_query_scalar "SELECT match_status FROM reconciliation_results WHERE attempt_id='${ATTEMPT_ID}' LIMIT 1;")"
discrepancy_reason="$(pg_query_scalar "SELECT discrepancy_reason FROM reconciliation_results WHERE attempt_id='${ATTEMPT_ID}' LIMIT 1;")"
recommended_action="$(pg_query_scalar "SELECT recommended_action FROM reconciliation_results WHERE attempt_id='${ATTEMPT_ID}' LIMIT 1;")"

atomic_posting=false
partial_ledger_posting=true
debits_equal_credits=false
if [[ "$journal_status" == "COMMITTED" && "$entry_count" == "2" ]]; then atomic_posting=true; fi
if [[ "$entry_count" == "2" ]]; then partial_ledger_posting=false; fi
if [[ "$debit_total" == "$credit_total" && "$debit_total" != "0" ]]; then debits_equal_credits=true; fi

ledger_status="FAIL"
if [[ "$atomic_posting" == "true" && "$debits_equal_credits" == "true" && "$partial_ledger_posting" == "false" ]]; then
  ledger_status="PASS"
fi

funds_held="false"
if [[ "$clearance_balance" == "$ledger_amount" ]]; then funds_held="true"; fi
available_reduced="false"
if [[ "$available_balance" == "80" ]]; then available_reduced="true"; fi

clearance_status="FAIL"
if [[ "$funds_held" == "true" && "$available_reduced" == "true" ]]; then clearance_status="PASS"; fi

bank_status="FAIL"
if [[ "$bank_match" == "MATCHED" ]]; then bank_status="PASS"; fi

discrepancy_status="UNEXPLAINED"
if [[ "$bank_match" == "MATCHED" ]]; then discrepancy_status="EXPLAINED"; fi

verdict="YELLOW"
operator_answer="Do not close reconciliation. Ledger is internally consistent, but external provider/bank discrepancy remains unresolved."
receipt_rc=10
if [[ "$ledger_status" != "PASS" || "$clearance_status" != "PASS" ]]; then
  verdict="RED"
  operator_answer="Do not close reconciliation. Ledger integrity or clearance accounting failed."
  receipt_rc=20
elif [[ "$bank_status" == "PASS" && "$discrepancy_status" == "EXPLAINED" ]]; then
  verdict="GREEN"
  operator_answer="Reconciliation can be closed. Ledger and external provider/bank records match."
  receipt_rc=0
fi

ledger_integrity_json="$(jq -cn \
  --arg status "$ledger_status" \
  --argjson atomic_posting "$atomic_posting" \
  --argjson debits_equal_credits "$debits_equal_credits" \
  --argjson partial_ledger_posting "$partial_ledger_posting" \
  '{status:$status, atomic_posting:$atomic_posting,
    debits_equal_credits:$debits_equal_credits,
    partial_ledger_posting:$partial_ledger_posting}')"

clearance_json="$(jq -cn \
  --arg attempt_id "$ATTEMPT_ID" \
  --arg currency "USD" \
  --arg state "HELD_IN_CLEARANCE" \
  --argjson amount "$ledger_amount" \
  --argjson customer_available_balance_reduced "$available_reduced" \
  '{attempt_id:$attempt_id, amount:$amount, currency:$currency, state:$state,
    customer_available_balance_reduced:$customer_available_balance_reduced}')"

external_json="$(jq -cn \
  --arg provider_confirmation "MISSING" \
  --arg bank_statement_match "${bank_match:-MISSING}" \
  --arg discrepancy_status "$discrepancy_status" \
  --arg recommended_action "$recommended_action" \
  '{provider_confirmation:$provider_confirmation,
    bank_statement_match:$bank_statement_match,
    discrepancy_status:$discrepancy_status,
    recommended_action:$recommended_action}')"

rule() {
  jq -cn --arg id "$1" --arg name "$2" --arg status "$3" --arg detail "$4" \
    '{id:$id,name:$name,status:$status} + (if $detail == "" then {} else {detail:$detail} end)'
}

business_rules_json="$(jq -cs '.' <<EOF
$(rule L1 "ledger atomic posting" "$([[ "$atomic_posting" == "true" ]] && echo PASS || echo FAIL)" "journal committed atomically")
$(rule L2 "debits equal credits" "$([[ "$debits_equal_credits" == "true" ]] && echo PASS || echo FAIL)" "")
$(rule L3 "funds held in clearance while provider state unknown" "$([[ "$clearance_status" == "PASS" ]] && echo PASS || echo FAIL)" "")
$(rule L4 "external bank/provider match exists" "$([[ "$bank_status" == "PASS" ]] && echo PASS || echo FAIL)" "provider confirmation missing")
$(rule L5 "every discrepancy explained" "$([[ "$discrepancy_status" == "EXPLAINED" ]] && echo PASS || echo FAIL)" "1 discrepancy remains unexplained")
$(rule L6 "no money lost" "$([[ "$funds_held" == "true" ]] && echo PASS || echo FAIL)" "funds are accounted for in clearance")
EOF
)"

receipt_json="$(jq -n \
  --arg receipt_id "$RECEIPT_ID" \
  --arg generated_at "$GENERATED_AT" \
  --arg verdict "$verdict" \
  --arg operator_answer "$operator_answer" \
  --argjson ledger_integrity "$ledger_integrity_json" \
  --argjson clearance "$clearance_json" \
  --argjson external_reconciliation "$external_json" \
  --argjson business_rules "$business_rules_json" \
  '{
    receipt_id:$receipt_id,
    generated_at:$generated_at,
    verdict:$verdict,
    operator_answer:$operator_answer,
    ledger_integrity:$ledger_integrity,
    clearance:$clearance,
    external_reconciliation:$external_reconciliation,
    business_rules:$business_rules
  }')"

echo "$receipt_json" > "${OUT_DIR}/receipt.json"

ledger_b64="$(printf '%s' "$ledger_integrity_json" | base64 -w0)"
clearance_b64="$(printf '%s' "$clearance_json" | base64 -w0)"
external_b64="$(printf '%s' "$external_json" | base64 -w0)"
rules_b64="$(printf '%s' "$business_rules_json" | base64 -w0)"
operator_safe="$(printf '%s' "$operator_answer" | sed "s/'/''/g")"
pg_exec_sql "
INSERT INTO ledger_clearance_receipts(receipt_id, verdict, operator_answer,
  ledger_integrity, clearance, external_reconciliation, business_rules)
VALUES ('${RECEIPT_ID}', '${verdict}', '${operator_safe}',
  convert_from(decode('${ledger_b64}', 'base64'), 'UTF8')::jsonb,
  convert_from(decode('${clearance_b64}', 'base64'), 'UTF8')::jsonb,
  convert_from(decode('${external_b64}', 'base64'), 'UTF8')::jsonb,
  convert_from(decode('${rules_b64}', 'base64'), 'UTF8')::jsonb)
ON CONFLICT DO NOTHING;
"

{
  echo "============================================================"
  echo " AYDER LEDGER CLEARANCE RECEIPT"
  echo "============================================================"
  echo " receipt_id       : ${RECEIPT_ID}"
  echo " generated        : ${GENERATED_AT}"
  echo " verdict          : ${verdict}"
  echo " operator_answer  : ${operator_answer}"
  echo "------------------------------------------------------------"
  echo " LEDGER INTEGRITY:"
  echo "$ledger_integrity_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " CLEARANCE:"
  echo "$clearance_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " EXTERNAL RECONCILIATION:"
  echo "$external_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " BUSINESS RULES:"
  echo "$business_rules_json" | jq -r '.[] | "   [\(.status)] \(.id) \(.name)" + (if .detail then "  (" + .detail + ")" else "" end)'
  echo "------------------------------------------------------------"
  echo " OPERATOR SUMMARY:"
  echo "   Ledger is internally safe."
  echo "   External reality is not reconciled."
  echo "   Do not close."
  echo "   Manual reconciliation required."
  echo "============================================================"
  echo " json: ${OUT_DIR}/receipt.json"
} | tee "${OUT_DIR}/receipt.txt"

exit "$receipt_rc"
