#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/dlq_redrive_safety/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq

OUT_DIR="${OUT_DIR:-${REPO_ROOT}/artifacts/dlq_redrive_safety/adhoc}"
mkdir -p "$OUT_DIR"
RECEIPT_ID="${RECEIPT_ID:-dlq_rcpt_$(date -u +%Y%m%dT%H%M%SZ)_$$}"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

pg_json() {
  dc exec -T "$PG_SERVICE" psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -X -A -t -c "$1" \
    | tr -d '\r' | sed '/^$/d'
}

dlq_summary_json="$(pg_json "
SELECT json_build_object(
  'items', COUNT(*),
  'safe_to_redrive', COUNT(*) FILTER (WHERE replay_safety='SAFE'),
  'manual_review_required', COUNT(*) FILTER (WHERE replay_safety <> 'SAFE')
)::text
FROM sqs_dlq_items
WHERE message_id LIKE 'msg_%';")"

dlq_items_json="$(pg_json "
SELECT COALESCE(json_agg(json_build_object(
  'message_id', message_id,
  'business_operation', business_operation,
  'failure_reason', failure_reason,
  'last_known_effect', last_known_effect,
  'replay_safety', replay_safety,
  'recommended_action', recommended_action
) ORDER BY message_id)::text, '[]')
FROM sqs_dlq_items
WHERE message_id LIKE 'msg_%';")"

dlq_items="$(printf '%s' "$dlq_summary_json" | jq -r '.items')"
safe_to_redrive="$(printf '%s' "$dlq_summary_json" | jq -r '.safe_to_redrive')"
manual_review="$(printf '%s' "$dlq_summary_json" | jq -r '.manual_review_required')"
continuing_processed="$(pg_query_scalar "SELECT COUNT(*) FROM sqs_messages WHERE status='PROCESSED';")"
failed_attempts="$(pg_query_scalar "SELECT COUNT(*) FROM sqs_processing_attempts WHERE message_id='msg_009' AND outcome='FAILED';")"
unresolved_dlq="$(pg_query_scalar "SELECT COUNT(*) FROM sqs_dlq_items WHERE disposition='UNRESOLVED';")"

verdict="GREEN"
operator_answer="All DLQ items are safe to redrive."
receipt_rc=0
if (( manual_review > 0 )); then
  verdict="YELLOW"
  operator_answer="Do not blindly redrive. One DLQ item represents unresolved business state."
  receipt_rc=10
fi

rule() {
  jq -cn --arg id "$1" --arg name "$2" --arg status "$3" --arg detail "$4" \
    '{id:$id,name:$name,status:$status} + (if $detail == "" then {} else {detail:$detail} end)'
}

business_rules_json="$(jq -cs '.' <<EOF
$(rule D1 "message moved to DLQ after retry budget exhausted" "$([[ "$failed_attempts" == "3" ]] && echo PASS || echo FAIL)" "msg_009 failed ${failed_attempts} times")
$(rule D2 "other messages continue processing" "$([[ "$continuing_processed" == "2" ]] && echo PASS || echo FAIL)" "processed=${continuing_processed}")
$(rule D3 "redrive safety known before replay" "$([[ "$safe_to_redrive" == "$dlq_items" ]] && echo PASS || echo FAIL)" "${manual_review} item requires manual review")
$(rule D4 "unresolved business state is not blindly redriven" "$([[ "$unresolved_dlq" == "1" ]] && echo PASS || echo FAIL)" "disposition=UNRESOLVED")
EOF
)"

receipt_json="$(jq -n \
  --arg receipt_id "$RECEIPT_ID" \
  --arg generated_at "$GENERATED_AT" \
  --arg verdict "$verdict" \
  --arg operator_answer "$operator_answer" \
  --argjson dlq_summary "$dlq_summary_json" \
  --argjson dlq_items "$dlq_items_json" \
  --argjson business_rules "$business_rules_json" \
  '{
    receipt_id:$receipt_id,
    generated_at:$generated_at,
    verdict:$verdict,
    operator_answer:$operator_answer,
    dlq_summary:$dlq_summary,
    dlq_items:$dlq_items,
    business_rules:$business_rules
  }')"

echo "$receipt_json" > "${OUT_DIR}/receipt.json"

summary_b64="$(printf '%s' "$dlq_summary_json" | base64 -w0)"
items_b64="$(printf '%s' "$dlq_items_json" | base64 -w0)"
rules_b64="$(printf '%s' "$business_rules_json" | base64 -w0)"
operator_safe="$(printf '%s' "$operator_answer" | sed "s/'/''/g")"
pg_exec_sql "
INSERT INTO dlq_redrive_receipts(receipt_id, verdict, operator_answer, dlq_summary, dlq_items, business_rules)
VALUES ('${RECEIPT_ID}', '${verdict}', '${operator_safe}',
  convert_from(decode('${summary_b64}', 'base64'), 'UTF8')::jsonb,
  convert_from(decode('${items_b64}', 'base64'), 'UTF8')::jsonb,
  convert_from(decode('${rules_b64}', 'base64'), 'UTF8')::jsonb)
ON CONFLICT DO NOTHING;
"

{
  echo "============================================================"
  echo " AYDER DLQ REDRIVE SAFETY RECEIPT"
  echo "============================================================"
  echo " receipt_id      : ${RECEIPT_ID}"
  echo " generated       : ${GENERATED_AT}"
  echo " verdict         : ${verdict}"
  echo " operator_answer : ${operator_answer}"
  echo "------------------------------------------------------------"
  echo " DLQ SUMMARY:"
  echo "$dlq_summary_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " DLQ ITEMS:"
  echo "$dlq_items_json" | jq -r '.[] |
    "   - " + .message_id + "\n" +
    "     business_operation: " + .business_operation + "\n" +
    "     failure_reason: " + .failure_reason + "\n" +
    "     last_known_effect: " + .last_known_effect + "\n" +
    "     replay_safety: " + .replay_safety + "\n" +
    "     recommended_action: " + .recommended_action'
  echo "------------------------------------------------------------"
  echo " BUSINESS RULES:"
  echo "$business_rules_json" | jq -r '.[] | "   [\(.status)] \(.id) \(.name)" + (if .detail then "  (" + .detail + ")" else "" end)'
  echo "============================================================"
  echo " json: ${OUT_DIR}/receipt.json"
} | tee "${OUT_DIR}/receipt.txt"

exit "$receipt_rc"
