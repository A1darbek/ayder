#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/data_pipeline_audit/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq

OUT_DIR="${OUT_DIR:-${REPO_ROOT}/artifacts/data_pipeline_audit/adhoc}"
mkdir -p "$OUT_DIR"
RECEIPT_ID="${RECEIPT_ID:-data_rcpt_$(date -u +%Y%m%dT%H%M%SZ)_$$}"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
JOB_ID="${JOB_ID:-job_daily_api_ingestion_001}"

pg_json() {
  dc exec -T "$PG_SERVICE" psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -X -A -t -c "$1" \
    | tr -d '\r' | sed '/^$/d'
}

job_json="$(pg_json "
SELECT json_build_object('name', name, 'stage', stage)::text
FROM pipeline_jobs
WHERE job_id='${JOB_ID}';")"

retry_json="$(pg_json "
SELECT json_build_object(
  'api_attempts', COUNT(*),
  'final_error', (SELECT error FROM api_retry_attempts WHERE job_id='${JOB_ID}' ORDER BY attempt_no DESC LIMIT 1),
  'source_status', (SELECT source_status FROM pipeline_jobs WHERE job_id='${JOB_ID}')
)::text
FROM api_retry_attempts
WHERE job_id='${JOB_ID}';")"

quality_json="$(pg_json "
SELECT json_build_object(
  'input_rows', input_rows,
  'output_rows', output_rows,
  'valid_rows', valid_rows,
  'invalid_rows', invalid_rows,
  'missing_rows', missing_rows
)::text
FROM pipeline_data_quality
WHERE job_id='${JOB_ID}';")"

invalid_rows="$(printf '%s' "$quality_json" | jq -r '.invalid_rows')"
missing_rows="$(printf '%s' "$quality_json" | jq -r '.missing_rows')"
input_rows="$(printf '%s' "$quality_json" | jq -r '.input_rows')"
output_rows="$(printf '%s' "$quality_json" | jq -r '.output_rows')"
final_error="$(printf '%s' "$retry_json" | jq -r '.final_error')"
source_status="$(printf '%s' "$retry_json" | jq -r '.source_status')"

rule() {
  jq -cn --arg id "$1" --arg name "$2" --arg status "$3" --arg detail "$4" \
    '{id:$id,name:$name,status:$status} + (if $detail == "" then {} else {detail:$detail} end)'
}

business_rules_json="$(jq -cs '.' <<EOF
$(rule D1 "input/output row reconciliation" "$([[ "$missing_rows" == "0" ]] && echo PASS || echo WARN)" "${input_rows} input rows vs ${output_rows} output rows")
$(rule D2 "required fields valid" "$([[ "$invalid_rows" == "0" ]] && echo PASS || echo FAIL)" "${invalid_rows} rows have invalid required fields")
EOF
)"

verdict="WARN"
operator_answer="Job completed with incomplete source data. Business output requires review."
receipt_rc=10
if [[ "$missing_rows" == "0" && "$invalid_rows" == "0" && "$source_status" == "COMPLETE" ]]; then
  verdict="GREEN"
  operator_answer="Job output is complete and data quality checks passed."
  receipt_rc=0
fi

receipt_json="$(jq -n \
  --arg receipt_id "$RECEIPT_ID" \
  --arg generated_at "$GENERATED_AT" \
  --arg verdict "$verdict" \
  --arg operator_answer "$operator_answer" \
  --argjson job "$job_json" \
  --argjson retry_summary "$retry_json" \
  --argjson data_quality "$quality_json" \
  --argjson business_rules "$business_rules_json" \
  '{
    receipt_id:$receipt_id,
    generated_at:$generated_at,
    verdict:$verdict,
    operator_answer:$operator_answer,
    job:$job,
    retry_summary:$retry_summary,
    data_quality:$data_quality,
    business_rules:$business_rules
  }')"

echo "$receipt_json" > "${OUT_DIR}/receipt.json"

job_b64="$(printf '%s' "$job_json" | base64 -w0)"
retry_b64="$(printf '%s' "$retry_json" | base64 -w0)"
quality_b64="$(printf '%s' "$quality_json" | base64 -w0)"
rules_b64="$(printf '%s' "$business_rules_json" | base64 -w0)"
operator_safe="$(printf '%s' "$operator_answer" | sed "s/'/''/g")"
pg_exec_sql "
INSERT INTO pipeline_audit_receipts(receipt_id, verdict, operator_answer,
  job, retry_summary, data_quality, business_rules)
VALUES ('${RECEIPT_ID}', '${verdict}', '${operator_safe}',
  convert_from(decode('${job_b64}', 'base64'), 'UTF8')::jsonb,
  convert_from(decode('${retry_b64}', 'base64'), 'UTF8')::jsonb,
  convert_from(decode('${quality_b64}', 'base64'), 'UTF8')::jsonb,
  convert_from(decode('${rules_b64}', 'base64'), 'UTF8')::jsonb)
ON CONFLICT DO NOTHING;
"

{
  echo "============================================================"
  echo " AYDER DATA PIPELINE AUDIT RECEIPT"
  echo "============================================================"
  echo " receipt_id      : ${RECEIPT_ID}"
  echo " generated       : ${GENERATED_AT}"
  echo " verdict         : ${verdict}"
  echo " operator_answer : ${operator_answer}"
  echo "------------------------------------------------------------"
  echo " JOB:"
  echo "$job_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " RETRY SUMMARY:"
  echo "$retry_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " DATA QUALITY:"
  echo "$quality_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " BUSINESS RULES:"
  echo "$business_rules_json" | jq -r '.[] | "   [\(.status)] \(.id) \(.name)" + (if .detail then "  (" + .detail + ")" else "" end)'
  echo "============================================================"
  echo " json: ${OUT_DIR}/receipt.json"
} | tee "${OUT_DIR}/receipt.txt"

exit "$receipt_rc"
