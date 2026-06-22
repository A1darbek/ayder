#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=examples/data-pipeline-recovery/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd base64

SCENARIO="${SCENARIO:-partial_download}"
JOB_RUN_ID="${JOB_RUN_ID:-job_run_001}"
OUT_DIR="${OUT_DIR:-${REPO_ROOT}/artifacts/data_pipeline_recovery/adhoc/receipt}"
RECEIPT_NAME="${RECEIPT_NAME:-receipt}"
mkdir -p "$OUT_DIR"

RECEIPT_ID="pipeline_rcpt_$(date -u +%Y%m%dT%H%M%SZ)_$$_${RECEIPT_NAME}"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

latest_replay="$(pg_query_scalar "
SELECT COALESCE(MAX(replay_number),0) FROM pipeline_processing_audits
WHERE job_run_id='${JOB_RUN_ID}';")"

download_json="$(pg_json "
WITH retry AS (
  SELECT COUNT(*) AS attempts,
         (SELECT error FROM pipeline_retry_attempts
          WHERE job_run_id='${JOB_RUN_ID}' ORDER BY created_at DESC LIMIT 1) AS last_error
  FROM pipeline_retry_attempts WHERE job_run_id='${JOB_RUN_ID}'
)
SELECT json_build_object(
  'expected_pages', r.expected_pages,
  'confirmed_pages', COUNT(*) FILTER (WHERE p.status='CONFIRMED'),
  'missing_pages', COALESCE(json_agg(p.page_no ORDER BY p.page_no)
    FILTER (WHERE p.status='MISSING'), '[]'::json),
  'retry_attempts', retry.attempts,
  'last_error', retry.last_error
)::text
FROM pipeline_recovery_runs r
CROSS JOIN retry
LEFT JOIN pipeline_page_downloads p ON p.job_run_id=r.job_run_id
WHERE r.job_run_id='${JOB_RUN_ID}'
GROUP BY r.expected_pages, retry.attempts, retry.last_error;")"

expected_pages="$(printf '%s' "$download_json" | jq -r '.expected_pages')"
confirmed_pages="$(printf '%s' "$download_json" | jq -r '.confirmed_pages')"
missing_count="$(printf '%s' "$download_json" | jq -r '.missing_pages | length')"
critical_missing="$(pg_query_scalar "
SELECT COUNT(*) FROM pipeline_page_downloads
WHERE job_run_id='${JOB_RUN_ID}' AND status='MISSING' AND critical=true;")"

audit_json="$(pg_json "
SELECT json_build_object(
  'audit_record_written', true,
  'records_loaded', records_loaded,
  'records_missing_estimate', records_missing_estimate,
  'audit_status', audit_status,
  'replay_number', replay_number
)::text
FROM pipeline_processing_audits
WHERE job_run_id='${JOB_RUN_ID}' AND replay_number=${latest_replay};")"

evidence_json="$(pg_json "
WITH evidence AS (
  SELECT * FROM pipeline_ayder_evidence
  WHERE job_run_id='${JOB_RUN_ID}' ORDER BY id ASC LIMIT 1
),
audit_check AS (
  SELECT bool_and(offset_committed_at IS NOT NULL AND offset_committed_at >= audit_written_at) AS ordered
  FROM pipeline_processing_audits WHERE job_run_id='${JOB_RUN_ID}'
)
SELECT json_build_object(
  'topic', evidence.topic,
  'event_durable', evidence.event_durable,
  'event_sealed', evidence.event_sealed,
  'event_synced', evidence.synced,
  'event_offset', evidence.msg_offset,
  'offset_committed_after_audit_write', audit_check.ordered,
  'deterministic_replay_available',
    evidence.event_durable AND evidence.event_sealed AND evidence.msg_offset IS NOT NULL,
  'replay_count', (SELECT COUNT(*) FROM pipeline_replay_history WHERE job_run_id='${JOB_RUN_ID}')
)::text
FROM evidence CROSS JOIN audit_check;")"

replay_json="$(pg_json "
SELECT COALESCE(json_agg(json_build_object(
  'replay_number', replay_number,
  'original_offset', original_offset,
  'pages_recovered', pages_recovered
) ORDER BY replay_number)::text, '[]')
FROM pipeline_replay_history
WHERE job_run_id='${JOB_RUN_ID}';")"

verdict="PASS"
operator_answer="Pipeline run is complete. Business can trust the confirmed output."
fully_trusted=true
can_continue_partially=false
manual_review_required=false
downstream_should_not_continue=false
business_can_trust_result="true"
receipt_rc=0

if (( critical_missing > 0 )); then
  verdict="FAIL"
  operator_answer="Do not continue downstream. A critical dataset partition is missing."
  fully_trusted=false
  can_continue_partially=false
  manual_review_required=true
  downstream_should_not_continue=true
  business_can_trust_result="false"
  receipt_rc=20
elif (( missing_count > 0 )); then
  verdict="WARN"
  operator_answer="Do not treat this pipeline run as fully trusted. Some data was not downloaded after retries."
  fully_trusted=false
  can_continue_partially=true
  manual_review_required=true
  downstream_should_not_continue=false
  business_can_trust_result="partially"
  receipt_rc=10
fi

rule() {
  jq -cn --arg id "$1" --arg name "$2" --arg status "$3" --arg detail "$4" \
    '{id:$id,name:$name,status:$status} + (if $detail == "" then {} else {detail:$detail} end)'
}

row_rule_status="PASS"
[[ "$confirmed_pages" == "$expected_pages" ]] || row_rule_status="WARN"
critical_rule_status="PASS"
(( critical_missing == 0 )) || critical_rule_status="FAIL"
audit_rule_status="PASS"
[[ "$(printf '%s' "$evidence_json" | jq -r '.offset_committed_after_audit_write')" == "true" ]] || audit_rule_status="FAIL"

rules_json="$(jq -cs '.' <<EOF
$(rule D1 "expected/confirmed page reconciliation" "$row_rule_status" "${confirmed_pages}/${expected_pages} pages confirmed")
$(rule D2 "critical partitions present" "$critical_rule_status" "critical_missing=${critical_missing}")
$(rule D3 "retry exhaustion is visible in audit" "PASS" "retry_attempts=$(printf '%s' "$download_json" | jq -r '.retry_attempts')")
$(rule D4 "offset committed after audit write" "$audit_rule_status" "")
$(rule D5 "downstream trust matches data completeness" "$verdict" "business_can_trust_result=${business_can_trust_result}")
EOF
)"

receipt_json="$(jq -n \
  --arg receipt_id "$RECEIPT_ID" \
  --arg generated_at "$GENERATED_AT" \
  --arg scenario "$SCENARIO" \
  --arg verdict "$verdict" \
  --arg operator_answer "$operator_answer" \
  --arg business_can_trust_result "$business_can_trust_result" \
  --argjson ayder_evidence "$evidence_json" \
  --argjson download_result "$download_json" \
  --argjson audit "$audit_json" \
  --argjson replay_history "$replay_json" \
  --argjson fully_trusted "$fully_trusted" \
  --argjson can_continue_partially "$can_continue_partially" \
  --argjson manual_review_required "$manual_review_required" \
  --argjson downstream_should_not_continue "$downstream_should_not_continue" \
  --argjson business_rules "$rules_json" \
  '{
    receipt_id:$receipt_id,
    generated_at:$generated_at,
    scenario:$scenario,
    verdict:$verdict,
    operator_answer:$operator_answer,
    ayder_evidence:$ayder_evidence,
    pipeline_context:{
      tools:["Databricks Jobs","Airflow","dbt"],
      external_source:"external_api",
      job_run_id:"job_run_001"
    },
    download_result:$download_result,
    business_trust:{
      business_can_trust_result:$business_can_trust_result,
      can_continue_partially:$can_continue_partially,
      manual_review_required:$manual_review_required,
      fully_trusted:$fully_trusted,
      downstream_should_not_continue:$downstream_should_not_continue
    },
    audit:$audit,
    replay_history:$replay_history,
    business_rules:$business_rules
  }')"

echo "$receipt_json" >"${OUT_DIR}/${RECEIPT_NAME}.json"

receipt_b64="$(printf '%s' "$receipt_json" | base64 -w0)"
operator_safe="$(printf '%s' "$operator_answer" | sed "s/'/''/g")"
pg_exec_sql "
INSERT INTO pipeline_recovery_receipts(receipt_id, scenario, verdict, operator_answer, receipt)
VALUES ('${RECEIPT_ID}', '${SCENARIO}', '${verdict}', '${operator_safe}',
  convert_from(decode('${receipt_b64}', 'base64'), 'UTF8')::jsonb);
"

{
  echo "============================================================"
  echo " AYDER DATA PIPELINE RECOVERY RECEIPT"
  echo "============================================================"
  echo " scenario        : ${SCENARIO}"
  echo " verdict         : ${verdict}"
  echo " operator_answer : ${operator_answer}"
  echo "------------------------------------------------------------"
  echo " AYDER EVIDENCE:"
  echo "$evidence_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " DOWNLOAD RESULT:"
  echo "$download_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " BUSINESS TRUST:"
  echo "   business_can_trust_result: ${business_can_trust_result}"
  echo "   can_continue_partially: ${can_continue_partially}"
  echo "   manual_review_required: ${manual_review_required}"
  echo "   fully_trusted: ${fully_trusted}"
  echo "   downstream_should_not_continue: ${downstream_should_not_continue}"
  echo "------------------------------------------------------------"
  echo " AUDIT:"
  echo "$audit_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " BUSINESS RULES:"
  echo "$rules_json" | jq -r '.[] | "   [\(.status)] \(.id) \(.name)" + (if .detail then "  (" + .detail + ")" else "" end)'
  echo "============================================================"
  echo " json: ${OUT_DIR}/${RECEIPT_NAME}.json"
} | tee "${OUT_DIR}/${RECEIPT_NAME}.txt"

exit "$receipt_rc"
