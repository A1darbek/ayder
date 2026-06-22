#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/data_pipeline_audit/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd curl
require_cmd base64

consume_url="${AYDER_BASE}/broker/consume/${TOPIC}/${GROUP}/${PARTITION}?offset=0&limit=10&encoding=b64"
raw="$(curl -sS -w $'\n%{http_code}' -H "$AUTH_HEADER" "$consume_url")"
http_code="${raw##*$'\n'}"
body="${raw%$'\n'*}"
[[ "$http_code" == "200" ]] || { echo "consume failed http=${http_code}: ${body}" >&2; exit 1; }

count="$(echo "$body" | jq -r '.count // 0')"
[[ "$count" == "1" ]] || { echo "expected 1 job event from Ayder, got ${count}" >&2; exit 1; }

value_b64="$(echo "$body" | jq -r '.messages[0].value_b64')"
payload_json="$(printf '%s' "$value_b64" | base64 -d)"
job_id="$(printf '%s' "$payload_json" | jq -r '.job_id')"
name="$(printf '%s' "$payload_json" | jq -r '.name')"
stage="$(printf '%s' "$payload_json" | jq -r '.stage')"

pg_exec_sql "
BEGIN;
INSERT INTO pipeline_jobs(job_id, name, stage, status, source_status)
VALUES ('${job_id}', '${name}', '${stage}', 'COMPLETED_WITH_WARNINGS', 'PARTIAL')
ON CONFLICT (job_id) DO UPDATE
SET status=EXCLUDED.status,
    source_status=EXCLUDED.source_status,
    updated_at=now();

INSERT INTO api_retry_attempts(attempt_id, job_id, attempt_no, outcome, error)
VALUES
  ('api_attempt_${job_id}_1', '${job_id}', 1, 'FAILED', 'timeout'),
  ('api_attempt_${job_id}_2', '${job_id}', 2, 'FAILED', 'timeout'),
  ('api_attempt_${job_id}_3', '${job_id}', 3, 'FAILED', 'timeout')
ON CONFLICT DO NOTHING;

INSERT INTO pipeline_data_quality(job_id, input_rows, output_rows, valid_rows, invalid_rows, missing_rows,
  invalid_fields, failed_rules)
VALUES ('${job_id}', 10000, 8420, 8300, 120, 1580,
  '[{\"field\":\"customer_id\",\"invalid_rows\":70},{\"field\":\"amount\",\"invalid_rows\":50}]'::jsonb,
  '[\"input_output_row_reconciliation\",\"required_fields_valid\"]'::jsonb)
ON CONFLICT (job_id) DO UPDATE
SET input_rows=EXCLUDED.input_rows,
    output_rows=EXCLUDED.output_rows,
    valid_rows=EXCLUDED.valid_rows,
    invalid_rows=EXCLUDED.invalid_rows,
    missing_rows=EXCLUDED.missing_rows,
    invalid_fields=EXCLUDED.invalid_fields,
    failed_rules=EXCLUDED.failed_rules;
COMMIT;
"

payload="$(jq -cn --arg topic "$TOPIC" --arg group "$GROUP" --argjson partition "$PARTITION" --argjson offset 0 \
  '{topic:$topic,group:$group,partition:$partition,offset:$offset}')"
curl -fsS -X POST "${AYDER_BASE}/broker/commit" \
  -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$payload" >/dev/null

log "simulated partial API ingestion and data quality evidence"
