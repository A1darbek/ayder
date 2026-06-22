#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=examples/data-pipeline-recovery/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd curl
require_cmd base64

SCENARIO="${SCENARIO:-partial_download}"
JOB_RUN_ID="${JOB_RUN_ID:-job_run_001}"
IDEMPOTENCY_KEY="${IDEMPOTENCY_KEY:-download_job_run_001}"

payload="$(jq -cn \
  --arg event_type "data.download.requested" \
  --arg job_run_id "$JOB_RUN_ID" \
  --arg job_name "daily_external_api_ingestion" \
  --arg stage "bronze_to_silver" \
  --arg external_source "external_api" \
  --arg scenario "$SCENARIO" \
  --argjson expected_pages 10 \
  '{event_type:$event_type,job_run_id:$job_run_id,job_name:$job_name,
    stage:$stage,external_source:$external_source,scenario:$scenario,
    expected_pages:$expected_pages}')"

response="$(curl -sS -X POST \
  "${AYDER_BASE}/broker/topics/${TOPIC}/produce?partition=${PARTITION}&timeout_ms=5000&idempotency_key=${IDEMPOTENCY_KEY}" \
  -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$payload")"
echo "$response" | jq -e '.ok==true' >/dev/null || {
  echo "produce failed: $response" >&2
  exit 1
}

response_b64="$(printf '%s' "$response" | base64 -w0)"
durable="$(echo "$response" | jq -r '.durable // (.batch_id > 0) // false')"
sealed="$(echo "$response" | jq -r '.sealed // false')"
synced="$(echo "$response" | jq -r 'if .synced == null then "null" else (.synced|tostring) end')"
offset="$(echo "$response" | jq -r '.offset')"
batch_id="$(echo "$response" | jq -r '.batch_id')"

pg_exec_sql "
INSERT INTO pipeline_ayder_evidence(job_run_id, topic, idempotency_key, produce_response,
  msg_offset, batch_id, event_durable, event_sealed, synced)
VALUES ('${JOB_RUN_ID}', '${TOPIC}', '${IDEMPOTENCY_KEY}',
  convert_from(decode('${response_b64}', 'base64'), 'UTF8')::jsonb,
  ${offset}, ${batch_id}, ${durable}, ${sealed}, ${synced});
"
printf '%s\n' "$response" >"${ARTIFACT_DIR}/produce.json"

log "produced data.download.requested scenario=${SCENARIO}"
