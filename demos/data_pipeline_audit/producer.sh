#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/data_pipeline_audit/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd curl

ensure_topic

payload="$(jq -cn \
  --arg event_type "api_ingestion_job_started" \
  --arg job_id "job_daily_api_ingestion_001" \
  --arg name "daily_api_ingestion" \
  --arg stage "bronze_to_silver" \
  '{event_type:$event_type,job_id:$job_id,name:$name,stage:$stage}')"

resp="$(curl -sS -X POST \
  "${AYDER_BASE}/broker/topics/${TOPIC}/produce?partition=${PARTITION}&timeout_ms=5000&idempotency_key=job_daily_api_ingestion_001" \
  -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$payload")"

echo "$resp" | jq -e '.ok==true' >/dev/null || {
  echo "produce failed: $resp" >&2
  exit 1
}

log "produced data pipeline ingestion job event"
