#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/data_pipeline_audit/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd "$DOCKER_BIN"
require_cmd curl

if ! "$DOCKER_BIN" info >/dev/null 2>&1; then
  echo "docker daemon is not reachable from this environment" >&2
  exit 1
fi

log "starting postgres compose stack"
dc up -d "$PG_SERVICE"

log "waiting for postgres readiness"
wait_pg_ready 90 || { echo "postgres failed readiness" >&2; exit 1; }

log "applying data-pipeline-audit schema"
pg_exec_file "${SCRIPT_DIR}/schema.sql"

log "resetting data-pipeline-audit demo rows"
pg_exec_sql "
DELETE FROM pipeline_audit_receipts WHERE receipt_id LIKE 'data_rcpt_%';
DELETE FROM pipeline_data_quality WHERE job_id='job_daily_api_ingestion_001';
DELETE FROM api_retry_attempts WHERE job_id='job_daily_api_ingestion_001';
DELETE FROM pipeline_jobs WHERE job_id='job_daily_api_ingestion_001';
"

log "ensuring ayder topic"
ensure_topic

log "init complete"
