#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=examples/data-pipeline-recovery/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd "$DOCKER_BIN"
require_cmd curl

dc up -d "$PG_SERVICE"
wait_postgres || { echo "postgres failed readiness" >&2; exit 1; }

pg_exec_file "${SCRIPT_DIR}/schema.sql"
pg_exec_sql "
TRUNCATE TABLE
  pipeline_recovery_receipts,
  pipeline_replay_history,
  pipeline_processing_audits,
  pipeline_retry_attempts,
  pipeline_page_downloads,
  pipeline_ayder_evidence,
  pipeline_recovery_runs
RESTART IDENTITY CASCADE;
"
ensure_topic

log "data pipeline recovery example initialized"
