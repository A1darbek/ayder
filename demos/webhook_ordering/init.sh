#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/webhook_ordering/env.sh
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

log "applying webhook-ordering schema"
pg_exec_file "${SCRIPT_DIR}/schema.sql"

log "resetting webhook-ordering demo rows"
pg_exec_sql "
DELETE FROM webhook_receipts WHERE payment_id='pay_123';
DELETE FROM webhook_suppressed_attempts WHERE payment_id='pay_123';
DELETE FROM webhook_events WHERE payment_id='pay_123';
DELETE FROM webhook_payments WHERE payment_id='pay_123';
"

log "ensuring ayder topic"
ensure_topic

log "init complete"
