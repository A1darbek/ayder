#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/dlq_redrive_safety/env.sh
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

log "applying dlq-redrive schema"
pg_exec_file "${SCRIPT_DIR}/schema.sql"

log "resetting dlq-redrive demo rows"
pg_exec_sql "
DELETE FROM dlq_redrive_receipts WHERE receipt_id LIKE 'dlq_rcpt_%';
DELETE FROM sqs_dlq_items WHERE message_id LIKE 'msg_%';
DELETE FROM sqs_business_effects WHERE message_id LIKE 'msg_%';
DELETE FROM sqs_processing_attempts WHERE message_id LIKE 'msg_%';
DELETE FROM sqs_messages WHERE message_id LIKE 'msg_%';
"

log "ensuring ayder topic"
ensure_topic

log "init complete"
