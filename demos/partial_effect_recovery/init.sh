#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/partial_effect_recovery/env.sh
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

log "applying partial-effect-recovery schema"
pg_exec_file "${SCRIPT_DIR}/schema.sql"

log "resetting partial-effect-recovery demo rows"
pg_exec_sql "
DELETE FROM partial_effect_receipts WHERE receipt_id LIKE 'partial_recovery_rcpt_%';
DELETE FROM partial_effect_audit WHERE event_id='evt_order_001';
DELETE FROM partial_effect_expected_effects WHERE event_id='evt_order_001';
DELETE FROM partial_effect_events WHERE event_id='evt_order_001';
"

log "ensuring ayder topic"
ensure_topic

log "init complete"
