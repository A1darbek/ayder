#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/e2e/partial_batch_receipt/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd "$DOCKER_BIN"
require_cmd curl

if ! "$DOCKER_BIN" info >/dev/null 2>&1; then
  echo "docker daemon is not reachable from this environment" >&2
  echo "if using WSL2 + Docker Desktop, enable WSL integration for this distro" >&2
  exit 1
fi

log "starting postgres compose stack"
dc up -d "$PG_SERVICE"

log "waiting for postgres readiness"
if ! wait_pg_ready 90; then
  echo "postgres failed readiness" >&2
  exit 1
fi

log "applying schema"
pg_exec_file "${SCRIPT_DIR}/schema.sql"

log "resetting run tables for topic=${TOPIC} group=${GROUP} partition=${PARTITION}"
# Scope every delete to this demo's topic/group so a co-located exactly-once
# database is never disturbed. expected_events / account_balances are keyed by
# event_id / account_id (no topic column); this demo owns its own id namespace
# (evt-pbr-*, acct-pbr-*), so we only clear rows in that namespace.
pg_exec_sql "
DELETE FROM recovery_receipt_item WHERE receipt_id IN (SELECT receipt_id FROM recovery_receipt WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION});
DELETE FROM recovery_receipt WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION};
DELETE FROM dlq WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION};
DELETE FROM commit_log WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION};
DELETE FROM consumer_state_history WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION};
DELETE FROM consumer_state WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION};
DELETE FROM observed_effects WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION};
DELETE FROM processed_events WHERE topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION};
DELETE FROM expected_effects WHERE event_id IN (SELECT event_id FROM expected_events WHERE account_id LIKE 'acct-pbr-%');
DELETE FROM expected_events WHERE account_id LIKE 'acct-pbr-%';
DELETE FROM account_balances WHERE account_id LIKE 'acct-pbr-%';
"

log "ensuring ayder topic"
ensure_topic

log "init complete"
