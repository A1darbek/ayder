#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/provider_timeout/env.sh
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

log "applying provider-timeout schema"
pg_exec_file "${SCRIPT_DIR}/schema.sql"

log "resetting provider-timeout demo rows"
pg_exec_sql "
DELETE FROM provider_timeout_receipts WHERE receipt_id LIKE 'pay_rcpt_%';
DELETE FROM payment_observed_effects WHERE attempt_id LIKE 'pay_attempt_%';
DELETE FROM payment_expected_effects WHERE attempt_id LIKE 'pay_attempt_%';
DELETE FROM suppressed_attempts WHERE attempt_id LIKE 'pay_attempt_%' OR duplicate_of LIKE 'pay_attempt_%';
DELETE FROM verification_attempts WHERE attempt_id LIKE 'pay_attempt_%';
DELETE FROM provider_calls WHERE attempt_id LIKE 'pay_attempt_%';
DELETE FROM payment_attempts WHERE attempt_id LIKE 'pay_attempt_%';
"

log "ensuring ayder topic"
ensure_topic

log "init complete"
