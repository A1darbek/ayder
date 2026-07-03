#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=examples/payment-ambiguity-paypal/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd "$DOCKER_BIN"
require_cmd curl

dc up -d "$PG_SERVICE" "$REDIS_SERVICE"
wait_services || { echo "postgres/redis failed readiness" >&2; exit 1; }

pg_exec_file "${SCRIPT_DIR}/schema.sql"
pg_exec_sql "
TRUNCATE TABLE
  paypal_receipts,
  paypal_cache_reconciliation,
  paypal_wallet_effects,
  paypal_processing_audit,
  paypal_verification_attempts,
  paypal_provider_calls,
  paypal_ayder_evidence,
  paypal_payment_attempts
RESTART IDENTITY CASCADE;
"
redis_cmd FLUSHDB >/dev/null
ensure_topic

log "paypal ambiguity demo initialized"
