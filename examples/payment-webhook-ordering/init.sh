#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=examples/payment-webhook-ordering/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd "$DOCKER_BIN"
require_cmd curl

dc up -d "$PG_SERVICE"
wait_postgres || { echo "postgres failed readiness" >&2; exit 1; }

pg_exec_file "${SCRIPT_DIR}/schema.sql"
pg_exec_sql "
TRUNCATE TABLE
  ordering_receipts,
  ordering_business_effects,
  ordering_provider_event_dedupe,
  ordering_webhook_handling,
  ordering_ayder_evidence,
  ordering_payments
RESTART IDENTITY CASCADE;
"
ensure_topic

log "payment webhook ordering example initialized"
