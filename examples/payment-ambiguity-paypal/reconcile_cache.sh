#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=examples/payment-ambiguity-paypal/env.sh
source "${SCRIPT_DIR}/env.sh"

SCENARIO="${SCENARIO:-timeout_unknown}"
ATTEMPT_ID="${ATTEMPT_ID:-attempt_001}"
CACHE_KEY="payment:${ATTEMPT_ID}:state"

database_state="$(pg_query_scalar "SELECT customer_visible_state FROM paypal_payment_attempts WHERE attempt_id='${ATTEMPT_ID}';")"

case "$SCENARIO" in
  redis_stale)
    redis_cmd SET "$CACHE_KEY" PENDING >/dev/null
    ;;
  happy_path|duplicate_initiation)
    redis_cmd DEL "$CACHE_KEY" >/dev/null
    ;;
  timeout_then_success|timeout_unknown)
    redis_cmd SET "$CACHE_KEY" PENDING >/dev/null
    ;;
esac

redis_before="$(redis_cmd GET "$CACHE_KEY" || true)"
[[ -z "$redis_before" ]] && redis_before=""

# Database is authoritative. Redis is backfilled from the committed DB state.
redis_cmd SET "$CACHE_KEY" "$database_state" >/dev/null
redis_after="$(redis_cmd GET "$CACHE_KEY")"

before_safe="$(printf '%s' "$redis_before" | sed "s/'/''/g")"
pg_exec_sql "
INSERT INTO paypal_cache_reconciliation(reconciliation_id, attempt_id, database_state,
  redis_state_before, redis_state_after, resolution)
VALUES ('cache_recon_${ATTEMPT_ID}', '${ATTEMPT_ID}', '${database_state}',
  NULLIF('${before_safe}',''), '${redis_after}', 'database_wins')
ON CONFLICT (reconciliation_id) DO UPDATE
SET database_state=EXCLUDED.database_state,
    redis_state_before=EXCLUDED.redis_state_before,
    redis_state_after=EXCLUDED.redis_state_after,
    resolution=EXCLUDED.resolution,
    created_at=now();
"

log "redis reconciled from database before=${redis_before:-missing} after=${redis_after}"
