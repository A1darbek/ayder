#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=examples/payment-ambiguity-paypal/env.sh
source "${SCRIPT_DIR}/env.sh"

SCENARIO="${SCENARIO:-timeout_unknown}"
ATTEMPT_ID="${ATTEMPT_ID:-attempt_001}"

case "$SCENARIO" in
  timeout_then_success)
    pg_exec_sql "
BEGIN;
INSERT INTO paypal_verification_attempts(verification_id, attempt_id, attempt_no, provider_result)
VALUES ('verify_${ATTEMPT_ID}_1', '${ATTEMPT_ID}', 1, 'SUCCESS');
UPDATE paypal_payment_attempts
SET internal_state='SUCCESS',
    customer_visible_state='SUCCESS',
    provider_commitment='SUCCESS',
    safe_to_close=true,
    manual_reconciliation_required=false,
    updated_at=now()
WHERE attempt_id='${ATTEMPT_ID}';
INSERT INTO paypal_wallet_effects(effect_id, attempt_id, effect_type, amount, currency)
SELECT 'wallet_credit_${ATTEMPT_ID}', '${ATTEMPT_ID}', 'payment_success_credit', amount, currency
FROM paypal_payment_attempts WHERE attempt_id='${ATTEMPT_ID}'
ON CONFLICT (attempt_id, effect_type) DO NOTHING;
COMMIT;
"
    ;;
  timeout_unknown)
    pg_exec_sql "
BEGIN;
INSERT INTO paypal_verification_attempts(verification_id, attempt_id, attempt_no, provider_result)
VALUES
  ('verify_${ATTEMPT_ID}_1', '${ATTEMPT_ID}', 1, 'UNKNOWN'),
  ('verify_${ATTEMPT_ID}_2', '${ATTEMPT_ID}', 2, 'UNKNOWN'),
  ('verify_${ATTEMPT_ID}_3', '${ATTEMPT_ID}', 3, 'UNKNOWN')
ON CONFLICT DO NOTHING;
UPDATE paypal_payment_attempts
SET internal_state='TIMEOUT',
    customer_visible_state='PENDING',
    provider_commitment='UNKNOWN',
    safe_to_close=false,
    manual_reconciliation_required=true,
    updated_at=now()
WHERE attempt_id='${ATTEMPT_ID}';
COMMIT;
"
    ;;
  happy_path|duplicate_initiation|redis_stale)
    ;;
  *)
    echo "invalid SCENARIO=${SCENARIO}" >&2
    exit 2
    ;;
esac

log "provider verification applied scenario=${SCENARIO}"
