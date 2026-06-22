#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/ledger_clearance/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd curl

ensure_topic

payload="$(jq -cn \
  --arg event_type "payment_started" \
  --arg attempt_id "pay_attempt_004" \
  --arg source_account "user_available" \
  --arg clearance_account "clearance_account" \
  --arg currency "USD" \
  --arg provider_confirmation "MISSING" \
  --argjson amount 20 \
  '{event_type:$event_type,attempt_id:$attempt_id,source_account:$source_account,
    clearance_account:$clearance_account,amount:$amount,currency:$currency,
    provider_confirmation:$provider_confirmation}')"

resp="$(curl -sS -X POST \
  "${AYDER_BASE}/broker/topics/${TOPIC}/produce?partition=${PARTITION}&timeout_ms=5000&idempotency_key=pay_attempt_004" \
  -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$payload")"

echo "$resp" | jq -e '.ok==true' >/dev/null || {
  echo "produce failed: $resp" >&2
  exit 1
}

log "produced ledger clearance payment event"
