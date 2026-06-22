#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/provider_timeout/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd curl

produce_attempt() {
  local attempt_id="$1" merchant_attempt_id="$2" idem="$3" amount="$4" currency="$5" scenario="$6"
  local payload resp rc
  payload="$(jq -cn \
    --arg event_type "payment_attempt_received" \
    --arg attempt_id "$attempt_id" \
    --arg merchant_attempt_id "$merchant_attempt_id" \
    --arg idempotency_key "$idem" \
    --arg currency "$currency" \
    --arg scenario "$scenario" \
    --argjson amount "$amount" \
    '{event_type:$event_type,attempt_id:$attempt_id,merchant_attempt_id:$merchant_attempt_id,
      idempotency_key:$idempotency_key,amount:$amount,currency:$currency,scenario:$scenario}')"

  set +e
  resp="$(curl -sS -X POST \
    "${AYDER_BASE}/broker/topics/${TOPIC}/produce?partition=${PARTITION}&timeout_ms=5000&idempotency_key=${attempt_id}" \
    -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$payload")"
  rc=$?
  set -e

  if [[ $rc -ne 0 ]] || ! echo "$resp" | jq -e '.ok==true' >/dev/null 2>&1; then
    echo "produce failed for ${attempt_id}: ${resp}" >&2
    exit 1
  fi
}

ensure_topic

produce_attempt pay_attempt_001 merchant_attempt_001 order_001_payment_attempt_1 10000 USD success
produce_attempt pay_attempt_002 merchant_attempt_002 order_002_payment_attempt_1 2500 USD failed
produce_attempt pay_attempt_003 merchant_attempt_003 order_003_payment_attempt_1 4400 USD timeout_then_success
produce_attempt pay_attempt_004 merchant_attempt_004 order_004_payment_attempt_1 9900 USD timeout_unknown
produce_attempt pay_attempt_005 merchant_attempt_005 order_001_payment_attempt_1 10000 USD duplicate_retry

log "produced 5 provider-timeout payment attempts"
