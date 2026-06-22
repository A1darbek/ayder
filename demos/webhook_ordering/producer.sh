#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/webhook_ordering/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd curl

ensure_topic

produce_webhook() {
  local event_id="$1" payment_id="$2" state="$3" order="$4"
  local rank payload resp
  case "$state" in
    PROCESSING) rank=1 ;;
    SUCCESS) rank=2 ;;
    FAILED) rank=2 ;;
    *) echo "unknown state: $state" >&2; exit 2 ;;
  esac

  payload="$(jq -cn \
    --arg event_type "payment_provider_webhook" \
    --arg event_id "$event_id" \
    --arg payment_id "$payment_id" \
    --arg incoming_state "$state" \
    --argjson state_rank "$rank" \
    --argjson received_order "$order" \
    '{event_type:$event_type,event_id:$event_id,payment_id:$payment_id,
      incoming_state:$incoming_state,state_rank:$state_rank,received_order:$received_order}')"

  resp="$(curl -sS -X POST \
    "${AYDER_BASE}/broker/topics/${TOPIC}/produce?partition=${PARTITION}&timeout_ms=5000&idempotency_key=${event_id}" \
    -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$payload")"

  echo "$resp" | jq -e '.ok==true' >/dev/null || {
    echo "produce failed for ${event_id}: ${resp}" >&2
    exit 1
  }
}

produce_webhook webhook_001 pay_123 SUCCESS 1
produce_webhook webhook_002 pay_123 PROCESSING 2
produce_webhook webhook_003 pay_123 SUCCESS 3

log "produced 3 webhook events"
