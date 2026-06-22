#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/dlq_redrive_safety/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd curl

ensure_topic

produce_message() {
  local message_id="$1" operation="$2" should_fail="$3"
  local payload resp
  payload="$(jq -cn \
    --arg event_type "sqs_message_received" \
    --arg message_id "$message_id" \
    --arg business_operation "$operation" \
    --argjson should_fail "$should_fail" \
    '{event_type:$event_type,message_id:$message_id,
      business_operation:$business_operation,should_fail:$should_fail}')"

  resp="$(curl -sS -X POST \
    "${AYDER_BASE}/broker/topics/${TOPIC}/produce?partition=${PARTITION}&timeout_ms=5000&idempotency_key=${message_id}" \
    -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$payload")"

  echo "$resp" | jq -e '.ok==true' >/dev/null || {
    echo "produce failed for ${message_id}: ${resp}" >&2
    exit 1
  }
}

produce_message msg_008 order_inventory_reservation false
produce_message msg_009 order_payment_confirmation true
produce_message msg_010 order_email_receipt false

log "produced SQS/DLQ redrive safety messages"
