#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=examples/payment-webhook-ordering/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd curl
require_cmd base64

produce_webhook() {
  local ingestion_event_id="$1" provider_event_id="$2" state="$3" sequence="$4"
  local payload response response_b64 durable sealed synced offset batch_id
  payload="$(jq -cn \
    --arg event_type "webhook.received" \
    --arg ingestion_event_id "$ingestion_event_id" \
    --arg provider_event_id "$provider_event_id" \
    --arg payment_id "pay_123" \
    --arg state "$state" \
    --argjson sequence "$sequence" \
    '{event_type:$event_type,ingestion_event_id:$ingestion_event_id,
      provider_event_id:$provider_event_id,payment_id:$payment_id,
      state:$state,sequence:$sequence}')"

  response="$(curl -sS -X POST \
    "${AYDER_BASE}/broker/topics/${TOPIC}/produce?partition=${PARTITION}&timeout_ms=5000&idempotency_key=${ingestion_event_id}" \
    -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$payload")"
  echo "$response" | jq -e '.ok==true' >/dev/null || {
    echo "produce failed for ${ingestion_event_id}: ${response}" >&2
    exit 1
  }

  response_b64="$(printf '%s' "$response" | base64 -w0)"
  durable="$(echo "$response" | jq -r '.durable // (.batch_id > 0) // false')"
  sealed="$(echo "$response" | jq -r '.sealed // false')"
  synced="$(echo "$response" | jq -r 'if .synced == null then "null" else (.synced|tostring) end')"
  offset="$(echo "$response" | jq -r '.offset')"
  batch_id="$(echo "$response" | jq -r '.batch_id')"

  pg_exec_sql "
INSERT INTO ordering_ayder_evidence(ingestion_event_id, provider_event_id, topic,
  produce_response, msg_offset, batch_id, event_durable, event_sealed, synced)
VALUES ('${ingestion_event_id}', '${provider_event_id}', '${TOPIC}',
  convert_from(decode('${response_b64}', 'base64'), 'UTF8')::jsonb,
  ${offset}, ${batch_id}, ${durable}, ${sealed}, ${synced});
"
  printf '%s\n' "$response" >"${ARTIFACT_DIR}/produce_${sequence}.json"
}

ensure_topic

produce_webhook webhook_001 provider_evt_success_001 SUCCESS 1
produce_webhook webhook_002 provider_evt_processing_002 PROCESSING 2
produce_webhook webhook_003 provider_evt_success_001 SUCCESS 3

log "produced SUCCESS, stale PROCESSING, and duplicate SUCCESS webhooks"
