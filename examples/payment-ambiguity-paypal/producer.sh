#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=examples/payment-ambiguity-paypal/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd curl
require_cmd base64

SCENARIO="${SCENARIO:-timeout_unknown}"
ATTEMPT_ID="${ATTEMPT_ID:-attempt_001}"
IDEMPOTENCY_KEY="${IDEMPOTENCY_KEY:-idem_attempt_001}"

produce_once() {
  local sequence="$1"
  local payload response response_b64 durable sealed synced duplicate offset batch_id
  payload="$(jq -cn \
    --arg event_type "payment.initiated" \
    --arg attempt_id "$ATTEMPT_ID" \
    --arg idempotency_key "$IDEMPOTENCY_KEY" \
    --arg provider "paypal" \
    --arg scenario "$SCENARIO" \
    --argjson amount 2000 \
    --arg currency "USD" \
    '{event_type:$event_type,attempt_id:$attempt_id,idempotency_key:$idempotency_key,
      provider:$provider,scenario:$scenario,amount:$amount,currency:$currency}')"

  response="$(curl -sS -X POST \
    "${AYDER_BASE}/broker/topics/${TOPIC}/produce?partition=${PARTITION}&timeout_ms=5000&idempotency_key=${IDEMPOTENCY_KEY}" \
    -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$payload")"
  echo "$response" | jq -e '.ok==true' >/dev/null || {
    echo "produce failed: $response" >&2
    exit 1
  }

  response_b64="$(printf '%s' "$response" | base64 -w0)"
  durable="$(echo "$response" | jq -r '.durable // (.batch_id > 0) // false')"
  sealed="$(echo "$response" | jq -r '.sealed // false')"
  synced="$(echo "$response" | jq -r 'if .synced == null then "null" else (.synced|tostring) end')"
  duplicate="$(echo "$response" | jq -r '.duplicate // false')"
  offset="$(echo "$response" | jq -r '.offset // "null"')"
  batch_id="$(echo "$response" | jq -r '.batch_id // "null"')"

  pg_exec_sql "
INSERT INTO paypal_ayder_evidence(attempt_id, topic, idempotency_key, produce_response,
  msg_offset, batch_id, event_durable, event_sealed, synced, duplicate)
VALUES ('${ATTEMPT_ID}', '${TOPIC}', '${IDEMPOTENCY_KEY}',
  convert_from(decode('${response_b64}', 'base64'), 'UTF8')::jsonb,
  ${offset}, ${batch_id}, ${durable}, ${sealed}, ${synced}, ${duplicate});
"

  printf '%s\n' "$response" >"${ARTIFACT_DIR}/produce_${sequence}.json"
}

ensure_topic
produce_once 1

if [[ "$SCENARIO" == "duplicate_initiation" ]]; then
  produce_once 2
fi

log "produced payment.initiated scenario=${SCENARIO}"
