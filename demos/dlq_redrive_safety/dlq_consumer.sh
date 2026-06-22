#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/dlq_redrive_safety/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd curl
require_cmd base64

process_message() {
  local payload_json="$1"
  local message_id operation should_fail payload_b64
  message_id="$(printf '%s' "$payload_json" | jq -r '.message_id')"
  operation="$(printf '%s' "$payload_json" | jq -r '.business_operation')"
  should_fail="$(printf '%s' "$payload_json" | jq -r '.should_fail')"
  payload_b64="$(printf '%s' "$payload_json" | base64 -w0)"

  pg_exec_sql "
INSERT INTO sqs_messages(message_id, business_operation, payload, status)
VALUES ('${message_id}', '${operation}', convert_from(decode('${payload_b64}', 'base64'), 'UTF8')::jsonb, 'RECEIVED')
ON CONFLICT (message_id) DO UPDATE
SET status=EXCLUDED.status, updated_at=now();
"

  if [[ "$should_fail" == "true" ]]; then
    for attempt in 1 2 3; do
      pg_exec_sql "
INSERT INTO sqs_processing_attempts(attempt_id, message_id, attempt_no, outcome, failure_reason)
VALUES ('attempt_${message_id}_${attempt}', '${message_id}', ${attempt}, 'FAILED', 'consumer_error')
ON CONFLICT DO NOTHING;
"
    done

    pg_exec_sql "
BEGIN;
UPDATE sqs_messages SET status='DLQ', updated_at=now() WHERE message_id='${message_id}';
INSERT INTO sqs_business_effects(effect_id, message_id, effect_type, status, detail)
VALUES ('effect_${message_id}_payment_state', '${message_id}', 'payment_state', 'UNKNOWN', '{\"last_known_effect\":\"payment_state_unknown\"}'::jsonb)
ON CONFLICT DO NOTHING;
INSERT INTO sqs_dlq_items(message_id, business_operation, failure_reason, last_known_effect,
  replay_safety, disposition, recommended_action)
VALUES ('${message_id}', '${operation}', 'consumer_error', 'payment_state_unknown',
  'UNKNOWN', 'UNRESOLVED', 'manual review before redrive')
ON CONFLICT (message_id) DO UPDATE
SET replay_safety=EXCLUDED.replay_safety,
    disposition=EXCLUDED.disposition,
    recommended_action=EXCLUDED.recommended_action,
    moved_at=now();
COMMIT;
"
  else
    pg_exec_sql "
BEGIN;
INSERT INTO sqs_processing_attempts(attempt_id, message_id, attempt_no, outcome)
VALUES ('attempt_${message_id}_1', '${message_id}', 1, 'SUCCESS')
ON CONFLICT DO NOTHING;
INSERT INTO sqs_business_effects(effect_id, message_id, effect_type, status, detail)
VALUES ('effect_${message_id}_completed', '${message_id}', '${operation}', 'CONFIRMED', '{\"processed\":true}'::jsonb)
ON CONFLICT DO NOTHING;
UPDATE sqs_messages SET status='PROCESSED', updated_at=now() WHERE message_id='${message_id}';
COMMIT;
"
  fi
}

consume_url="${AYDER_BASE}/broker/consume/${TOPIC}/${GROUP}/${PARTITION}?offset=0&limit=10&encoding=b64"
raw="$(curl -sS -w $'\n%{http_code}' -H "$AUTH_HEADER" "$consume_url")"
http_code="${raw##*$'\n'}"
body="${raw%$'\n'*}"
[[ "$http_code" == "200" ]] || { echo "consume failed http=${http_code}: ${body}" >&2; exit 1; }

count="$(echo "$body" | jq -r '.count // 0')"
[[ "$count" == "3" ]] || { echo "expected 3 SQS messages from Ayder, got ${count}" >&2; exit 1; }

for idx in $(seq 0 $((count - 1))); do
  value_b64="$(echo "$body" | jq -r ".messages[${idx}].value_b64")"
  process_message "$(printf '%s' "$value_b64" | base64 -d)"
done

payload="$(jq -cn --arg topic "$TOPIC" --arg group "$GROUP" --argjson partition "$PARTITION" --argjson offset 2 \
  '{topic:$topic,group:$group,partition:$partition,offset:$offset}')"
curl -fsS -X POST "${AYDER_BASE}/broker/commit" \
  -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$payload" >/dev/null

log "processed continuing messages and moved msg_009 to DLQ after 3 failures"
