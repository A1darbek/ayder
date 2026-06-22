#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/provider_timeout/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd curl
require_cmd base64

record_expected_effects_sql() {
  local attempt_id="$1"
  printf "INSERT INTO payment_expected_effects(attempt_id, effect_type)
VALUES
  ('%s','payment_attempt_persisted'),
  ('%s','provider_call_recorded'),
  ('%s','provider_commitment_determined'),
  ('%s','customer_state_consistent'),
  ('%s','duplicate_attempt_suppressed_if_retried')
ON CONFLICT DO NOTHING;
" "$attempt_id" "$attempt_id" "$attempt_id" "$attempt_id" "$attempt_id"
}

observe_sql() {
  local attempt_id="$1" effect_type="$2" status="$3" detail="$4"
  printf "INSERT INTO payment_observed_effects(attempt_id, effect_type, status, detail)
VALUES ('%s','%s','%s','%s'::jsonb)
ON CONFLICT (attempt_id, effect_type)
DO UPDATE SET status=EXCLUDED.status, detail=EXCLUDED.detail, observed_at=now();
" "$attempt_id" "$effect_type" "$status" "$detail"
}

insert_attempt_sql() {
  local attempt_id="$1" merchant_attempt_id="$2" idem="$3" amount="$4" currency="$5"
  local internal="$6" customer="$7" ambiguity="$8" provider_txn="$9"
  printf "INSERT INTO payment_attempts(attempt_id, merchant_attempt_id, idempotency_key, amount, currency,
  internal_state, customer_state, ambiguity_reason, provider_transaction_id)
VALUES ('%s','%s','%s',%s,'%s','%s','%s',NULLIF('%s',''),NULLIF('%s',''))
ON CONFLICT (attempt_id) DO UPDATE
SET internal_state=EXCLUDED.internal_state,
    customer_state=EXCLUDED.customer_state,
    ambiguity_reason=EXCLUDED.ambiguity_reason,
    provider_transaction_id=EXCLUDED.provider_transaction_id,
    updated_at=now();
" "$attempt_id" "$merchant_attempt_id" "$idem" "$amount" "$currency" "$internal" "$customer" "$ambiguity" "$provider_txn"
}

process_attempt() {
  local payload_json="$1"
  local attempt_id merchant_attempt_id idem amount currency scenario duplicate_of
  attempt_id="$(printf '%s' "$payload_json" | jq -r '.attempt_id')"
  merchant_attempt_id="$(printf '%s' "$payload_json" | jq -r '.merchant_attempt_id')"
  idem="$(printf '%s' "$payload_json" | jq -r '.idempotency_key')"
  amount="$(printf '%s' "$payload_json" | jq -r '.amount')"
  currency="$(printf '%s' "$payload_json" | jq -r '.currency')"
  scenario="$(printf '%s' "$payload_json" | jq -r '.scenario')"

  duplicate_of="$(pg_query_scalar "
SELECT attempt_id FROM payment_attempts
WHERE idempotency_key='${idem}' AND internal_state <> 'SUPPRESSED_DUPLICATE'
ORDER BY created_at ASC LIMIT 1;")"

  if [[ "$scenario" == "duplicate_retry" || -n "$duplicate_of" ]]; then
    pg_exec_sql "
BEGIN;
$(insert_attempt_sql "$attempt_id" "$merchant_attempt_id" "$idem" "$amount" "$currency" "SUPPRESSED_DUPLICATE" "PAID" "" "")
$(record_expected_effects_sql "$attempt_id")
INSERT INTO suppressed_attempts(attempt_id, duplicate_of, idempotency_key, reason)
VALUES ('${attempt_id}', '${duplicate_of:-pay_attempt_001}', '${idem}', 'idempotency_key_already_used')
ON CONFLICT (attempt_id) DO NOTHING;
$(observe_sql "$attempt_id" "payment_attempt_persisted" "CONFIRMED" "{\"state\":\"SUPPRESSED_DUPLICATE\"}")
$(observe_sql "$attempt_id" "duplicate_attempt_suppressed_if_retried" "CONFIRMED" "{\"duplicate_of\":\"${duplicate_of:-pay_attempt_001}\"}")
$(observe_sql "$attempt_id" "customer_state_consistent" "CONFIRMED" "{\"customer_state\":\"PAID\"}")
COMMIT;
"
    return
  fi

  case "$scenario" in
    success)
      pg_exec_sql "
BEGIN;
$(insert_attempt_sql "$attempt_id" "$merchant_attempt_id" "$idem" "$amount" "$currency" "SUCCESS_CONFIRMED" "PAID" "" "ptx_001")
$(record_expected_effects_sql "$attempt_id")
INSERT INTO provider_calls(call_id, attempt_id, request_type, outcome, provider_transaction_id)
VALUES ('call_${attempt_id}_charge', '${attempt_id}', 'CHARGE', 'SUCCESS', 'ptx_001')
ON CONFLICT DO NOTHING;
$(observe_sql "$attempt_id" "payment_attempt_persisted" "CONFIRMED" "{\"state\":\"SUCCESS_CONFIRMED\"}")
$(observe_sql "$attempt_id" "provider_call_recorded" "CONFIRMED" "{\"outcome\":\"SUCCESS\"}")
$(observe_sql "$attempt_id" "provider_commitment_determined" "CONFIRMED" "{\"provider_commitment\":\"SUCCESS\"}")
$(observe_sql "$attempt_id" "customer_state_consistent" "CONFIRMED" "{\"customer_state\":\"PAID\"}")
$(observe_sql "$attempt_id" "duplicate_attempt_suppressed_if_retried" "CONFIRMED" "{\"duplicate\":false}")
COMMIT;
"
      ;;
    failed)
      pg_exec_sql "
BEGIN;
$(insert_attempt_sql "$attempt_id" "$merchant_attempt_id" "$idem" "$amount" "$currency" "FAILED_CONFIRMED" "FAILED" "" "")
$(record_expected_effects_sql "$attempt_id")
INSERT INTO provider_calls(call_id, attempt_id, request_type, outcome, reason)
VALUES ('call_${attempt_id}_charge', '${attempt_id}', 'CHARGE', 'FAILED', 'provider_declined')
ON CONFLICT DO NOTHING;
$(observe_sql "$attempt_id" "payment_attempt_persisted" "CONFIRMED" "{\"state\":\"FAILED_CONFIRMED\"}")
$(observe_sql "$attempt_id" "provider_call_recorded" "CONFIRMED" "{\"outcome\":\"FAILED\"}")
$(observe_sql "$attempt_id" "provider_commitment_determined" "CONFIRMED" "{\"provider_commitment\":\"FAILED\"}")
$(observe_sql "$attempt_id" "customer_state_consistent" "CONFIRMED" "{\"customer_state\":\"FAILED\"}")
$(observe_sql "$attempt_id" "duplicate_attempt_suppressed_if_retried" "CONFIRMED" "{\"duplicate\":false}")
COMMIT;
"
      ;;
    timeout_then_success)
      pg_exec_sql "
BEGIN;
$(insert_attempt_sql "$attempt_id" "$merchant_attempt_id" "$idem" "$amount" "$currency" "SUCCESS_CONFIRMED" "PAID" "provider_timeout_resolved" "ptx_003")
$(record_expected_effects_sql "$attempt_id")
INSERT INTO provider_calls(call_id, attempt_id, request_type, outcome, reason)
VALUES ('call_${attempt_id}_charge', '${attempt_id}', 'CHARGE', 'TIMEOUT', 'provider_timeout')
ON CONFLICT DO NOTHING;
INSERT INTO verification_attempts(verification_id, attempt_id, provider_result)
VALUES ('verify_${attempt_id}_001', '${attempt_id}', 'SUCCESS')
ON CONFLICT DO NOTHING;
$(observe_sql "$attempt_id" "payment_attempt_persisted" "CONFIRMED" "{\"state\":\"SUCCESS_CONFIRMED\"}")
$(observe_sql "$attempt_id" "provider_call_recorded" "CONFIRMED" "{\"outcome\":\"TIMEOUT\"}")
$(observe_sql "$attempt_id" "provider_commitment_determined" "CONFIRMED" "{\"provider_commitment\":\"SUCCESS\",\"source\":\"verification\"}")
$(observe_sql "$attempt_id" "customer_state_consistent" "CONFIRMED" "{\"customer_state\":\"PAID\"}")
$(observe_sql "$attempt_id" "duplicate_attempt_suppressed_if_retried" "CONFIRMED" "{\"duplicate\":false}")
COMMIT;
"
      ;;
    timeout_unknown)
      pg_exec_sql "
BEGIN;
$(insert_attempt_sql "$attempt_id" "$merchant_attempt_id" "$idem" "$amount" "$currency" "AMBIGUOUS" "PENDING_CONFIRMATION" "provider_timeout" "")
$(record_expected_effects_sql "$attempt_id")
INSERT INTO provider_calls(call_id, attempt_id, request_type, outcome, reason)
VALUES ('call_${attempt_id}_charge', '${attempt_id}', 'CHARGE', 'TIMEOUT', 'provider_timeout')
ON CONFLICT DO NOTHING;
INSERT INTO verification_attempts(verification_id, attempt_id, provider_result)
VALUES ('verify_${attempt_id}_001', '${attempt_id}', 'UNKNOWN')
ON CONFLICT DO NOTHING;
$(observe_sql "$attempt_id" "payment_attempt_persisted" "CONFIRMED" "{\"state\":\"AMBIGUOUS\"}")
$(observe_sql "$attempt_id" "provider_call_recorded" "CONFIRMED" "{\"outcome\":\"TIMEOUT\"}")
$(observe_sql "$attempt_id" "customer_state_consistent" "CONFIRMED" "{\"customer_state\":\"PENDING_CONFIRMATION\"}")
$(observe_sql "$attempt_id" "duplicate_attempt_suppressed_if_retried" "CONFIRMED" "{\"duplicate\":false}")
COMMIT;
"
      ;;
    *)
      echo "unknown scenario: $scenario" >&2
      exit 2
      ;;
  esac
}

consume_url="${AYDER_BASE}/broker/consume/${TOPIC}/${GROUP}/${PARTITION}?offset=0&limit=20&encoding=b64"
raw="$(curl -sS -w $'\n%{http_code}' -H "$AUTH_HEADER" "$consume_url")"
http_code="${raw##*$'\n'}"
body="${raw%$'\n'*}"
[[ "$http_code" == "200" ]] || { echo "consume failed http=${http_code}: ${body}" >&2; exit 1; }

count="$(echo "$body" | jq -r '.count // 0')"
[[ "$count" == "5" ]] || { echo "expected 5 payment attempts from Ayder, got ${count}" >&2; exit 1; }

for idx in $(seq 0 $((count - 1))); do
  value_b64="$(echo "$body" | jq -r ".messages[${idx}].value_b64")"
  process_attempt "$(printf '%s' "$value_b64" | base64 -d)"
done

payload="$(jq -cn --arg topic "$TOPIC" --arg group "$GROUP" --argjson partition "$PARTITION" --argjson offset 4 \
  '{topic:$topic,group:$group,partition:$partition,offset:$offset}')"
curl -fsS -X POST "${AYDER_BASE}/broker/commit" \
  -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$payload" >/dev/null

log "replayed ${count} payment attempts through provider-timeout recovery path"
