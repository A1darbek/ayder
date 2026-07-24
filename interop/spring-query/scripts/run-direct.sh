#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INTEROP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${INTEROP_DIR}/../.." && pwd)"

TARGET="${TARGET:-ayder-direct}"
BASE="${BASE:-http://127.0.0.1:1109}"
TOKEN="${TOKEN:-dev}"
AUTH_HEADER="Authorization: Bearer ${TOKEN}"
JSON_HEADER="Content-Type: application/json"
RUN_ID="${RUN_ID:-spring_query_$(date -u +%Y%m%dT%H%M%SZ)}"
TOPIC="${MATRIX_TOPIC:-payment-recovery-${RUN_ID}}"
GROUP="${MATRIX_GROUP:-payment-worker-${RUN_ID}}"
OUT_DIR="${OUT_DIR:-${INTEROP_DIR}/receipts/${TARGET}}"
RAW_DIR="${OUT_DIR}/raw"
ROWS="${RAW_DIR}/rows.ndjson"
COMMIT_SHA="$(git -C "$REPO_ROOT" rev-parse HEAD)"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing command: $1" >&2
    exit 127
  }
}

log() {
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

header_value() {
  local name="$1"
  local file="$2"
  awk -v key="$(printf '%s' "$name" | tr '[:upper:]' '[:lower:]')" '
    BEGIN { FS=":" }
    {
      h=tolower($1)
      if (h == key) {
        sub(/^[^:]*:[ \t]*/, "", $0)
        sub(/\r$/, "", $0)
        print $0
        exit
      }
    }
  ' "$file"
}

result_for() {
  local condition="$1"
  if [[ "$condition" == "1" ]]; then
    printf 'PASS'
  else
    printf 'FAIL'
  fi
}

is_sha256_fingerprint() {
  [[ "${1:-}" =~ ^sha256:[0-9a-f]{64}$ ]]
}

is_nonempty_etag() {
  local value="${1:-}"
  [[ -n "$value" && "$value" != "null" ]]
}

is_non_negative_integer() {
  [[ "${1:-}" =~ ^[0-9]+$ ]]
}

append_status_row() {
  local id="$1"
  local result="$2"
  local expected="$3"
  local observed="$4"
  jq -nc \
    --arg id "$id" \
    --arg result "$result" \
    --argjson expected "$expected" \
    --argjson observed "$observed" \
    '{id:$id,result:$result,expected_status:$expected,observed_status:$observed}' >>"$ROWS"
}

query_request() {
  local query_file="$1"
  local headers_file="$2"
  local body_file="$3"
  shift 3
  curl -sS -D "$headers_file" -o "$body_file" -w '%{http_code}' \
    -X QUERY "${BASE}/broker/query" \
    -H "$AUTH_HEADER" \
    -H "$JSON_HEADER" \
    "$@" \
    --data-binary @"$query_file"
}

prepare_query() {
  local fixture="$1"
  local out="$2"
  jq --arg topic "$TOPIC" --arg group "$GROUP" \
    '.source.topic = $topic | .include.commit_status_for_group = $group' \
    "$fixture" >"$out"
}

need curl
need jq
need python3

mkdir -p "$RAW_DIR"
: >"$ROWS"

log "checking Ayder health at ${BASE}"
curl -fsS "${BASE}/health" >/dev/null

log "creating topic ${TOPIC}"
curl -fsS -X POST "${BASE}/broker/topics" \
  -H "$AUTH_HEADER" -H "$JSON_HEADER" \
  --data-binary "$(jq -nc --arg name "$TOPIC" '{name:$name,partitions:1}')" \
  >"${RAW_DIR}/create_topic.json"

cat >"${RAW_DIR}/initial_events.ndjson" <<EOF_EVENTS
{"event_id":"pay_evt_001","payment_id":"pay_001","provider_order_id":"po_001","provider_commitment":"UNKNOWN","safe_to_close":false,"manual_reconciliation_required":true}
{"event_id":"pay_evt_002","payment_id":"pay_002","provider_order_id":"po_002","provider_commitment":"SUCCESS","safe_to_close":true,"manual_reconciliation_required":false}
{"event_id":"pay_evt_003","payment_id":"pay_003","provider_order_id":"po_003","provider_commitment":"UNKNOWN","safe_to_close":false,"manual_reconciliation_required":true}
EOF_EVENTS

log "producing initial payment recovery events"
curl -fsS -X POST "${BASE}/broker/topics/${TOPIC}/produce-ndjson?partition=0&timeout_ms=5000&idempotency_key=${RUN_ID}_initial" \
  -H "$AUTH_HEADER" -H 'Content-Type: application/x-ndjson' \
  --data-binary @"${RAW_DIR}/initial_events.ndjson" \
  >"${RAW_DIR}/produce_initial.json"

log "setting committed offset baseline for ${GROUP}"
curl -fsS -X POST "${BASE}/broker/commit" \
  -H "$AUTH_HEADER" -H "$JSON_HEADER" \
  --data-binary "$(jq -nc --arg topic "$TOPIC" --arg group "$GROUP" '{topic:$topic,group:$group,partition:0,offset:0}')" \
  >"${RAW_DIR}/commit_baseline.json"

prepare_query "${INTEROP_DIR}/fixtures/bounded.json" "${RAW_DIR}/bounded.json"
prepare_query "${INTEROP_DIR}/fixtures/bounded-equivalent.json" "${RAW_DIR}/bounded-equivalent.json"
prepare_query "${INTEROP_DIR}/fixtures/expanded.json" "${RAW_DIR}/expanded.json"

headers_first="${RAW_DIR}/first.headers"
body_first="${RAW_DIR}/first.response.json"
log "running native QUERY with bounded JSON body"
code_first="$(query_request "${RAW_DIR}/bounded.json" "$headers_first" "$body_first")"
append_status_row "core.native_query" "$(result_for "$([[ "$code_first" == "200" ]] && echo 1 || echo 0)")" 200 "$code_first"

accept_query="$(header_value Accept-Query "$headers_first")"
etag_first="$(header_value ETag "$headers_first")"
content_location="$(header_value Content-Location "$headers_first")"
cors_methods="$(header_value Access-Control-Allow-Methods "$headers_first")"
rate_limit_limit="$(header_value X-RateLimit-Limit "$headers_first")"
rate_limit_remaining="$(header_value X-RateLimit-Remaining "$headers_first")"
rate_limit_reset="$(header_value X-RateLimit-Reset "$headers_first")"
fingerprint_first="$(jq -r '.query_fingerprint // ""' "$body_first")"
rows_first="$(jq '.rows | length' "$body_first")"

jq -nc --arg result "$(result_for "$([[ "$accept_query" == "application/json" ]] && echo 1 || echo 0)")" \
  --arg accept_query "$accept_query" \
  '{id:"core.accept_query_header",result:$result,accept_query:$accept_query}' >>"$ROWS"

jq -nc --arg result "$(result_for "$([[ "$rows_first" == "2" && "$fingerprint_first" == sha256:* ]] && echo 1 || echo 0)")" \
  --argjson rows "$rows_first" \
  --arg fingerprint "$fingerprint_first" \
  '{id:"core.query_body_reaches_handler",result:$result,matched_rows:$rows,fingerprint:$fingerprint}' >>"$ROWS"

jq -nc --arg result "$(result_for "$([[ "$cors_methods" == *QUERY* ]] && echo 1 || echo 0)")" \
  --arg allow_methods "$cors_methods" \
  '{id:"cors.allow_methods_mentions_query",result:$result,access_control_allow_methods:$allow_methods,detail:"Ayder response CORS header advertises QUERY; Spring must still prove OPTIONS preflight separately."}' >>"$ROWS"

rate_limit_result="FAIL"
if is_non_negative_integer "$rate_limit_limit" \
  && is_non_negative_integer "$rate_limit_remaining" \
  && is_non_negative_integer "$rate_limit_reset" \
  && (( rate_limit_limit > 0 )) \
  && (( rate_limit_remaining <= rate_limit_limit )) \
  && (( rate_limit_reset > 0 )); then
  rate_limit_result="PASS"
fi

jq -nc \
  --arg result "$rate_limit_result" \
  --arg limit "$rate_limit_limit" \
  --arg remaining "$rate_limit_remaining" \
  --arg reset "$rate_limit_reset" \
  '{
    id: "ayder.rate_limit_headers_sane",
    result: $result,
    limit: (if ($limit | test("^[0-9]+$")) then ($limit | tonumber) else null end),
    remaining: (if ($remaining | test("^[0-9]+$")) then ($remaining | tonumber) else null end),
    reset: (if ($reset | test("^[0-9]+$")) then ($reset | tonumber) else null end)
  }' >>"$ROWS"

headers_repeat="${RAW_DIR}/repeat.headers"
body_repeat="${RAW_DIR}/repeat.response.json"
log "repeating identical bounded QUERY"
code_repeat="$(query_request "${RAW_DIR}/bounded.json" "$headers_repeat" "$body_repeat")"
fingerprint_repeat="$(jq -r '.query_fingerprint // ""' "$body_repeat")"
etag_repeat="$(header_value ETag "$headers_repeat")"
repeat_valid=0
if [[ "$code_repeat" == "200" ]] \
  && is_sha256_fingerprint "$fingerprint_first" \
  && is_sha256_fingerprint "$fingerprint_repeat" \
  && [[ "$fingerprint_repeat" == "$fingerprint_first" ]] \
  && is_nonempty_etag "$etag_first" \
  && is_nonempty_etag "$etag_repeat" \
  && [[ "$etag_repeat" == "$etag_first" ]]; then
  repeat_valid=1
fi

jq -nc \
  --arg result "$(result_for "$repeat_valid")" \
  --argjson expected_status 200 \
  --argjson observed_status "$code_repeat" \
  --arg first_fingerprint "$fingerprint_first" \
  --arg repeat_fingerprint "$fingerprint_repeat" \
  --arg first_etag "$etag_first" \
  --arg repeat_etag "$etag_repeat" \
  '{id:"representation.repeat_identical_query",result:$result,expected_status:$expected_status,observed_status:$observed_status,first_fingerprint:$first_fingerprint,repeat_fingerprint:$repeat_fingerprint,first_etag:$first_etag,repeat_etag:$repeat_etag}' >>"$ROWS"

headers_equiv="${RAW_DIR}/equivalent.headers"
body_equiv="${RAW_DIR}/equivalent.response.json"
log "running equivalent reordered JSON QUERY"
code_equiv="$(query_request "${RAW_DIR}/bounded-equivalent.json" "$headers_equiv" "$body_equiv")"
fingerprint_equiv="$(jq -r '.query_fingerprint // ""' "$body_equiv")"
rows_equiv="$(jq '.rows | length' "$body_equiv")"
equivalent_valid=0
if [[ "$code_equiv" == "200" ]] \
  && is_sha256_fingerprint "$fingerprint_first" \
  && is_sha256_fingerprint "$fingerprint_equiv" \
  && [[ "$fingerprint_equiv" == "$fingerprint_first" ]]; then
  equivalent_valid=1
fi

jq -nc \
  --arg result "$(result_for "$equivalent_valid")" \
  --argjson first_status "$code_first" \
  --argjson equivalent_status "$code_equiv" \
  --argjson first_rows "$rows_first" \
  --argjson equivalent_rows "$rows_equiv" \
  --arg first_fingerprint "$fingerprint_first" \
  --arg equivalent_fingerprint "$fingerprint_equiv" \
  '{id:"representation.equivalent_json",result:$result,first_status:$first_status,equivalent_status:$equivalent_status,first_rows:$first_rows,equivalent_rows:$equivalent_rows,first_fingerprint:$first_fingerprint,equivalent_fingerprint:$equivalent_fingerprint,detail:"Object key order and whitespace changed; array order preserved."}' >>"$ROWS"

headers_304="${RAW_DIR}/revalidation.headers"
body_304="${RAW_DIR}/revalidation.response"
log "running If-None-Match revalidation"
code_304="$(query_request "${RAW_DIR}/bounded.json" "$headers_304" "$body_304" -H "If-None-Match: ${etag_first}")"
jq -nc \
  --arg result "$(result_for "$([[ "$code_304" == "304" ]] && echo 1 || echo 0)")" \
  --argjson expected_status 304 \
  --argjson observed_status "$code_304" \
  '{
    id: "representation.revalidation",
    result: $result,
    expected_status: $expected_status,
    observed_status: $observed_status,
    scope: "unchanged selected representation",
    does_not_prove: "validator invalidation after the selected representation changes",
    tracked_issue: "https://github.com/A1darbek/ayder/issues/13"
  }' >>"$ROWS"

if [[ -z "$content_location" ]]; then
  jq -nc \
    --arg etag "$etag_first" \
    --arg fingerprint "$fingerprint_first" \
    '{
      id: "representation.content_location",
      result: "NOT_SUPPORTED",
      etag: $etag,
      fingerprint: $fingerprint,
      reason: "Ayder does not advertise Content-Location until a retrievable authenticated result resource exists",
      design_issue: "https://github.com/A1darbek/ayder/issues/11"
    }' >>"$ROWS"
else
  jq -nc \
    --arg content_location "$content_location" \
    '{
      id: "representation.content_location",
      result: "FAIL",
      content_location: $content_location,
      reason: "Unexpected Content-Location advertised before retrievable result-resource support exists"
    }' >>"$ROWS"
fi

unsupported_code="$(curl -sS -o "${RAW_DIR}/unsupported.response.json" -w '%{http_code}' \
  -X QUERY "${BASE}/broker/query" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: text/plain' \
  --data-binary @"${INTEROP_DIR}/fixtures/unsupported.txt")"
append_status_row "core.unsupported_content_type" "$(result_for "$([[ "$unsupported_code" == "415" ]] && echo 1 || echo 0)")" 415 "$unsupported_code"

missing_ct_code="$(curl -sS -o "${RAW_DIR}/missing_content_type.response.json" -w '%{http_code}' \
  -X QUERY "${BASE}/broker/query" \
  -H "$AUTH_HEADER" \
  --data-binary @"${RAW_DIR}/bounded.json")"
jq -nc --argjson observed_status "$missing_ct_code" \
  '{id:"core.missing_content_type",result:"OBSERVED",observed_status:$observed_status,detail:"Recorded actual Ayder behavior when QUERY has a body but no Content-Type."}' >>"$ROWS"

post_code="$(curl -sS -D "${RAW_DIR}/post_fallback.headers" -o "${RAW_DIR}/post_fallback.response.json" -w '%{http_code}' \
  -X POST "${BASE}/broker/query" \
  -H "$AUTH_HEADER" -H "$JSON_HEADER" \
  --data-binary @"${RAW_DIR}/bounded.json")"
jq -nc --argjson observed_status "$post_code" \
  '{id:"core.post_compatibility_fallback",result:"OBSERVED",observed_status:$observed_status,detail:"Legacy POST /broker/query remains recorded separately from native QUERY."}' >>"$ROWS"

jq -nc \
  --arg result "$(result_for "$([[ "$code_first" == "200" && "$rows_first" == "2" ]] && echo 1 || echo 0)")" \
  --argjson expected_status 200 \
  --argjson observed_status "$code_first" \
  --argjson rows "$rows_first" \
  --arg fingerprint "$fingerprint_first" \
  --arg etag "$etag_first" \
  '{id:"representation.first_bounded_query",result:$result,expected_status:$expected_status,observed_status:$observed_status,matched_rows:$rows,fingerprint:$fingerprint,etag:$etag}' >>"$ROWS"

broker_mutated="$(jq -r '.safety.broker_state_mutated' "$body_first")"
high_before="$(jq -r '.safety.high_watermark_before' "$body_first")"
high_after="$(jq -r '.safety.high_watermark_after' "$body_first")"
message_before="$(jq -r '.safety.message_count_before' "$body_first")"
message_after="$(jq -r '.safety.message_count_after' "$body_first")"
idemp_before="$(jq -r '.safety.idempotency_records_before' "$body_first")"
idemp_after="$(jq -r '.safety.idempotency_records_after' "$body_first")"
commit_before="$(jq -r '.safety.committed_offset_before' "$body_first")"
commit_after="$(jq -r '.safety.committed_offset_after' "$body_first")"

safety_evidence_valid=0
if jq -e '
  (.safety | type == "object") and
  (.safety.broker_state_mutated | type == "boolean") and
  (.safety.high_watermark_before | type == "number") and
  (.safety.high_watermark_after | type == "number") and
  (.safety.message_count_before | type == "number") and
  (.safety.message_count_after | type == "number") and
  (.safety.idempotency_records_before | type == "number") and
  (.safety.idempotency_records_after | type == "number") and
  (.safety.committed_offset_before | type == "number") and
  (.safety.committed_offset_after | type == "number")
' "$body_first" >/dev/null; then
  safety_evidence_valid=1
fi

if (( safety_evidence_valid == 1 )) \
  && [[ "$broker_mutated" == "false" ]] \
  && [[ "$high_before" == "$high_after" ]] \
  && [[ "$message_before" == "$message_after" ]] \
  && [[ "$idemp_before" == "$idemp_after" ]]; then
  safety_state_result="PASS"
else
  safety_state_result="FAIL"
fi

if (( safety_evidence_valid == 1 )) \
  && is_non_negative_integer "$commit_before" \
  && is_non_negative_integer "$commit_after" \
  && [[ "$commit_before" == "$commit_after" ]]; then
  commit_result="PASS"
else
  commit_result="FAIL"
fi

jq -nc \
  --arg result "$safety_state_result" \
  --argjson broker_state_mutated "$broker_mutated" \
  --argjson high_watermark_before "$high_before" \
  --argjson high_watermark_after "$high_after" \
  --argjson message_count_before "$message_before" \
  --argjson message_count_after "$message_after" \
  --argjson idempotency_records_before "$idemp_before" \
  --argjson idempotency_records_after "$idemp_after" \
  '{id:"safety.broker_state",result:$result,broker_state_mutated:$broker_state_mutated,high_watermark_before:$high_watermark_before,high_watermark_after:$high_watermark_after,message_count_before:$message_count_before,message_count_after:$message_count_after,idempotency_records_before:$idempotency_records_before,idempotency_records_after:$idempotency_records_after}' >>"$ROWS"

jq -nc \
  --arg result "$commit_result" \
  --argjson before "$commit_before" \
  --argjson after "$commit_after" \
  '{id:"safety.committed_offset",result:$result,before:$before,after:$after,committed_offset_advanced:($after > $before)}' >>"$ROWS"

cat >"${RAW_DIR}/appended_events.ndjson" <<EOF_EVENTS
{"event_id":"pay_evt_004","payment_id":"pay_004","provider_order_id":"po_004","provider_commitment":"UNKNOWN","safe_to_close":false,"manual_reconciliation_required":true}
{"event_id":"pay_evt_005","payment_id":"pay_005","provider_order_id":"po_005","provider_commitment":"UNKNOWN","safe_to_close":false,"manual_reconciliation_required":true}
EOF_EVENTS

log "appending two more matching recovery events"
curl -fsS -X POST "${BASE}/broker/topics/${TOPIC}/produce-ndjson?partition=0&timeout_ms=5000&idempotency_key=${RUN_ID}_appended" \
  -H "$AUTH_HEADER" -H 'Content-Type: application/x-ndjson' \
  --data-binary @"${RAW_DIR}/appended_events.ndjson" \
  >"${RAW_DIR}/produce_appended.json"

headers_bounded_after="${RAW_DIR}/bounded_after_append.headers"
body_bounded_after="${RAW_DIR}/bounded_after_append.response.json"
log "rerunning bounded 0..3 QUERY after append"
code_bounded_after="$(query_request "${RAW_DIR}/bounded.json" "$headers_bounded_after" "$body_bounded_after")"
rows_bounded_after="$(jq '.rows | length' "$body_bounded_after")"
scanned_bounded_after="$(jq '.explain.messages_scanned // -1' "$body_bounded_after")"
matched_bounded_after="$(jq '.explain.messages_matched // -1' "$body_bounded_after")"
jq -nc \
  --arg result "$(result_for "$([[ "$code_bounded_after" == "200" && "$rows_bounded_after" == "2" && "$scanned_bounded_after" == "3" && "$matched_bounded_after" == "2" ]] && echo 1 || echo 0)")" \
  --argjson observed_status "$code_bounded_after" \
  --argjson matched_rows "$rows_bounded_after" \
  --argjson messages_scanned "$scanned_bounded_after" \
  --argjson messages_matched "$matched_bounded_after" \
  '{id:"safety.bounded_after_append",result:$result,observed_status:$observed_status,matched_rows:$matched_rows,messages_scanned:$messages_scanned,messages_matched:$messages_matched,detail:"Bounded sealed range 0..3 remains stable after new events are appended."}' >>"$ROWS"

headers_expanded="${RAW_DIR}/expanded.headers"
body_expanded="${RAW_DIR}/expanded.response.json"
log "running expanded 0..5 QUERY after append"
code_expanded="$(query_request "${RAW_DIR}/expanded.json" "$headers_expanded" "$body_expanded")"
rows_expanded="$(jq '.rows | length' "$body_expanded")"
fingerprint_expanded="$(jq -r '.query_fingerprint // ""' "$body_expanded")"
jq -nc \
  --arg result "$(result_for "$([[ "$code_expanded" == "200" && "$rows_expanded" == "4" && "$fingerprint_expanded" != "$fingerprint_first" ]] && echo 1 || echo 0)")" \
  --argjson observed_status "$code_expanded" \
  --argjson matched_rows "$rows_expanded" \
  --arg bounded_fingerprint "$fingerprint_first" \
  --arg expanded_fingerprint "$fingerprint_expanded" \
  '{id:"representation.expanded_query",result:$result,observed_status:$observed_status,matched_rows:$matched_rows,bounded_fingerprint:$bounded_fingerprint,expanded_fingerprint:$expanded_fingerprint}' >>"$ROWS"

jq -nc \
  --arg result "$(result_for "$([[ "$rows_expanded" == "4" ]] && echo 1 || echo 0)")" \
  --argjson matched_rows "$rows_expanded" \
  '{id:"safety.expanded_after_append",result:$result,matched_rows:$matched_rows,detail:"Expanded range 0..5 sees the two newly appended matching events."}' >>"$ROWS"

headers_collision_a="${RAW_DIR}/collision_a_again.headers"
body_collision_a="${RAW_DIR}/collision_a_again.response.json"
code_collision_a="$(query_request "${RAW_DIR}/bounded.json" "$headers_collision_a" "$body_collision_a")"
rows_collision_a="$(jq '.rows | length' "$body_collision_a")"
jq -nc \
  --argjson body_a_rows "$rows_collision_a" \
  --argjson body_b_rows "$rows_expanded" \
  --argjson observed_status "$code_collision_a" \
  '{id:"cache.collision_test",result:"UNVERIFIED",observed_status:$observed_status,body_a_rows:$body_a_rows,body_b_rows:$body_b_rows,detail:"A/B/A bodies returned distinct correct results, but no cache hit/miss or cache key evidence is measured yet."}' >>"$ROWS"

jq -nc \
  '{id:"cache.forwarded_not_cache_proof",result:"UNVERIFIED",detail:"Forwarding and revalidation are verified separately. Body-aware cache compatibility requires origin request count and cache hit/miss evidence."}' >>"$ROWS"

if [[ "$TARGET" == "ayder-envoy" && -n "${DIRECT_RECEIPT:-}" && -f "$DIRECT_RECEIPT" ]]; then
  direct_fp="$(jq -r '.rows[] | select(.id=="representation.first_bounded_query") | .fingerprint' "$DIRECT_RECEIPT")"
  direct_etag="$(jq -r '.rows[] | select(.id=="representation.first_bounded_query") | .etag' "$DIRECT_RECEIPT")"
  direct_result="$(jq -r '.rows[] | select(.id=="representation.first_bounded_query") | .result' "$DIRECT_RECEIPT")"
  direct_status="$(jq -r '.rows[] | select(.id=="representation.first_bounded_query") | .observed_status' "$DIRECT_RECEIPT")"
  transport_valid=0
  if [[ "$direct_result" == "PASS" ]] \
    && [[ "$direct_status" == "200" ]] \
    && [[ "$code_first" == "200" ]] \
    && is_sha256_fingerprint "$direct_fp" \
    && is_sha256_fingerprint "$fingerprint_first" \
    && [[ "$direct_fp" == "$fingerprint_first" ]] \
    && is_nonempty_etag "$direct_etag" \
    && is_nonempty_etag "$etag_first" \
    && [[ "$direct_etag" == "$etag_first" ]]; then
    transport_valid=1
  fi

  jq -nc \
    --arg result "$(result_for "$transport_valid")" \
    --arg direct_status "$direct_status" \
    --arg through_envoy_status "$code_first" \
    --arg direct_fingerprint "$direct_fp" \
    --arg through_envoy_fingerprint "$fingerprint_first" \
    --arg direct_etag "$direct_etag" \
    --arg through_envoy_etag "$etag_first" \
    '{id:"transport.direct_envoy_equivalence",result:$result,direct:{status:($direct_status | tonumber? // $direct_status),fingerprint:$direct_fingerprint,etag:$direct_etag},through_envoy:{status:($through_envoy_status | tonumber? // $through_envoy_status),fingerprint:$through_envoy_fingerprint,etag:$through_envoy_etag},transport_equivalence:$result,direct_fingerprint:$direct_fingerprint,through_envoy_fingerprint:$through_envoy_fingerprint}' >>"$ROWS"
fi

receipt_path="$("${SCRIPT_DIR}/render-receipt.py" \
  --target "$TARGET" \
  --base-url "$BASE" \
  --commit "$COMMIT_SHA" \
  --rows "$ROWS" \
  --out-dir "$OUT_DIR")"

cat "$receipt_path"
