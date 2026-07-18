#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

BASE="${BASE:-http://127.0.0.1:1109}"
TOKEN="${TOKEN:-dev}"
AUTH_HEADER="Authorization: Bearer ${TOKEN}"
JSON_HEADER="Content-Type: application/json"
RUN_ID="http_query_recovery_snapshot_$(date -u +%Y%m%dT%H%M%SZ)"
TOPIC="payment_events_${RUN_ID}"
GROUP="payment_worker_${RUN_ID}"
ARTIFACT_DIR="${ARTIFACT_DIR:-${REPO_ROOT}/artifacts/http_query_recovery_snapshot/${RUN_ID}}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing command: $1" >&2
    exit 127
  }
}

log() {
  printf "[%s] %s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
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

require_cmd curl
require_cmd jq
require_cmd awk
require_cmd mktemp

mkdir -p "$ARTIFACT_DIR"

log "checking Ayder health at ${BASE}"
curl -fsS "${BASE}/health" >/dev/null

log "creating topic ${TOPIC}"
curl -fsS -X POST "${BASE}/broker/topics" \
  -H "$AUTH_HEADER" -H "$JSON_HEADER" \
  --data-binary "$(jq -nc --arg name "$TOPIC" '{name:$name,partitions:1}')" \
  >"${ARTIFACT_DIR}/create_topic.json"

cat >"${ARTIFACT_DIR}/initial_events.ndjson" <<EOF
{"event_id":"pay_evt_001","payment_id":"pay_001","provider_order_id":"po_001","provider_commitment":"UNKNOWN","safe_to_close":false,"manual_reconciliation_required":true}
{"event_id":"pay_evt_002","payment_id":"pay_002","provider_order_id":"po_002","provider_commitment":"SUCCESS","safe_to_close":true,"manual_reconciliation_required":false}
{"event_id":"pay_evt_003","payment_id":"pay_003","provider_order_id":"po_003","provider_commitment":"UNKNOWN","safe_to_close":false,"manual_reconciliation_required":true}
EOF

log "producing initial recovery events"
curl -fsS -X POST "${BASE}/broker/topics/${TOPIC}/produce-ndjson?partition=0&timeout_ms=5000&idempotency_key=${RUN_ID}_initial" \
  -H "$AUTH_HEADER" -H 'Content-Type: application/x-ndjson' \
  --data-binary @"${ARTIFACT_DIR}/initial_events.ndjson" \
  >"${ARTIFACT_DIR}/produce_initial.json"

log "setting committed offset baseline for ${GROUP}"
curl -fsS -X POST "${BASE}/broker/commit" \
  -H "$AUTH_HEADER" -H "$JSON_HEADER" \
  --data-binary "$(jq -nc --arg topic "$TOPIC" --arg group "$GROUP" '{topic:$topic,group:$group,partition:0,offset:0}')" \
  >"${ARTIFACT_DIR}/commit_baseline.json"

cat >"${ARTIFACT_DIR}/query_a.json" <<EOF
{
  "source": {
    "topic": "${TOPIC}",
    "partition": 0,
    "from_offset": 0,
    "to_offset": 3,
    "limit": 10,
    "sealed_only": true
  },
  "filter": [
    {
      "field": "provider_commitment",
      "op": "eq",
      "value": "UNKNOWN"
    },
    {
      "field": "safe_to_close",
      "op": "eq",
      "value": false
    }
  ],
  "include": {
    "metadata": true,
    "commit_status_for_group": "${GROUP}"
  },
  "transform": {
    "left_fields": [
      "event_id",
      "payment_id",
      "provider_order_id",
      "provider_commitment",
      "safe_to_close",
      "manual_reconciliation_required"
    ]
  },
  "explain": true
}
EOF

cat >"${ARTIFACT_DIR}/query_b.json" <<EOF
{"explain":true,"transform":{"left_fields":["event_id","payment_id","provider_order_id","provider_commitment","safe_to_close","manual_reconciliation_required"]},"include":{"commit_status_for_group":"${GROUP}","metadata":true},"filter":[{"value":"UNKNOWN","op":"eq","field":"provider_commitment"},{"value":false,"field":"safe_to_close","op":"eq"}],"source":{"sealed_only":true,"limit":10,"to_offset":3,"from_offset":0,"partition":0,"topic":"${TOPIC}"}}
EOF

headers_a="${ARTIFACT_DIR}/query_a.headers"
body_a="${ARTIFACT_DIR}/query_a.response.json"
log "running bounded recovery QUERY"
code_a="$(query_request "${ARTIFACT_DIR}/query_a.json" "$headers_a" "$body_a")"
[[ "$code_a" == "200" ]] || { echo "expected query_a HTTP 200, got ${code_a}" >&2; cat "$body_a" >&2; exit 1; }

etag="$(header_value ETag "$headers_a")"
accept_query="$(header_value Accept-Query "$headers_a")"
content_location="$(header_value Content-Location "$headers_a")"
expose_headers="$(header_value Access-Control-Expose-Headers "$headers_a")"

[[ "$accept_query" == "application/json" ]] || { echo "missing Accept-Query header" >&2; cat "$headers_a" >&2; exit 1; }
[[ "$etag" == \"ayder-query-sha256-*\" ]] || { echo "missing Ayder query ETag" >&2; cat "$headers_a" >&2; exit 1; }
[[ -z "$content_location" ]] || { echo "unexpected Content-Location before result resources are retrievable" >&2; cat "$headers_a" >&2; exit 1; }
[[ "$expose_headers" != *Content-Location* ]] || { echo "Access-Control-Expose-Headers must not expose Content-Location" >&2; cat "$headers_a" >&2; exit 1; }

jq -e '
  .ok == true and
  (.rows | length) == 2 and
  (.query_fingerprint | startswith("sha256:")) and
  .snapshot.from_offset == 0 and
  .snapshot.to_offset == 3 and
  .safety.broker_state_mutated == false and
  .safety.high_watermark_before == 3 and
  .safety.high_watermark_after == 3 and
  .safety.message_count_before == 3 and
  .safety.message_count_after == 3 and
  .safety.committed_offset_before == 0 and
  .safety.committed_offset_after == 0 and
  .safety.idempotency_records_before == .safety.idempotency_records_after and
  .explain.plan == "bounded_partition_scan" and
  .explain.messages_scanned == 3 and
  .explain.messages_matched == 2 and
  .explain.commits_mutated == false and
  .explain.broker_state_mutated == false
' "$body_a" >/dev/null

fingerprint_a="$(jq -r '.query_fingerprint' "$body_a")"

headers_b="${ARTIFACT_DIR}/query_b.headers"
body_b="${ARTIFACT_DIR}/query_b.response.json"
log "running semantically identical canonical JSON QUERY"
code_b="$(query_request "${ARTIFACT_DIR}/query_b.json" "$headers_b" "$body_b")"
[[ "$code_b" == "200" ]] || { echo "expected query_b HTTP 200, got ${code_b}" >&2; cat "$body_b" >&2; exit 1; }
fingerprint_b="$(jq -r '.query_fingerprint' "$body_b")"
[[ "$fingerprint_a" == "$fingerprint_b" ]] || {
  echo "canonical fingerprints differ: ${fingerprint_a} vs ${fingerprint_b}" >&2
  exit 1
}

headers_304="${ARTIFACT_DIR}/query_304.headers"
body_304="${ARTIFACT_DIR}/query_304.response"
log "running conditional QUERY with If-None-Match"
code_304="$(query_request "${ARTIFACT_DIR}/query_a.json" "$headers_304" "$body_304" -H "If-None-Match: ${etag}")"
[[ "$code_304" == "304" ]] || { echo "expected conditional QUERY HTTP 304, got ${code_304}" >&2; cat "$headers_304" >&2; cat "$body_304" >&2; exit 1; }

unsupported_code="$(curl -sS -o "${ARTIFACT_DIR}/unsupported_media_type.response.json" -w '%{http_code}' \
  -X QUERY "${BASE}/broker/query" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: text/plain' \
  --data-binary @"${ARTIFACT_DIR}/query_a.json")"
[[ "$unsupported_code" == "415" ]] || { echo "expected unsupported media type HTTP 415, got ${unsupported_code}" >&2; exit 1; }

cat >"${ARTIFACT_DIR}/appended_events.ndjson" <<EOF
{"event_id":"pay_evt_004","payment_id":"pay_004","provider_order_id":"po_004","provider_commitment":"UNKNOWN","safe_to_close":false,"manual_reconciliation_required":true}
{"event_id":"pay_evt_005","payment_id":"pay_005","provider_order_id":"po_005","provider_commitment":"UNKNOWN","safe_to_close":false,"manual_reconciliation_required":true}
EOF

log "appending more ambiguous recovery events"
curl -fsS -X POST "${BASE}/broker/topics/${TOPIC}/produce-ndjson?partition=0&timeout_ms=5000&idempotency_key=${RUN_ID}_appended" \
  -H "$AUTH_HEADER" -H 'Content-Type: application/x-ndjson' \
  --data-binary @"${ARTIFACT_DIR}/appended_events.ndjson" \
  >"${ARTIFACT_DIR}/produce_appended.json"

headers_bounded_after="${ARTIFACT_DIR}/query_bounded_after_append.headers"
body_bounded_after="${ARTIFACT_DIR}/query_bounded_after_append.response.json"
log "rerunning original bounded QUERY after append"
code_bounded_after="$(query_request "${ARTIFACT_DIR}/query_a.json" "$headers_bounded_after" "$body_bounded_after")"
[[ "$code_bounded_after" == "200" ]] || { echo "expected bounded-after HTTP 200, got ${code_bounded_after}" >&2; exit 1; }
jq -e '(.rows | length) == 2 and .explain.messages_scanned == 3 and .explain.messages_matched == 2' "$body_bounded_after" >/dev/null

cat >"${ARTIFACT_DIR}/query_expanded.json" <<EOF
{
  "source": {
    "topic": "${TOPIC}",
    "partition": 0,
    "from_offset": 0,
    "to_offset": 5,
    "limit": 10,
    "sealed_only": true
  },
  "filter": [
    {"field": "provider_commitment", "op": "eq", "value": "UNKNOWN"},
    {"field": "safe_to_close", "op": "eq", "value": false}
  ],
  "include": {
    "commit_status_for_group": "${GROUP}"
  },
  "transform": {
    "left_fields": ["event_id", "payment_id", "provider_order_id", "provider_commitment", "safe_to_close", "manual_reconciliation_required"]
  },
  "explain": true
}
EOF

headers_expanded="${ARTIFACT_DIR}/query_expanded.headers"
body_expanded="${ARTIFACT_DIR}/query_expanded.response.json"
log "running expanded sealed-range QUERY"
code_expanded="$(query_request "${ARTIFACT_DIR}/query_expanded.json" "$headers_expanded" "$body_expanded")"
[[ "$code_expanded" == "200" ]] || { echo "expected expanded HTTP 200, got ${code_expanded}" >&2; exit 1; }
jq -e '(.rows | length) == 4 and .explain.messages_scanned == 5 and .explain.messages_matched == 4' "$body_expanded" >/dev/null

jq -n \
  --arg demo "http_query_recovery_snapshot" \
  --arg verdict "GREEN" \
  --arg operator_answer "Ayder can run safe, bounded, read-only recovery queries over durable broker history using HTTP QUERY request content." \
  --arg accept_query "$accept_query" \
  --arg query_fingerprint "$fingerprint_a" \
  --arg etag "$etag" \
  --argjson high_before "$(jq '.safety.high_watermark_before' "$body_a")" \
  --argjson high_after "$(jq '.safety.high_watermark_after' "$body_a")" \
  --argjson commit_before "$(jq '.safety.committed_offset_before' "$body_a")" \
  --argjson commit_after "$(jq '.safety.committed_offset_after' "$body_a")" \
  --argjson scanned "$(jq '.explain.messages_scanned' "$body_a")" \
  --argjson matched "$(jq '.explain.messages_matched' "$body_a")" \
  '{
    demo: $demo,
    verdict: $verdict,
    operator_answer: $operator_answer,
    http_query_evidence: {
      method: "QUERY",
      accept_query: $accept_query,
      query_fingerprint: $query_fingerprint,
      etag: $etag,
      content_location_advertised: false,
      conditional_query_returned_304: true,
      unsupported_media_type_rejected: true,
      canonical_json_same_fingerprint: true
    },
    safety_evidence: {
      broker_state_mutated: false,
      high_watermark_before: $high_before,
      high_watermark_after: $high_after,
      committed_offset_before: $commit_before,
      committed_offset_after: $commit_after
    },
    snapshot_evidence: {
      bounded_range: "0..3",
      sealed_only: true,
      same_result_after_new_events_appended: true
    },
    query_result: {
      messages_scanned: $scanned,
      messages_matched: $matched,
      unknown_payments_found: $matched,
      manual_reconciliation_required: true
    }
  }' >"${ARTIFACT_DIR}/receipt.json"

cat >"${ARTIFACT_DIR}/receipt.txt" <<EOF
============================================================
 AYDER HTTP QUERY RECOVERY SNAPSHOT RECEIPT
============================================================
 demo            : http_query_recovery_snapshot
 verdict         : GREEN
 operator_answer : Ayder can run safe, bounded, read-only recovery
                   queries over durable broker history using HTTP QUERY.
------------------------------------------------------------
 HTTP QUERY:
   accept_query: ${accept_query}
   etag: ${etag}
   content_location_advertised: false
   fingerprint: ${fingerprint_a}
   conditional_query_returned_304: true
   canonical_json_same_fingerprint: true
------------------------------------------------------------
 SAFETY:
   broker_state_mutated: false
   high_watermark_before: $(jq -r '.safety.high_watermark_before' "$body_a")
   high_watermark_after: $(jq -r '.safety.high_watermark_after' "$body_a")
   committed_offset_before: $(jq -r '.safety.committed_offset_before' "$body_a")
   committed_offset_after: $(jq -r '.safety.committed_offset_after' "$body_a")
------------------------------------------------------------
 SNAPSHOT:
   bounded_range: 0..3
   same_result_after_new_events_appended: true
   expanded_range_0..5_unknown_payments: $(jq -r '.rows | length' "$body_expanded")
------------------------------------------------------------
 QUERY RESULT:
   messages_scanned: $(jq -r '.explain.messages_scanned' "$body_a")
   messages_matched: $(jq -r '.explain.messages_matched' "$body_a")
   unknown_payments_found: $(jq -r '.rows | length' "$body_a")
   manual_reconciliation_required: true
------------------------------------------------------------
 artifacts: ${ARTIFACT_DIR}
============================================================
EOF

cat "${ARTIFACT_DIR}/receipt.txt"
log "http QUERY recovery snapshot demo success"
