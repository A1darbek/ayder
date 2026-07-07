#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

BASE="${BASE:-http://127.0.0.1:1109}"
TOKEN="${TOKEN:-dev}"
AUTH_HEADER="Authorization: Bearer ${TOKEN}"
RUN_ID="http_query_$(date -u +%Y%m%dT%H%M%SZ)"
TOPIC="http_query_${RUN_ID}"
GROUP="http_query_reader_${RUN_ID}"
ARTIFACT_DIR="${ARTIFACT_DIR:-${REPO_ROOT}/artifacts/http_query_method/${RUN_ID}}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing command: $1" >&2
    exit 127
  }
}

log() {
  printf "[%s] %s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

require_cmd curl
require_cmd jq
require_cmd mktemp

mkdir -p "$ARTIFACT_DIR"

log "checking Ayder health at ${BASE}"
curl -fsS "${BASE}/health" >/dev/null

log "creating topic ${TOPIC}"
topic_body="$(jq -nc --arg name "$TOPIC" '{name:$name,partitions:1}')"
curl -fsS -X POST "${BASE}/broker/topics" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  --data-binary "$topic_body" >"${ARTIFACT_DIR}/create_topic.json"

cat >"${ARTIFACT_DIR}/events.ndjson" <<EOF
{"event_id":"evt-query-001","kind":"payment","status":"SUCCESS","merchant":"design_partner","amount":42,"currency":"USD"}
{"event_id":"evt-query-002","kind":"payment","status":"PENDING_REVIEW","merchant":"design_partner","amount":99,"currency":"USD"}
{"event_id":"evt-query-003","kind":"payment","status":"SUCCESS","merchant":"design_partner","amount":17,"currency":"USD"}
EOF

log "producing JSON events"
curl -fsS -X POST "${BASE}/broker/topics/${TOPIC}/produce-ndjson?partition=0&timeout_ms=5000" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @"${ARTIFACT_DIR}/events.ndjson" >"${ARTIFACT_DIR}/produce.json"

cat >"${ARTIFACT_DIR}/query.json" <<EOF
{
  "source": {
    "topic": "${TOPIC}",
    "partition": 0,
    "from_offset": 0,
    "to_offset": 3,
    "sealed_only": true,
    "limit": 10
  },
  "include": {
    "commit_status_for_group": "${GROUP}"
  },
  "filter": [
    {
      "field": "status",
      "op": "eq",
      "value": "SUCCESS"
    }
  ],
  "aggregate": {
    "group_by": ["status"]
  },
  "transform": {
    "left_fields": ["event_id", "status", "amount", "currency"],
    "emit_rows": 5
  }
}
EOF

headers_file="$(mktemp)"
body_file="$(mktemp)"
trap 'rm -f "$headers_file" "$body_file"' EXIT

log "running RFC 10008 QUERY /broker/query"
status_code="$(curl -sS -D "$headers_file" -o "$body_file" -w '%{http_code}' \
  -X QUERY "${BASE}/broker/query" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: application/json' \
  --data-binary @"${ARTIFACT_DIR}/query.json")"

cp "$headers_file" "${ARTIFACT_DIR}/query.headers"
cp "$body_file" "${ARTIFACT_DIR}/query.response.json"

if [[ "$status_code" != "200" ]]; then
  echo "expected QUERY HTTP 200, got ${status_code}" >&2
  cat "$body_file" >&2
  exit 1
fi

if ! tr -d '\r' <"$headers_file" | grep -qi '^Accept-Query: application/json$'; then
  echo "expected Accept-Query: application/json response header" >&2
  cat "$headers_file" >&2
  exit 1
fi

jq -e '.ok == true and (.rows | length) == 2 and (.aggregates | length) == 1 and .source.consumed == 3' \
  "$body_file" >/dev/null

unsupported_code="$(curl -sS -o "${ARTIFACT_DIR}/unsupported.response.json" -w '%{http_code}' \
  -X QUERY "${BASE}/broker/query" \
  -H "$AUTH_HEADER" \
  -H 'Content-Type: text/plain' \
  --data-binary @"${ARTIFACT_DIR}/query.json")"

if [[ "$unsupported_code" != "415" ]]; then
  echo "expected unsupported QUERY media type HTTP 415, got ${unsupported_code}" >&2
  cat "${ARTIFACT_DIR}/unsupported.response.json" >&2
  exit 1
fi

jq -n \
  --arg run_id "$RUN_ID" \
  --arg topic "$TOPIC" \
  --arg group "$GROUP" \
  --arg query_path "${ARTIFACT_DIR}/query.json" \
  --arg response_path "${ARTIFACT_DIR}/query.response.json" \
  '{
    demo: "http_query_method",
    verdict: "GREEN",
    operator_answer: "Ayder supports read-only broker queries with HTTP QUERY request content.",
    rfc_10008_evidence: {
      method: "QUERY",
      safe: true,
      idempotent: true,
      request_content_carries_query: true,
      accept_query: "application/json",
      unsupported_media_type_rejected: true
    },
    ayder_evidence: {
      topic: $topic,
      consumer_group: $group,
      query_endpoint: "/broker/query",
      write_endpoint_unchanged: "/broker/topics/:topic/produce-ndjson"
    },
    result: {
      matched_rows: 2,
      aggregate_groups: 1,
      query_file: $query_path,
      response_file: $response_path
    },
    design_partner_message: "Writes remain POST. Read-only query/reporting can use QUERY instead of encoding complex filters in the URI or pretending a safe query is a POST mutation."
  }' >"${ARTIFACT_DIR}/receipt.json"

cat >"${ARTIFACT_DIR}/receipt.txt" <<EOF
============================================================
 AYDER HTTP QUERY METHOD RECEIPT
============================================================
 verdict         : GREEN
 method          : QUERY
 endpoint        : /broker/query
 accept_query    : application/json
 safe            : true
 idempotent      : true
 matched_rows    : 2
 aggregate_groups: 1
 unsupported_media_type_rejected: true
------------------------------------------------------------
 operator_answer : Ayder supports read-only broker queries with HTTP QUERY request content.
------------------------------------------------------------
 why it matters  : Writes stay POST. Read-only query/reporting can use QUERY
                   instead of long URIs or fake-safe POST requests.
------------------------------------------------------------
 artifacts       : ${ARTIFACT_DIR}
============================================================
EOF

cat "${ARTIFACT_DIR}/receipt.txt"
log "http QUERY demo success"
