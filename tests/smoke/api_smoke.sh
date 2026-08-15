#!/usr/bin/env bash
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ -z "${PORT:-}" ]]; then
  if [[ -n "${BASE:-}" && "$BASE" =~ :([0-9]+)(/.*)?$ ]]; then
    PORT="${BASH_REMATCH[1]}"
  else
    PORT="1109"
  fi
fi
BASE="${BASE:-http://127.0.0.1:${PORT}}"
TOKEN="${TOKEN:-dev}"
START_SERVER="${START_SERVER:-0}"
SERVER_BIN="${SERVER_BIN:-${REPO_ROOT}/ayder}"
SERVER_WORKERS="${SERVER_WORKERS:-0}"
STARTUP_TIMEOUT_SECS="${STARTUP_TIMEOUT_SECS:-15}"

AUTH_ARGS=()
if [[ -n "${TOKEN}" ]]; then
  AUTH_ARGS=(-H "Authorization: Bearer ${TOKEN}")
fi

FAILS=0
PASSES=0
SKIPS=0
RESP_BODY=""
RESP_CODE=""
SERVER_PID=""
SERVER_TMP=""
ROUTE_COVERAGE="${ROUTE_COVERAGE:-1}"
declare -A COVERED_ROUTES=()

normalize_route_id() {
  local raw="$1"
  printf '%s' "$raw" | sed -E \
    -e 's#:[A-Za-z_][A-Za-z0-9_]*\+#:param+#g' \
    -e 's#:[A-Za-z_][A-Za-z0-9_]*#:param#g'
}

mark_route() {
  local method="$1"
  local path="$2"
  local key
  key="$(normalize_route_id "${method} ${path}")"
  COVERED_ROUTES["$key"]=1
}

verify_route_coverage() {
  if [[ "$ROUTE_COVERAGE" != "1" ]]; then
    skip "route_coverage" "disabled (ROUTE_COVERAGE=${ROUTE_COVERAGE})"
    return 0
  fi

  local routes_file="${REPO_ROOT}/src/app_routes.c"
  if [[ ! -f "$routes_file" ]]; then
    skip "route_coverage" "missing ${routes_file}"
    return 0
  fi

  local missing=0
  local total=0
  while IFS= read -r line; do
    local method path key
    if [[ "$line" =~ app-\>(get|post|delete|query)[[:space:]]*\([[:space:]]*app[[:space:]]*,[[:space:]]*\"([^\"]+)\" ]]; then
      method="${BASH_REMATCH[1]^^}"
      path="${BASH_REMATCH[2]}"
    elif [[ "$line" =~ register_route[[:space:]]*\([[:space:]]*\"([A-Z]+)\"[[:space:]]*,[[:space:]]*\"([^\"]+)\" ]]; then
      method="${BASH_REMATCH[1]}"
      path="${BASH_REMATCH[2]}"
    else
      continue
    fi
    key="$(normalize_route_id "${method} ${path}")"
    total=$((total + 1))
    if [[ -z "${COVERED_ROUTES[$key]:-}" ]]; then
      fail "route_coverage" "missing smoke coverage for ${key}"
      missing=$((missing + 1))
    fi
  done < <(grep -E 'app->(get|post|delete|query)\s*\(\s*app\s*,\s*"/|register_route\s*\(\s*"[A-Z]+"\s*,\s*"/' "$routes_file")

  if (( missing == 0 )); then
    pass "route_coverage" "covered ${total}/${total} registered routes"
  fi
}

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'FAIL dependency                         missing %s\n' "$1"
    exit 127
  fi
}

pass() {
  PASSES=$((PASSES + 1))
  printf 'PASS %-34s %s\n' "$1" "$2"
}

fail() {
  FAILS=$((FAILS + 1))
  printf 'FAIL %-34s %s\n' "$1" "$2"
}

skip() {
  SKIPS=$((SKIPS + 1))
  printf 'SKIP %-34s %s\n' "$1" "$2"
}

snippet() {
  printf '%s' "$1" | tr '\n' ' ' | cut -c 1-300
}

request() {
  local method="$1"
  local path="$2"
  local body="${3-}"
  local content_type="${4:-application/json}"
  local tmp
  tmp="$(mktemp)"

  local curl_args=(-sS -o "$tmp" -w "%{http_code}" -X "$method")
  curl_args+=("${AUTH_ARGS[@]}")
  if [[ $# -ge 3 ]]; then
    curl_args+=(-H "Content-Type: ${content_type}" --data-binary "$body")
  fi
  curl_args+=("${BASE}${path}")

  RESP_CODE="$(curl "${curl_args[@]}" 2>"${tmp}.err" || printf '000')"
  RESP_BODY="$(cat "$tmp")"
  local err
  err="$(cat "${tmp}.err")"
  rm -f "$tmp" "${tmp}.err"

  if [[ "$RESP_CODE" == "000" && -n "$err" ]]; then
    RESP_BODY="$err"
  fi
}

expect_2xx() {
  local label="$1"
  if [[ "$RESP_CODE" =~ ^2[0-9][0-9]$ ]]; then
    return 0
  fi
  fail "$label" "HTTP ${RESP_CODE}: $(snippet "$RESP_BODY")"
  return 1
}

expect_json() {
  local label="$1"
  shift
  if ! expect_2xx "$label"; then
    return 1
  fi
  if printf '%s' "$RESP_BODY" | jq -e "$@" >/dev/null 2>&1; then
    pass "$label" "HTTP ${RESP_CODE}"
    return 0
  fi
  fail "$label" "JSON check failed: $(snippet "$RESP_BODY")"
  return 1
}

expect_json_any_status() {
  local label="$1"
  shift
  if printf '%s' "$RESP_BODY" | jq -e "$@" >/dev/null 2>&1; then
    pass "$label" "HTTP ${RESP_CODE}"
    return 0
  fi
  fail "$label" "JSON check failed: HTTP ${RESP_CODE}: $(snippet "$RESP_BODY")"
  return 1
}

expect_text() {
  local label="$1"
  local pattern="${2:-.}"
  if ! expect_2xx "$label"; then
    return 1
  fi
  if [[ -n "$RESP_BODY" ]] && printf '%s' "$RESP_BODY" | grep -Eq "$pattern"; then
    pass "$label" "HTTP ${RESP_CODE}"
    return 0
  fi
  fail "$label" "text check failed: $(snippet "$RESP_BODY")"
  return 1
}

wait_for_server() {
  local deadline=$((SECONDS + STARTUP_TIMEOUT_SECS))
  while (( SECONDS < deadline )); do
    request GET /health
    if [[ "$RESP_CODE" == "200" ]] && printf '%s' "$RESP_BODY" | jq -e '.ok==1' >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

need curl
need jq
need mktemp
need base64
need realpath

if [[ "$START_SERVER" == "1" ]]; then
  if [[ ! -x "$SERVER_BIN" ]]; then
    printf 'FAIL start_server                       binary not executable: %s\n' "$SERVER_BIN"
    exit 1
  fi
  SERVER_BIN="$(realpath -e -- "$SERVER_BIN")"
  SERVER_TMP="$(mktemp -d)"
  (
    cd "$SERVER_TMP" || exit 1
    if [[ -n "$TOKEN" && -z "${RF_BEARER_TOKENS:-}" ]]; then
      export RF_BEARER_TOKENS="${TOKEN}@55555555555555:11111111111111111:111111111111111111111"
    fi
    "$SERVER_BIN" --port "$PORT" --workers "$SERVER_WORKERS" >ayder-smoke.log 2>&1
  ) &
  SERVER_PID="$!"
  if ! wait_for_server; then
    printf 'FAIL start_server                       server did not become healthy at %s\n' "$BASE"
    printf 'INFO start_server                       log: %s/ayder-smoke.log\n' "$SERVER_TMP"
    exit 1
  fi
  pass "start_server" "pid ${SERVER_PID} at ${BASE}"
else
  if ! wait_for_server; then
    printf 'FAIL connect                            no healthy server at %s\n' "$BASE"
    exit 1
  fi
fi

RUN_ID="smoke_$(date +%Y%m%d%H%M%S)_$$"
TOPIC="api_${RUN_ID}"
RIGHT_TOPIC="api_right_${RUN_ID}"
GROUP="group_${RUN_ID}"
QGROUP="query_${RUN_ID}"
JGROUP_L="join_l_${RUN_ID}"
JGROUP_R="join_r_${RUN_ID}"
NS="smoke"
KEY="api/${RUN_ID}"

printf 'INFO base                               %s\n' "$BASE"
printf 'INFO run_id                             %s\n' "$RUN_ID"

request GET /health
mark_route GET /health
expect_json "GET /health" '.ok==1'

request GET /ready
mark_route GET /ready
if [[ "$RESP_CODE" == "404" ]]; then
  skip "GET /ready" "route not present"
else
  expect_json "GET /ready" '(.ready | type) == "boolean"'
  if [[ "$RESP_CODE" == "200" ]] && ! printf '%s' "$RESP_BODY" | jq -e '.ready==true' >/dev/null 2>&1; then
    fail "GET /ready" "server is not ready for writes: $(snippet "$RESP_BODY")"
  fi
fi

request GET /metrics
mark_route GET /metrics
expect_text "GET /metrics"

request GET /metrics_ha
mark_route GET /metrics_ha
expect_text "GET /metrics_ha"

request POST /admin/compact '{}'
mark_route POST /admin/compact
expect_json "POST /admin/compact" '.result=="compaction_started" and .async==true'

request GET /admin/sealed/status
mark_route GET /admin/sealed/status
expect_json "GET /admin/sealed/status" '.ok==true and (.last_synced_batch_id|type)=="number"'

request POST /admin/sealed/gc '{"max_age_ms":0,"max_bytes":0}'
mark_route POST /admin/sealed/gc
if [[ "$RESP_CODE" == "503" ]]; then
  expect_json_any_status "POST /admin/sealed/gc" \
    '.ok==false and .error=="sealed_gc_unavailable"'
else
  fail "POST /admin/sealed/gc" "expected disabled 503 contract: HTTP ${RESP_CODE}: $(snippet "$RESP_BODY")"
fi

request POST /admin/sealed/release '{}'
mark_route POST /admin/sealed/release
expect_json "POST /admin/sealed/release" '.ok==true and .released==1'

request GET /ha/membership
mark_route GET /ha/membership
expect_json_any_status "GET /ha/membership" '(.ok==true) or (.error=="ha_not_enabled")'

request POST /ha/membership/add '{"node_id":"smoke-node","advertise_addr":"127.0.0.1:7701","priority":1}'
mark_route POST /ha/membership/add
expect_json_any_status "POST /ha/membership/add" '(.ok==true) or (.error|type=="string")'

request POST /ha/membership/promote '{"node_id":"smoke-node"}'
mark_route POST /ha/membership/promote
expect_json_any_status "POST /ha/membership/promote" '(.ok==true) or (.error|type=="string")'

request POST /ha/membership/remove '{"node_id":"smoke-node"}'
mark_route POST /ha/membership/remove
expect_json_any_status "POST /ha/membership/remove" '(.ok==true) or (.error|type=="string")'

topic_body="$(jq -nc --arg name "$TOPIC" '{name:$name,partitions:2}')"
request POST /broker/topics "$topic_body"
mark_route POST /broker/topics
expect_json "POST /broker/topics" --arg topic "$TOPIC" '.ok==true and .topic==$topic and .partitions==2'

right_topic_body="$(jq -nc --arg name "$RIGHT_TOPIC" '{name:$name,partitions:1}')"
request POST /broker/topics "$right_topic_body"
expect_json "POST /broker/topics right" --arg topic "$RIGHT_TOPIC" '.ok==true and .topic==$topic'

raw_payload="raw-${RUN_ID}"
request POST "/broker/topics/${TOPIC}/produce?partition=0&timeout_ms=5000" "$raw_payload" "text/plain"
mark_route POST /broker/topics/:topic/produce
if expect_json "POST /broker/topics/:topic/produce" '.ok==true and (.offset|type)=="number" and .partition==0'; then
  RAW_OFFSET="$(printf '%s' "$RESP_BODY" | jq -r '.offset')"
else
  RAW_OFFSET=0
fi

left_lines="$(printf '%s\n%s\n' \
  "{\"kind\":\"left\",\"join_id\":\"a-${RUN_ID}\",\"score\":7}" \
  "{\"kind\":\"left\",\"join_id\":\"b-${RUN_ID}\",\"score\":3}")"
request POST "/broker/topics/${TOPIC}/produce-ndjson?partition=0&timeout_ms=5000" "$left_lines" "application/x-ndjson"
mark_route POST /broker/topics/:topic/produce-ndjson
expect_json "POST /broker/topics/:topic/produce-ndjson" '.ok==true and .count==2 and .partition==0'

right_lines="$(printf '%s\n' "{\"kind\":\"right\",\"join_id\":\"a-${RUN_ID}\",\"price\":9}")"
request POST "/broker/topics/${RIGHT_TOPIC}/produce-ndjson?partition=0&timeout_ms=5000" "$right_lines" "application/x-ndjson"
expect_json "POST /broker/topics/:right/produce-ndjson" '.ok==true and .count==1 and .partition==0'

request GET "/broker/consume/${TOPIC}/${GROUP}/0?offset=0&limit=10&encoding=b64"
mark_route GET /broker/consume/:topic/:group/:partition
if expect_json "GET /broker/consume/:topic/:group/:partition" '.count>=3 and (.messages|length)>=3 and (.messages[0].value_b64|type)=="string"'; then
  NEXT_OFFSET="$(printf '%s' "$RESP_BODY" | jq -r '.next_offset')"
else
  NEXT_OFFSET=1
fi

commit_body="$(jq -nc --arg topic "$TOPIC" --arg group "$GROUP" --argjson offset "$RAW_OFFSET" \
  '{topic:$topic,group:$group,partition:0,offset:$offset}')"
request POST /broker/commit "$commit_body"
mark_route POST /broker/commit
expect_json "POST /broker/commit" '.ok==true'

retention_body="$(jq -nc --arg topic "$TOPIC" '{topic:$topic,partition:0,ttl_ms:600000,max_bytes:1048576}')"
request POST /broker/retention "$retention_body"
mark_route POST /broker/retention
expect_json "POST /broker/retention" '.ok==true and .ttl_applied==true and .size_applied==true'

delete_before_body="$(jq -nc --arg topic "$TOPIC" '{topic:$topic,partition:0,before_offset:0}')"
request POST /broker/delete-before "$delete_before_body"
mark_route POST /broker/delete-before
expect_json "POST /broker/delete-before" '.ok==true and (.deleted_count|type)=="number" and (.freed_bytes|type)=="number"'

query_body="$(jq -nc --arg topic "$TOPIC" --arg group "$QGROUP" \
  '{source:{topic:$topic,group:$group,partition:0,offset:0,limit:10},
    filter:[{field:"kind",op:"eq",value:"left"}],
    aggregate:{group_by:["kind"]},
    transform:{left_fields:["kind","join_id"],emit_rows:2}}')"
request POST /broker/query "$query_body"
mark_route POST /broker/query
expect_json "POST /broker/query" '.ok==true and (.rows|length)>=2 and (.aggregates|length)>=1 and .source.consumed>=2'

request QUERY /broker/query "$query_body"
mark_route QUERY /broker/query
expect_json "QUERY /broker/query" '.ok==true and (.rows|length)>=2 and (.aggregates|length)>=1 and .source.consumed>=2'

join_body="$(jq -nc \
  --arg left "$TOPIC" --arg right "$RIGHT_TOPIC" \
  --arg gl "$JGROUP_L" --arg gr "$JGROUP_R" \
  --arg key "a-${RUN_ID}" \
  '{left:{topic:$left,group:$gl,partition:0,offset:0,limit:10},
    right:{topic:$right,group:$gr,partition:0,offset:0,limit:10},
    on:{left_key:"join_id",right_key:"join_id"},
    window:{size_ms:600000,allowed_lateness_ms:0},
    emit:{left_fields:["join_id","score"],right_fields:["join_id","price"],max_rows:5},
    join:{type:"inner"},
    expect_key:$key}')"
join_body="$(printf '%s' "$join_body" | jq 'del(.expect_key)')"
request POST /broker/join "$join_body"
mark_route POST /broker/join
expect_json "POST /broker/join" --arg key "a-${RUN_ID}" '.ok==true and (.rows|length)>=1 and any(.rows[]; .key==$key)'

kv_payload="kv-${RUN_ID}"
request POST "/kv/${NS}/${KEY}?timeout_ms=5000" "$kv_payload" "application/octet-stream"
mark_route POST /kv/:ns/:key+
if expect_json "POST /kv/:namespace/:key" '.ok==true and (.cas|type)=="number"'; then
  KV_CAS="$(printf '%s' "$RESP_BODY" | jq -r '.cas')"
else
  KV_CAS=0
fi

request GET "/kv/${NS}/${KEY}"
mark_route GET /kv/:ns/:key+
if expect_json "GET /kv/:namespace/:key" --argjson cas "$KV_CAS" '.cas==$cas and (.value|type)=="string"'; then
  got="$(printf '%s' "$RESP_BODY" | jq -r '.value' | base64 -d 2>/dev/null || true)"
  if [[ "$got" == "$kv_payload" ]]; then
    pass "GET /kv/:namespace/:key decode" "base64 value matched"
  else
    fail "GET /kv/:namespace/:key decode" "expected ${kv_payload}, got $(snippet "$got")"
  fi
fi

request GET "/kv/${NS}/${KEY}/meta"
mark_route GET /kv/:ns/:key+/meta
expect_json "GET /kv/:namespace/:key/meta" --argjson cas "$KV_CAS" '.cas==$cas and (.ttl_ms|type)=="number"'

request DELETE "/kv/${NS}/${KEY}?cas=${KV_CAS}&timeout_ms=5000"
mark_route DELETE /kv/:ns/:key+
expect_json "DELETE /kv/:namespace/:key" '.ok==true and .deleted==true'

request GET "/kv/${NS}/${KEY}"
expect_json "GET /kv/:namespace/:key after delete" '.error=="not_found"'

verify_route_coverage

printf 'INFO summary                            pass=%d fail=%d skip=%d next_offset=%s\n' "$PASSES" "$FAILS" "$SKIPS" "${NEXT_OFFSET:-unknown}"

if (( FAILS > 0 )); then
  exit 1
fi
