#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE_URL="${BASE_URL:-http://127.0.0.1:1109}"
TOKEN="${TOKEN:?set TOKEN to the benchmark bearer token}"
SUITE_OUTPUT_DIR="${SUITE_OUTPUT_DIR:-${ROOT_DIR}/bench/results/endpoints-$(date -u +%Y%m%dT%H%M%SZ)}"
SUITE_RATE="${SUITE_RATE:-1000}"
topic_prefix="rrv1_endpoints_$(date -u +%Y%m%dT%H%M%S)"
mkdir -p "$SUITE_OUTPUT_DIR"

auth=(-H "Authorization: Bearer ${TOKEN}")
overall=0

run_case() {
  local name="$1"
  local workload="$2"
  local topic="$3"
  local create_topic="$4"
  local contract="$5"
  set +e
  WORKLOAD="$workload" WORKLOAD_NAME="$name" TOPIC="$topic" CREATE_TOPIC="$create_topic" \
    DURABILITY_CONTRACT="$contract" LUA_SCRIPT="${ROOT_DIR}/bench/lua/endpoint_case.lua" \
    RATE="$SUITE_RATE" OUTPUT_DIR="${SUITE_OUTPUT_DIR}/${name}" \
    "${ROOT_DIR}/bench/run_fixed_rate.sh"
  rc=$?
  set -e
  if (( rc != 0 )); then overall=1; fi
}

run_case http_overhead health unused 0 'HTTP 200 and body ok=1'
run_case accepted_produce accepted_produce "${topic_prefix}_accepted" 1 \
  'HTTP 200 and body ok=true,durable=false,synced=null'

read_topic="${topic_prefix}_read"
curl -fsS -X POST "${BASE_URL}/broker/topics" "${auth[@]}" \
  -H 'Content-Type: application/json' \
  --data-binary "{\"name\":\"${read_topic}\",\"partitions\":1}" | jq -e '.ok == true' >/dev/null
curl -fsS -X POST \
  "${BASE_URL}/broker/topics/${read_topic}/produce?partition=0&timeout_ms=5000" \
  "${auth[@]}" -H 'Content-Type: application/json' \
  --data-binary '{"kind":"benchmark_seed"}' | jq -e '
    .ok == true and .durability_scope == "local" and
    .local_fsync == true and .applied_all_workers == true and
    .quorum_committed == false and .failover_safe == false and
    .durable == true and .synced == true
  ' >/dev/null

run_case consume consume "$read_topic" 0 'HTTP 200 and exactly one seeded record returned'
run_case query_inspection_post query "$read_topic" 0 \
  'POST /broker/query returns HTTP 200 with ok=true and a query fingerprint; QUERY method is covered by route smoke'

set +e
WORKLOAD_NAME=durable_produce TOPIC="${topic_prefix}_durable" CREATE_TOPIC=1 \
  DURABILITY_CONTRACT='local fsync receipt; no quorum or failover claim' \
  OUTPUT_DIR="${SUITE_OUTPUT_DIR}/durable_produce" RATE="$SUITE_RATE" \
  "${ROOT_DIR}/bench/run_fixed_rate.sh"
rc=$?
set -e
if (( rc != 0 )); then overall=1; fi

python3 "${ROOT_DIR}/bench/summarize_matrix.py" "$SUITE_OUTPUT_DIR"
printf 'endpoint results: %s\n' "$SUITE_OUTPUT_DIR"
exit "$overall"
