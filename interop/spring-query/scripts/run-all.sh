#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INTEROP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${INTEROP_DIR}/../.." && pwd)"

COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-ayder-spring-query}"
AYDER_DIRECT_BASE="${AYDER_DIRECT_BASE:-http://127.0.0.1:1119}"
AYDER_ENVOY_BASE="${AYDER_ENVOY_BASE:-http://127.0.0.1:18080}"
TOKEN="${TOKEN:-dev}"
RUN_ID="${RUN_ID:-spring_query_$(date -u +%Y%m%dT%H%M%SZ)}"
MATRIX_TOPIC="${MATRIX_TOPIC:-payment-recovery}"
MATRIX_GROUP="${MATRIX_GROUP:-payment-worker}"

compose() {
  docker compose -p "$COMPOSE_PROJECT_NAME" -f "${INTEROP_DIR}/docker-compose.yml" "$@"
}

wait_health() {
  local base="$1"
  local label="$2"
  for _ in $(seq 1 60); do
    if curl -fsS "${base}/health" >/dev/null 2>&1; then
      printf '[%s] %s healthy at %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$label" "$base"
      return 0
    fi
    sleep 1
  done
  echo "${label} did not become healthy at ${base}" >&2
  compose ps >&2 || true
  compose logs --tail=100 >&2 || true
  return 1
}

printf '[%s] starting direct Ayder matrix\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
compose down -v --remove-orphans
compose up -d --build
wait_health "$AYDER_DIRECT_BASE" "ayder-direct"

BASE="$AYDER_DIRECT_BASE" \
TOKEN="$TOKEN" \
TARGET="ayder-direct" \
RUN_ID="${RUN_ID}_direct" \
MATRIX_TOPIC="$MATRIX_TOPIC" \
MATRIX_GROUP="$MATRIX_GROUP" \
"${SCRIPT_DIR}/run-direct.sh"

printf '[%s] restarting clean stack for Envoy matrix\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
compose down -v --remove-orphans
compose up -d --build
wait_health "$AYDER_DIRECT_BASE" "ayder-origin"
wait_health "$AYDER_ENVOY_BASE" "ayder-envoy"

BASE="$AYDER_ENVOY_BASE" \
TOKEN="$TOKEN" \
TARGET="ayder-envoy" \
RUN_ID="${RUN_ID}_envoy" \
MATRIX_TOPIC="$MATRIX_TOPIC" \
MATRIX_GROUP="$MATRIX_GROUP" \
DIRECT_RECEIPT="${INTEROP_DIR}/receipts/ayder-direct/receipt.json" \
"${SCRIPT_DIR}/run-envoy.sh"

printf '\nReceipts written:\n'
printf '  %s\n' "${INTEROP_DIR}/receipts/ayder-direct/receipt.json"
printf '  %s\n' "${INTEROP_DIR}/receipts/ayder-direct/receipt.txt"
printf '  %s\n' "${INTEROP_DIR}/receipts/ayder-envoy/receipt.json"
printf '  %s\n' "${INTEROP_DIR}/receipts/ayder-envoy/receipt.txt"

if [[ "${KEEP_COMPOSE:-1}" != "1" ]]; then
  compose down -v --remove-orphans
fi
