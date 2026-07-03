#!/usr/bin/env bash
set -euo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${DEMO_DIR}/../.." && pwd)"

DOCKER_BIN="${DOCKER_BIN:-docker}"
COMPOSE_FILE="${COMPOSE_FILE:-${DEMO_DIR}/docker-compose.yml}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-ayder_paypal_demo}"
PG_SERVICE="${PG_SERVICE:-postgres}"
REDIS_SERVICE="${REDIS_SERVICE:-redis}"
PG_USER="${PG_USER:-ayder}"
PG_DB="${PG_DB:-paypal_demo}"

AYDER_BASE="${AYDER_BASE:-http://127.0.0.1:1130}"
TOKEN="${TOKEN:-dev}"
AUTH_HEADER="Authorization: Bearer ${TOKEN}"
TOPIC="${TOPIC:-payment_attempts}"
GROUP="${GROUP:-paypal_worker}"
PARTITION="${PARTITION:-0}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing command: $1" >&2
    exit 127
  }
}

log() {
  printf "[%s] %s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

dc() {
  "$DOCKER_BIN" compose -p "$COMPOSE_PROJECT_NAME" -f "$COMPOSE_FILE" "$@"
}

pg_exec_file() {
  dc exec -T "$PG_SERVICE" psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -f - < "$1"
}

pg_exec_sql() {
  dc exec -T "$PG_SERVICE" psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -X -q -c "$1"
}

pg_query_scalar() {
  dc exec -T "$PG_SERVICE" psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -X -A -t -c "$1" \
    | tr -d '\r' | sed '/^$/d' | tail -n 1
}

pg_json() {
  dc exec -T "$PG_SERVICE" psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -X -A -t -c "$1" \
    | tr -d '\r' | sed '/^$/d'
}

redis_cmd() {
  dc exec -T "$REDIS_SERVICE" redis-cli --raw "$@"
}

wait_services() {
  local i
  for ((i = 1; i <= 60; i++)); do
    if dc exec -T "$PG_SERVICE" pg_isready -U "$PG_USER" -d "$PG_DB" >/dev/null 2>&1 \
      && [[ "$(redis_cmd PING 2>/dev/null || true)" == "PONG" ]]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

ensure_topic() {
  curl -fsS -X POST "${AYDER_BASE}/broker/topics" \
    -H "$AUTH_HEADER" -H 'Content-Type: application/json' \
    --data-binary "{\"name\":\"${TOPIC}\",\"partitions\":1}" >/dev/null || true
}
