#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

COMPOSE_FILE="${COMPOSE_FILE:-${SCRIPT_DIR}/docker-compose.pg.yml}"
DOCKER_BIN="${DOCKER_BIN:-docker}"
PG_SERVICE="${PG_SERVICE:-postgres}"
PG_USER="${PG_USER:-ayder}"
PG_DB="${PG_DB:-ayder_e2e}"

AYDER_BASE="${AYDER_BASE:-http://127.0.0.1:1109}"
TOKEN="${TOKEN:-dev}"
AUTH_HEADER="Authorization: Bearer ${TOKEN}"

TOPIC="${TOPIC:-e2e_exactly_once}"
GROUP="${GROUP:-e2e_g1}"
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
  "$DOCKER_BIN" compose -f "$COMPOSE_FILE" "$@"
}

pg_exec_file() {
  local path="$1"
  dc exec -T "$PG_SERVICE" psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -f - < "$path"
}

pg_exec_sql() {
  local sql="$1"
  dc exec -T "$PG_SERVICE" psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -X -q -c "$sql"
}

pg_query_scalar() {
  local sql="$1"
  dc exec -T "$PG_SERVICE" psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -X -A -t -c "$sql" | tr -d '\r' | sed '/^$/d' | tail -n 1
}

wait_pg_ready() {
  local retries="${1:-60}"
  local i
  for ((i = 1; i <= retries; i++)); do
    if dc exec -T "$PG_SERVICE" pg_isready -U "$PG_USER" -d "$PG_DB" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

ensure_topic() {
  local payload
  payload="$(printf '{"name":"%s","partitions":1}' "$TOPIC")"
  curl -fsS -X POST "${AYDER_BASE}/broker/topics" \
    -H "$AUTH_HEADER" -H 'Content-Type: application/json' \
    --data-binary "$payload" >/dev/null || true
}

curl_json() {
  local method="$1"
  local url="$2"
  local body="${3:-}"
  local content_type="${4:-application/json}"
  if [[ -n "$body" ]]; then
    curl -fsS -X "$method" "$url" -H "$AUTH_HEADER" -H "Content-Type: ${content_type}" --data-binary "$body"
  else
    curl -fsS -X "$method" "$url" -H "$AUTH_HEADER"
  fi
}
