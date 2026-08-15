#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT/docker-compose.yml"
PROJECT_NAME="${PROJECT_NAME:-ayder-vs-kafka-nats}"
ARTIFACTS_DIR="${ARTIFACTS_DIR:-$ROOT/artifacts}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
SERVICES="${SERVICES:-ayder,kafka,nats}"
MESSAGES="${MESSAGES:-1000}"
CONCURRENCY="${CONCURRENCY:-1}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-256}"
TIMEOUT_MS="${TIMEOUT_MS:-5000}"
RESET="${RESET:-1}"

die() {
  echo "error: $*" >&2
  exit 1
}

command -v docker >/dev/null 2>&1 || die "Docker is not installed or not on PATH"
command -v openssl >/dev/null 2>&1 || die "OpenSSL is not installed or not on PATH"
docker info >/dev/null 2>&1 || die "Docker daemon is not reachable"
if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE")
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE")
else
  die "Docker Compose is not available; install docker compose v2 or docker-compose"
fi

BENCH_AYDER_TOKEN="${BENCH_AYDER_TOKEN:-$(openssl rand -hex 24)}"
BENCH_RF_BEARER_TOKENS="${BENCH_RF_BEARER_TOKENS:-${BENCH_AYDER_TOKEN}@55555555555555:11111111111111111:111111111111111111111}"
export BENCH_AYDER_TOKEN BENCH_RF_BEARER_TOKENS

mkdir -p "$ARTIFACTS_DIR/$RUN_ID"

echo "run_id=$RUN_ID"
echo "services=$SERVICES messages=$MESSAGES concurrency=$CONCURRENCY payload_bytes=$PAYLOAD_BYTES timeout_ms=$TIMEOUT_MS"
echo "artifacts=$ARTIFACTS_DIR/$RUN_ID"

if [ "$RESET" = "1" ]; then
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
fi

RF_RESET_DATA="${RF_RESET_DATA:-1}" "${COMPOSE[@]}" up -d --build ayder kafka nats
"${COMPOSE[@]}" build bench
"${COMPOSE[@]}" run --rm bench wait

"${COMPOSE[@]}" run --rm \
  -e "RUN_ID=$RUN_ID" \
  -e "SERVICES=$SERVICES" \
  -e "MESSAGES=$MESSAGES" \
  -e "CONCURRENCY=$CONCURRENCY" \
  -e "PAYLOAD_BYTES=$PAYLOAD_BYTES" \
  -e "TIMEOUT_MS=$TIMEOUT_MS" \
  bench run

docker stats --no-stream --format '{{json .}}' \
  "$PROJECT_NAME-ayder-1" "$PROJECT_NAME-kafka-1" "$PROJECT_NAME-nats-1" \
  > "$ARTIFACTS_DIR/$RUN_ID/docker-stats.jsonl" 2>/dev/null || true

"${COMPOSE[@]}" logs --no-color ayder kafka nats > "$ARTIFACTS_DIR/$RUN_ID/docker.log" 2>&1 || true

echo "wrote $ARTIFACTS_DIR/$RUN_ID"
