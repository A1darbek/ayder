#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_NAME="${PROJECT_NAME:-ayder-vs-kafka-nats}"
BENCH_AYDER_TOKEN="${BENCH_AYDER_TOKEN:-unused-for-shutdown}"
BENCH_RF_BEARER_TOKENS="${BENCH_RF_BEARER_TOKENS:-unused-for-shutdown@1:1:1}"
export BENCH_AYDER_TOKEN BENCH_RF_BEARER_TOKENS

if docker compose version >/dev/null 2>&1; then
  docker compose -p "$PROJECT_NAME" -f "$ROOT/docker-compose.yml" down -v --remove-orphans
elif command -v docker-compose >/dev/null 2>&1; then
  docker-compose -p "$PROJECT_NAME" -f "$ROOT/docker-compose.yml" down -v --remove-orphans
else
  echo "error: Docker Compose is not available" >&2
  exit 1
fi
