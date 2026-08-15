#!/usr/bin/env bash
set -euo pipefail

TARGET="${TARGET:-all}"
MESSAGES="${MESSAGES:-1000}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-128}"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-artifacts}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
PROJECT="${COMPOSE_PROJECT_NAME:-ayder-smallbench}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing dependency: $1" >&2
    exit 1
  }
}

need docker
need openssl

BENCH_AYDER_TOKEN="${BENCH_AYDER_TOKEN:-$(openssl rand -hex 24)}"
BENCH_RF_BEARER_TOKENS="${BENCH_RF_BEARER_TOKENS:-${BENCH_AYDER_TOKEN}@55555555555555:11111111111111111:111111111111111111111}"
export BENCH_AYDER_TOKEN BENCH_RF_BEARER_TOKENS

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
else
  echo "docker compose v2 is required" >&2
  exit 1
fi

case "$TARGET" in
  all|ayder|kafka|nats) ;;
  *)
    echo "TARGET must be one of: all, ayder, kafka, nats" >&2
    exit 2
    ;;
esac

mkdir -p "${ARTIFACT_ROOT}/${RUN_ID}"

targets=("$TARGET")
if [ "$TARGET" = "all" ]; then
  targets=(ayder kafka nats)
fi

service_for_target() {
  case "$1" in
    ayder) echo ayder ;;
    kafka) echo kafka ;;
    nats) echo nats ;;
  esac
}

echo "run_id=${RUN_ID}"
echo "messages=${MESSAGES}"
echo "payload_bytes=${PAYLOAD_BYTES}"
echo "artifacts=${ARTIFACT_ROOT}/${RUN_ID}"

"${COMPOSE[@]}" -p "$PROJECT" build bench-client

for t in "${targets[@]}"; do
  svc="$(service_for_target "$t")"
  echo
  echo "==> $t: starting $svc"
  "${COMPOSE[@]}" -p "$PROJECT" down -v --remove-orphans >/dev/null 2>&1 || true
  "${COMPOSE[@]}" -p "$PROJECT" up -d --build "$svc"

  echo "==> $t: running synchronous durable benchmark"
  "${COMPOSE[@]}" -p "$PROJECT" run --rm \
    -e TARGET="$t" \
    -e MESSAGES="$MESSAGES" \
    -e PAYLOAD_BYTES="$PAYLOAD_BYTES" \
    -e ARTIFACT_DIR="/bench/${ARTIFACT_ROOT}/${RUN_ID}" \
    bench-client \
    python /bench/runner.py \
      --target "$t" \
      --messages "$MESSAGES" \
      --payload-bytes "$PAYLOAD_BYTES" \
      --artifact-dir "/bench/${ARTIFACT_ROOT}/${RUN_ID}"
done

echo
echo "==> done"
echo "summary: ${ARTIFACT_ROOT}/${RUN_ID}/summary.csv"
