#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

echo "[run_all] pattern 1/3: postgres transactional consumer"
DOCKER_BIN="${DOCKER_BIN:-docker.exe}" \
AYDER_BASE="${AYDER_BASE:-http://127.0.0.1:1239}" \
AYDER_PORT="${AYDER_PORT:-1239}" \
TOTAL_EVENTS="${TOTAL_EVENTS:-300}" \
DUP_EVERY="${DUP_EVERY:-10}" \
ENABLE_PARTITION="${ENABLE_PARTITION:-1}" \
ENABLE_AYDER_KILL="${ENABLE_AYDER_KILL:-1}" \
DURATION_SEC="${DURATION_SEC:-180}" \
PARTITION_EVERY_SEC="${PARTITION_EVERY_SEC:-25}" \
PARTITION_HOLD_SEC="${PARTITION_HOLD_SEC:-5}" \
AYDER_KILL_EVERY_SEC="${AYDER_KILL_EVERY_SEC:-45}" \
bash ./tests/e2e/exactly_once/run_campaign.sh

echo "[run_all] pattern 2/3: webhook outbox"
AYDER_BASE="http://127.0.0.1:1249" \
AYDER_PORT="1249" \
SINK_PORT="18192" \
TOTAL_EVENTS="${TOTAL_EVENTS:-300}" \
DUP_EVERY="${DUP_EVERY:-10}" \
ENABLE_PARTITION="${ENABLE_PARTITION:-1}" \
ENABLE_AYDER_KILL="${ENABLE_AYDER_KILL:-1}" \
DURATION_SEC="${DURATION_SEC:-90}" \
PARTITION_EVERY_SEC="${PARTITION_EVERY_SEC:-3}" \
PARTITION_HOLD_SEC="${PARTITION_HOLD_SEC:-1}" \
AYDER_KILL_EVERY_SEC="${AYDER_KILL_EVERY_SEC:-6}" \
bash ./tests/e2e/patterns/webhook_outbox/run_campaign.sh

echo "[run_all] pattern 3/3: idempotent rest sink"
AYDER_BASE="http://127.0.0.1:1259" \
AYDER_PORT="1259" \
SINK_PORT="18193" \
TOTAL_EVENTS="${TOTAL_EVENTS:-300}" \
DUP_EVERY="${DUP_EVERY:-10}" \
ENABLE_PARTITION="${ENABLE_PARTITION:-1}" \
ENABLE_AYDER_KILL="${ENABLE_AYDER_KILL:-1}" \
DURATION_SEC="${DURATION_SEC:-90}" \
PARTITION_EVERY_SEC="${PARTITION_EVERY_SEC:-3}" \
PARTITION_HOLD_SEC="${PARTITION_HOLD_SEC:-1}" \
AYDER_KILL_EVERY_SEC="${AYDER_KILL_EVERY_SEC:-6}" \
bash ./tests/e2e/patterns/idempotent_rest_sink/run_campaign.sh

echo "[run_all] done"
