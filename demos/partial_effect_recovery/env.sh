#!/usr/bin/env bash
set -euo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EOS_DIR="$(cd "${DEMO_DIR}/../../tests/e2e/exactly_once" && pwd)"

export TOPIC="${TOPIC:-order_events}"
export GROUP="${GROUP:-order_worker}"
export PARTITION="${PARTITION:-0}"
export COMPOSE_FILE="${COMPOSE_FILE:-${DEMO_DIR}/docker-compose.yml}"

# shellcheck source=tests/e2e/exactly_once/lib.sh
source "${EOS_DIR}/lib.sh"

SCRIPT_DIR="${DEMO_DIR}"
