#!/usr/bin/env bash
set -euo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EOS_DIR="$(cd "${DEMO_DIR}/../../tests/e2e/exactly_once" && pwd)"

export TOPIC="${TOPIC:-ledger_clearance_demo}"
export GROUP="${GROUP:-ledger_clearance_g1}"
export PARTITION="${PARTITION:-0}"
export COMPOSE_FILE="${COMPOSE_FILE:-${EOS_DIR}/docker-compose.pg.yml}"

# shellcheck source=tests/e2e/exactly_once/lib.sh
source "${EOS_DIR}/lib.sh"

SCRIPT_DIR="${DEMO_DIR}"
