#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

scenarios=(
  all_confirmed
  fully_missing_event
  partial_missing_notification
  ambiguous_ledger
  duplicate_risk
)

base_port="${AYDER_PORT:-1126}"
idx=0

for scenario in "${scenarios[@]}"; do
  port=$((base_port + idx))
  echo "============================================================"
  echo "Running partial-effect-recovery scenario: ${scenario} on port ${port}"
  echo "============================================================"
  SCENARIO="$scenario" AYDER_PORT="$port" "${SCRIPT_DIR}/run_demo.sh"
  idx=$((idx + 1))
done
