#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
scenarios=(happy_path timeout_then_success timeout_unknown duplicate_initiation redis_stale)
port=1130

for scenario in "${scenarios[@]}"; do
  echo
  echo "=== ${scenario} ==="
  SCENARIO="$scenario" AYDER_PORT="$port" "${SCRIPT_DIR}/run_demo.sh"
  port=$((port + 1))
done
