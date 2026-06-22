#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
scenarios=(full_success partial_download hard_failure partial_usable replay_missing_partition)
port=1133

for scenario in "${scenarios[@]}"; do
  echo
  echo "=== ${scenario} ==="
  SCENARIO="$scenario" AYDER_PORT="$port" "${SCRIPT_DIR}/run_demo.sh"
  port=$((port + 1))
done
