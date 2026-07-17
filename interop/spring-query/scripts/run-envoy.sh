#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TARGET="${TARGET:-ayder-envoy}" \
BASE="${BASE:-http://127.0.0.1:18080}" \
"${SCRIPT_DIR}/run-direct.sh"
