#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PAYLOADS="${PAYLOADS:-128 1024 16384}"
RATES="${RATES:-10000 25000 50000}"
MATRIX_OUTPUT_DIR="${MATRIX_OUTPUT_DIR:-${ROOT_DIR}/bench/results/matrix-$(date -u +%Y%m%dT%H%M%SZ)}"
TOPIC_PREFIX="${TOPIC_PREFIX:-rrv1_matrix_$(date -u +%Y%m%dT%H%M%S)}"

mkdir -p "$MATRIX_OUTPUT_DIR"
overall=0
for payload in $PAYLOADS; do
  for rate in $RATES; do
    case_dir="${MATRIX_OUTPUT_DIR}/${payload}B-${rate}rps"
    printf 'running payload=%s offered_rps=%s\n' "$payload" "$rate"
    set +e
    OUTPUT_DIR="$case_dir" TOPIC="${TOPIC_PREFIX}_${payload}_${rate}" \
      PAYLOAD_BYTES="$payload" RATE="$rate" \
      "${ROOT_DIR}/bench/run_fixed_rate.sh"
    rc=$?
    set -e
    printf '%s\n' "$rc" >"${case_dir}/exit_status"
    if (( rc != 0 )); then overall=1; fi
  done
done

python3 "${ROOT_DIR}/bench/summarize_matrix.py" "$MATRIX_OUTPUT_DIR"
printf 'matrix results: %s\n' "$MATRIX_OUTPUT_DIR"
exit "$overall"
