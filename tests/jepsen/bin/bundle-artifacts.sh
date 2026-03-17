#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
RESULTS_ROOT="${AYDER_JEPSEN_RESULTS_ROOT:-$ROOT_DIR/tests/jepsen/results}"
ARTIFACTS_ROOT="${AYDER_JEPSEN_ARTIFACTS_ROOT:-$ROOT_DIR/tests/jepsen/artifacts}"

SRC_DIR="${1:-}"
OUT_FILE="${2:-}"

if [[ -z "$SRC_DIR" ]]; then
  latest="$(ls -1dt "$RESULTS_ROOT"/campaign_* "$RESULTS_ROOT"/matrix_* 2>/dev/null | head -n 1 || true)"
  SRC_DIR="$latest"
fi

if [[ -z "$SRC_DIR" || ! -d "$SRC_DIR" ]]; then
  echo "No source result directory found to bundle." >&2
  exit 1
fi

if [[ -z "$OUT_FILE" ]]; then
  mkdir -p "$ARTIFACTS_ROOT"
  base="$(basename "$SRC_DIR")"
  OUT_FILE="$ARTIFACTS_ROOT/${base}.tar.gz"
fi

mkdir -p "$(dirname "$OUT_FILE")"

src_parent="$(dirname "$SRC_DIR")"
src_base="$(basename "$SRC_DIR")"

tar -C "$src_parent" -czf "$OUT_FILE" "$src_base"

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$OUT_FILE" > "$OUT_FILE.sha256"
else
  shasum -a 256 "$OUT_FILE" > "$OUT_FILE.sha256"
fi

{
  echo "source_dir=$SRC_DIR"
  echo "bundle_file=$OUT_FILE"
  echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "$OUT_FILE.manifest"

echo "bundle_file=$OUT_FILE"
echo "sha256_file=$OUT_FILE.sha256"
