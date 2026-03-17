#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT_DIR/tests/jepsen/bin/_toolchain.sh"
jepsen_source_local_toolchain "$ROOT_DIR"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing: $1" >&2
    return 1
  fi
  return 0
}

missing_any=0

need java || missing_any=1
need lein || missing_any=1
need curl || missing_any=1
need jq || missing_any=1
need tar || missing_any=1

if ! command -v sha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
  echo "missing: sha256sum (or shasum)" >&2
  missing_any=1
fi

if [[ "$missing_any" -ne 0 ]]; then
  echo "Hint: run $ROOT_DIR/tests/jepsen/bin/bootstrap-toolchain.sh for local java+lein" >&2
  exit 1
fi

echo "OK: java, lein, curl, jq, tar, sha256 tooling are installed"
