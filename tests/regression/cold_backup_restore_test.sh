#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEST_ROOT="$(mktemp -d /tmp/ayder-cold-backup-test.XXXXXX)"

cleanup() {
  rm -rf -- "$TEST_ROOT"
}
trap cleanup EXIT

mkdir -p "$TEST_ROOT/source/nested"
printf 'alpha\n' >"$TEST_ROOT/source/alpha"
printf 'beta\n' >"$TEST_ROOT/source/nested/beta"
chmod 0640 "$TEST_ROOT/source/nested/beta"

"$ROOT_DIR/scripts/operations/cold_backup.sh" \
  "$TEST_ROOT/source" "$TEST_ROOT/backup" "$ROOT_DIR/ayder" >/dev/null
"$ROOT_DIR/scripts/operations/cold_restore.sh" \
  "$TEST_ROOT/backup" "$TEST_ROOT/restored" >"$TEST_ROOT/result.json"

diff -r "$TEST_ROOT/source" "$TEST_ROOT/restored"
[[ "$(stat -c %a "$TEST_ROOT/restored/nested/beta")" == 640 ]]
jq -e '.pass == true and .checksums_verified == true' \
  "$TEST_ROOT/result.json" >/dev/null

printf 'PASS cold backup/restore verifies manifests, contents, and modes\n'
