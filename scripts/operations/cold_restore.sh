#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf 'usage: %s BACKUP_DIR NEW_TARGET_DIR\n' "$0" >&2
  exit 2
}

[[ $# -eq 2 ]] || usage

BACKUP_DIR="$(realpath -e -- "$1")"
TARGET_DIR="$(realpath -m -- "$2")"
TARGET_PARENT="$(dirname "$TARGET_DIR")"

[[ -d "$BACKUP_DIR" ]] || {
  printf 'BACKUP_DIR must be an existing directory\n' >&2
  exit 2
}
[[ -d "$TARGET_PARENT" ]] || {
  printf 'target parent must already exist: %s\n' "$TARGET_PARENT" >&2
  exit 2
}
[[ ! -e "$TARGET_DIR" ]] || {
  printf 'restore target must not exist: %s\n' "$TARGET_DIR" >&2
  exit 2
}
case "$TARGET_DIR/" in
  "$BACKUP_DIR/"*)
    printf 'restore target must not be inside BACKUP_DIR\n' >&2
    exit 2
    ;;
esac

for tool in find jq realpath sha256sum sync tar; do
  command -v "$tool" >/dev/null 2>&1 || {
    printf 'missing required tool: %s\n' "$tool" >&2
    exit 127
  }
done

for required in MANIFEST.json SHA256SUMS FILES.sha256 ayder-data.tar; do
  [[ -f "$BACKUP_DIR/$required" ]] || {
    printf 'backup is missing %s\n' "$required" >&2
    exit 1
  }
done
[[ ! -e "$BACKUP_DIR/INCOMPLETE" ]] || {
  printf 'backup is marked incomplete\n' >&2
  exit 1
}

if ! (cd "$BACKUP_DIR" && sha256sum -c --status SHA256SUMS); then
  printf 'backup manifest checksum verification failed\n' >&2
  exit 1
fi
jq -e '
  .schema_version == 1 and .kind == "ayder-cold-backup" and
  .archive == "ayder-data.tar" and .files_manifest == "FILES.sha256" and
  .online_snapshot == false
' "$BACKUP_DIR/MANIFEST.json" >/dev/null

expected_archive_sha="$(jq -r '.archive_sha256' "$BACKUP_DIR/MANIFEST.json")"
actual_archive_sha="$(sha256sum "$BACKUP_DIR/ayder-data.tar" | awk '{print $1}')"
[[ "$actual_archive_sha" == "$expected_archive_sha" ]] || {
  printf 'archive digest disagrees with MANIFEST.json\n' >&2
  exit 1
}

while IFS= read -r member; do
  [[ -n "$member" ]] || continue
  case "$member" in
    .|./|./*) ;;
    *)
      printf 'unsafe archive member: %q\n' "$member" >&2
      exit 1
      ;;
  esac
  case "/$member/" in
    */../*)
      printf 'archive member escapes target: %q\n' "$member" >&2
      exit 1
      ;;
  esac
done < <(tar -tf "$BACKUP_DIR/ayder-data.tar")

umask 077
STAGING_DIR="$(mktemp -d "$TARGET_PARENT/.ayder-restore.XXXXXX")"
cleanup() {
  if [[ -n "${STAGING_DIR:-}" && -d "$STAGING_DIR" ]]; then
    rm -rf -- "$STAGING_DIR"
  fi
}
trap cleanup EXIT

tar --extract --file "$BACKUP_DIR/ayder-data.tar" \
  --directory "$STAGING_DIR" --same-permissions --no-same-owner

special="$(find -P "$STAGING_DIR" -mindepth 1 ! -type f ! -type d -print -quit)"
[[ -z "$special" ]] || {
  printf 'restored archive contains unsupported entry: %s\n' "$special" >&2
  exit 1
}

if ! (cd "$STAGING_DIR" &&
      sha256sum -c --status "$BACKUP_DIR/FILES.sha256"); then
  printf 'restored file checksum verification failed\n' >&2
  exit 1
fi

while IFS= read -r -d '' restored_file; do
  sync -f "$restored_file"
done < <(find "$STAGING_DIR" -type f -print0)
sync -f "$STAGING_DIR"
mv -- "$STAGING_DIR" "$TARGET_DIR"
STAGING_DIR=""
sync -f "$TARGET_PARENT"

jq -n \
  --arg restored_utc "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg backup_dir "$BACKUP_DIR" \
  --arg target_dir "$TARGET_DIR" \
  --arg archive_sha256 "$actual_archive_sha" \
  '{schema_version:1,kind:"ayder-cold-restore-result",restored_utc:$restored_utc,
    backup_dir:$backup_dir,target_dir:$target_dir,
    archive_sha256:$archive_sha256,checksums_verified:true,pass:true}'
