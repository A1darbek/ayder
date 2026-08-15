#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf 'usage: %s DATA_DIR NEW_BACKUP_DIR AYDER_BINARY\n' "$0" >&2
  exit 2
}

[[ $# -eq 3 ]] || usage

DATA_DIR="$(realpath -e -- "$1")"
BACKUP_DIR="$(realpath -m -- "$2")"
AYDER_BINARY="$(realpath -e -- "$3")"

[[ -d "$DATA_DIR" && "$DATA_DIR" != / ]] || {
  printf 'DATA_DIR must be an existing non-root directory\n' >&2
  exit 2
}
[[ -f "$AYDER_BINARY" && -r "$AYDER_BINARY" ]] || {
  printf 'AYDER_BINARY must be a readable regular file\n' >&2
  exit 2
}
[[ ! -e "$BACKUP_DIR" ]] || {
  printf 'backup target already exists: %s\n' "$BACKUP_DIR" >&2
  exit 2
}
case "$BACKUP_DIR/" in
  "$DATA_DIR/"*)
    printf 'backup target must not be inside DATA_DIR\n' >&2
    exit 2
    ;;
esac

for tool in find jq realpath readlink sha256sum sort stat sync tar; do
  command -v "$tool" >/dev/null 2>&1 || {
    printf 'missing required tool: %s\n' "$tool" >&2
    exit 127
  }
done

path_is_data() {
  [[ "$1" == "$DATA_DIR" || "$1" == "$DATA_DIR/"* ]]
}

# A cold backup is invalid while any process has its cwd or an open descriptor
# in the data directory. This check is Linux-specific by design.
for proc_dir in /proc/[0-9]*; do
  pid="${proc_dir##*/}"
  [[ "$pid" == "$$" ]] && continue
  cwd="$(readlink -- "$proc_dir/cwd" 2>/dev/null || true)"
  if [[ -n "$cwd" ]] && path_is_data "$cwd"; then
    printf 'refusing non-cold backup: PID %s cwd is inside DATA_DIR\n' "$pid" >&2
    exit 1
  fi
  for fd_path in "$proc_dir"/fd/*; do
    [[ -e "$fd_path" || -L "$fd_path" ]] || continue
    opened="$(readlink -- "$fd_path" 2>/dev/null || true)"
    opened="${opened% (deleted)}"
    if [[ -n "$opened" ]] && path_is_data "$opened"; then
      printf 'refusing non-cold backup: PID %s has %s open\n' "$pid" "$opened" >&2
      exit 1
    fi
  done
done

# Durable state must not depend on links, devices, sockets, or filenames that
# cannot be represented unambiguously in the checksum manifest.
while IFS= read -r -d '' entry; do
  case "$entry" in
    *$'\n'*)
      printf 'unsupported newline in data path: %q\n' "$entry" >&2
      exit 1
      ;;
  esac
  if [[ ! -f "$entry" && ! -d "$entry" ]]; then
    printf 'unsupported non-regular data entry: %s\n' "$entry" >&2
    exit 1
  fi
done < <(find -P "$DATA_DIR" -mindepth 1 -print0)

umask 077
mkdir -- "$BACKUP_DIR"
: >"$BACKUP_DIR/INCOMPLETE"

(
  cd "$DATA_DIR"
  find . -type f -print0 | LC_ALL=C sort -z |
    xargs -0 -r sha256sum
) >"$BACKUP_DIR/FILES.sha256"

tar --format=pax --sort=name --numeric-owner \
  -C "$DATA_DIR" -cpf "$BACKUP_DIR/ayder-data.tar" .

# Detect a data race even if the process scan missed a writer.
(
  cd "$DATA_DIR"
  find . -type f -print0 | LC_ALL=C sort -z |
    xargs -0 -r sha256sum
) >"$BACKUP_DIR/FILES.after.sha256"
if ! cmp -s "$BACKUP_DIR/FILES.sha256" "$BACKUP_DIR/FILES.after.sha256"; then
  printf 'data changed while the cold backup was being captured\n' >&2
  exit 1
fi
rm -f -- "$BACKUP_DIR/FILES.after.sha256"

archive_sha="$(sha256sum "$BACKUP_DIR/ayder-data.tar" | awk '{print $1}')"
files_sha="$(sha256sum "$BACKUP_DIR/FILES.sha256" | awk '{print $1}')"
binary_sha="$(sha256sum "$AYDER_BINARY" | awk '{print $1}')"
file_count="$(find "$DATA_DIR" -type f -printf . | wc -c)"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
git_sha="$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || printf unknown)"

jq -n \
  --arg created_utc "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg source_data_dir "$DATA_DIR" \
  --arg source_mode "$(stat -c %a "$DATA_DIR")" \
  --arg source_uid "$(stat -c %u "$DATA_DIR")" \
  --arg source_gid "$(stat -c %g "$DATA_DIR")" \
  --arg git_sha "$git_sha" \
  --arg binary_path "$AYDER_BINARY" \
  --arg binary_sha256 "$binary_sha" \
  --arg archive_sha256 "$archive_sha" \
  --arg files_manifest_sha256 "$files_sha" \
  --arg kernel "$(uname -srmo)" \
  --argjson file_count "$file_count" \
  '{schema_version:1,kind:"ayder-cold-backup",created_utc:$created_utc,
    source_data_dir:$source_data_dir,source_mode:$source_mode,
    source_uid:$source_uid,source_gid:$source_gid,git_sha:$git_sha,
    binary_path:$binary_path,binary_sha256:$binary_sha256,
    archive:"ayder-data.tar",archive_sha256:$archive_sha256,
    files_manifest:"FILES.sha256",files_manifest_sha256:$files_manifest_sha256,
    file_count:$file_count,kernel:$kernel,online_snapshot:false}' \
  >"$BACKUP_DIR/MANIFEST.json"

(
  cd "$BACKUP_DIR"
  sha256sum ayder-data.tar FILES.sha256 MANIFEST.json >SHA256SUMS
  sha256sum -c SHA256SUMS
)
rm -f -- "$BACKUP_DIR/INCOMPLETE"
sync -f "$BACKUP_DIR/ayder-data.tar"
sync -f "$BACKUP_DIR/FILES.sha256"
sync -f "$BACKUP_DIR/MANIFEST.json"
sync -f "$BACKUP_DIR/SHA256SUMS"
sync -f "$BACKUP_DIR"

printf 'cold backup complete: %s\n' "$BACKUP_DIR"
