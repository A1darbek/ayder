#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf 'Usage: bash %s <built-artifact> <output-directory-outside-repository>\n' "$0" >&2
}

if [ "$#" -ne 2 ]; then
  usage
  exit 2
fi

for tool in git jq realpath sha256sum syft; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    printf 'Required tool not found: %s\n' "$tool" >&2
    exit 127
  fi
done

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(git -C "$script_dir/../.." rev-parse --show-toplevel)
artifact=$(realpath -- "$1")
output_dir=$(realpath -m -- "$2")

if [ ! -f "$artifact" ]; then
  printf 'Built artifact is not a regular file: %s\n' "$artifact" >&2
  exit 2
fi

case "$output_dir/" in
  "$repo_root/"*)
    printf 'Output directory must be outside the repository: %s\n' "$output_dir" >&2
    exit 2
    ;;
esac

umask 077
mkdir -p -- "$output_dir"

commit=$(git -C "$repo_root" rev-parse HEAD)
short_commit=$(git -C "$repo_root" rev-parse --short=12 HEAD)
sbom="$output_dir/ayder-source-$short_commit.spdx.json"
metadata="$output_dir/evidence-$short_commit.txt"
status_file="$output_dir/git-status-$short_commit.txt"
syft_version_file="$output_dir/syft-version-$short_commit.txt"
checksums="$output_dir/checksums-$short_commit.sha256"

for target in "$sbom" "$metadata" "$status_file" "$syft_version_file" "$checksums"; do
  if [ -e "$target" ]; then
    printf 'Refusing to overwrite existing evidence: %s\n' "$target" >&2
    exit 1
  fi
done

git -C "$repo_root" status --porcelain=v1 --untracked-files=all >"$status_file"
if [ -s "$status_file" ]; then
  tree_state=dirty
else
  tree_state=clean
fi

syft version >"$syft_version_file"
syft scan "$repo_root" \
  --source-name ayder-source \
  --source-version "$commit" \
  -o "spdx-json=$sbom"

jq --exit-status '
  (.spdxVersion | type == "string" and startswith("SPDX-")) and
  (.packages | type == "array")
' "$sbom" >/dev/null

generated_at=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
{
  printf 'generated_at_utc=%s\n' "$generated_at"
  printf 'git_commit=%s\n' "$commit"
  printf 'git_tree_state=%s\n' "$tree_state"
  printf 'artifact=%s\n' "$artifact"
  printf 'architecture=%s\n' "$(uname -m)"
  printf 'sbom_scope=source-tree-package-catalog\n'
  printf 'signature=none\n'
  printf 'provenance_attestation=none\n'
} >"$metadata"

sha256sum "$artifact" "$sbom" "$metadata" "$status_file" "$syft_version_file" >"$checksums"
sha256sum --check "$checksums" >/dev/null

printf 'Source SBOM: %s\n' "$sbom"
printf 'Checksums:  %s\n' "$checksums"
printf 'Metadata:   %s (%s tree)\n' "$metadata" "$tree_state"
printf 'These files are unsigned local evidence, not provenance or certification.\n'
