#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST="${WRK2_BUILD_DIR:-${ROOT_DIR}/bench/.tools/wrk2-aarch64}"
REPOSITORY="https://github.com/AmpereTravis/wrk2-aarch64.git"
COMMIT="59c09ad4442329ade4343e1ade683c4d12b0b370"

if [[ -e "$DEST" ]]; then
  printf 'destination already exists; choose a fresh WRK2_BUILD_DIR: %s\n' "$DEST" >&2
  exit 2
fi

mkdir -p "$(dirname "$DEST")"
git clone --filter=blob:none "$REPOSITORY" "$DEST"
git -C "$DEST" checkout --detach "$COMMIT"
git -C "$DEST" submodule update --init --recursive
git -C "$DEST" apply "${ROOT_DIR}/bench/wrk2-aarch64.patch"
make -C "$DEST" -j"$(nproc)"

wrapper="${DEST}/wrk2"
printf '%s\n' \
  '#!/usr/bin/env bash' \
  'set -euo pipefail' \
  'tool_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"' \
  'export WRK2_LUA_MODULE="${tool_dir}/src/wrk.lua"' \
  'exec "${tool_dir}/wrk" "$@"' >"$wrapper"
chmod +x "$wrapper"

printf 'wrk2 wrapper: %s\n' "$wrapper"
printf 'source commit: %s\n' "$COMMIT"
sha256sum "$DEST/wrk" "${ROOT_DIR}/bench/wrk2-aarch64.patch"
