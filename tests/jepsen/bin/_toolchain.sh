#!/usr/bin/env bash

jepsen_source_local_toolchain() {
  local root_dir="$1"
  local env_file="${AYDER_JEPSEN_TOOLCHAIN_ENV:-$root_dir/tests/jepsen/.toolchain/env.sh}"
  if [[ -f "$env_file" ]]; then
    # shellcheck disable=SC1090
    source "$env_file"
  fi
}

jepsen_require_toolchain() {
  local root_dir="$1"
  jepsen_source_local_toolchain "$root_dir"

  local missing=()
  command -v java >/dev/null 2>&1 || missing+=(java)
  command -v lein >/dev/null 2>&1 || missing+=(lein)

  if [[ "${#missing[@]}" -gt 0 ]]; then
    printf 'missing: %s\n' "${missing[*]}" >&2
    echo "Install system packages, or run: $root_dir/tests/jepsen/bin/bootstrap-toolchain.sh" >&2
    return 1
  fi

  return 0
}
