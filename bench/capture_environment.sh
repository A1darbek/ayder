#!/usr/bin/env bash
set -euo pipefail

section() {
  printf '\n[%s]\n' "$1"
}

section timestamp_utc
date -u +%Y-%m-%dT%H:%M:%SZ

section git
git rev-parse HEAD
git status --short --branch

section kernel
uname -a
uname -m
cat /proc/version

section cpu
cat /proc/cpuinfo
nproc

section memory
free -h

section storage
lsblk -o NAME,TYPE,SIZE,ROTA,MODEL,FSTYPE,MOUNTPOINTS
df -T
mount

section compiler
gcc --version
if command -v clang >/dev/null 2>&1; then
  clang --version
else
  printf 'clang: not installed\n'
fi
ldd --version

section io_uring
if [[ -r /proc/sys/kernel/io_uring_disabled ]]; then
  printf 'kernel.io_uring_disabled='
  cat /proc/sys/kernel/io_uring_disabled
else
  printf 'kernel.io_uring_disabled: unavailable\n'
fi
ldconfig -p 2>/dev/null | grep liburing || true

section perf
if command -v perf >/dev/null 2>&1; then
  perf --version
  if [[ -r /proc/sys/kernel/perf_event_paranoid ]]; then
    printf 'kernel.perf_event_paranoid='
    cat /proc/sys/kernel/perf_event_paranoid
  fi
  perf stat -e task-clock true || true
else
  printf 'perf: not installed\n'
fi

section relevant_environment
while IFS='=' read -r name value; do
  case "$name" in
    RF_*|AYDER_*|WSL*|CC|CFLAGS|LDFLAGS|MAKEFLAGS)
      normalized_name="${name^^}"
      if [[ "$normalized_name" =~ (TOKEN|SECRET|PASSWORD|PASS|KEY|CERT|CREDENTIAL|AUTH) ]]; then
        printf '%s=<redacted>\n' "$name"
      else
        printf '%s=%s\n' "$name" "$value"
      fi
      ;;
  esac
done < <(env | LC_ALL=C sort)
