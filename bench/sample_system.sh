#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  printf 'usage: sample_system.sh "PID [PID ...]"\n' >&2
  exit 2
fi

pids="${1//,/ }"
for pid in $pids; do
  if ! [[ "$pid" =~ ^[1-9][0-9]*$ ]] || [[ ! -r "/proc/$pid/stat" ]]; then
    printf 'invalid or unavailable server PID: %s\n' "$pid" >&2
    exit 2
  fi
done

printf 'epoch_ms\tcpu_ticks\trss_kb\tread_bytes\twrite_bytes\tthreads\n'
while :; do
  cpu_ticks=0
  rss_kb=0
  read_bytes=0
  write_bytes=0
  threads=0
  live=0
  for pid in $pids; do
    [[ -r "/proc/$pid/stat" ]] || continue
    live=1
    ticks="$(awk '{print $14 + $15}' "/proc/$pid/stat" 2>/dev/null || printf '0')"
    rss="$(awk '/^VmRSS:/ {print $2}' "/proc/$pid/status" 2>/dev/null || printf '0')"
    nthr="$(awk '/^Threads:/ {print $2}' "/proc/$pid/status" 2>/dev/null || printf '0')"
    rbytes="$(awk '/^read_bytes:/ {print $2}' "/proc/$pid/io" 2>/dev/null || printf '0')"
    wbytes="$(awk '/^write_bytes:/ {print $2}' "/proc/$pid/io" 2>/dev/null || printf '0')"
    cpu_ticks=$((cpu_ticks + ${ticks:-0}))
    rss_kb=$((rss_kb + ${rss:-0}))
    read_bytes=$((read_bytes + ${rbytes:-0}))
    write_bytes=$((write_bytes + ${wbytes:-0}))
    threads=$((threads + ${nthr:-0}))
  done
  (( live == 1 )) || exit 0
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$(date +%s%3N)" "$cpu_ticks" "$rss_kb" "$read_bytes" "$write_bytes" "$threads"
  sleep 1
done
