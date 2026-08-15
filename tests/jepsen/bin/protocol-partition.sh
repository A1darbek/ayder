#!/usr/bin/env bash
set -euo pipefail

CONTROL_FILE="${AYDER_HA_TEST_PARTITION_FILE:?set AYDER_HA_TEST_PARTITION_FILE to the RF_HA_TEST_PARTITION_FILE used by every node}"
NODES_CSV="${AYDER_JEPSEN_NODES:-http://127.0.0.1:7001,http://127.0.0.1:8001,http://127.0.0.1:9001,http://127.0.0.1:10001,http://127.0.0.1:11001,http://127.0.0.1:12001}"

write_mask() {
  local mask="$1"
  local control_dir tmp
  control_dir="$(dirname -- "$CONTROL_FILE")"
  [[ -d "$control_dir" ]] || {
    printf 'control directory does not exist: %s\n' "$control_dir" >&2
    return 1
  }
  tmp="${CONTROL_FILE}.tmp.$$"
  umask 077
  printf 'cut_mask=%s\n' "$mask" > "$tmp"
  mv -f -- "$tmp" "$CONTROL_FILE"
}

leader_index() {
  local idx=0 url metrics role leaders=0 found=-1
  IFS=',' read -r -a urls <<< "$NODES_CSV"
  for url in "${urls[@]}"; do
    url="${url%/}"
    if [[ -n "${AYDER_JEPSEN_TOKEN:-}" ]]; then
      metrics="$(curl -fsS --connect-timeout 1 --max-time 2 \
        -H "Authorization: Bearer ${AYDER_JEPSEN_TOKEN}" \
        "$url/metrics_ha" 2>/dev/null || true)"
    else
      metrics="$(curl -fsS --connect-timeout 1 --max-time 2 \
        "$url/metrics_ha" 2>/dev/null || true)"
    fi
    role="$(awk '/^ramforge_ha_role /{print $2; exit}' <<< "$metrics")"
    if [[ "$role" == '0' ]]; then
      found="$idx"
      leaders=$((leaders + 1))
    fi
    idx=$((idx + 1))
  done
  if (( leaders != 1 )); then
    printf 'expected exactly one reachable leader, found %d\n' "$leaders" >&2
    return 1
  fi
  printf '%d\n' "$found"
}

case "${1:-}" in
  isolate-leader)
    idx="$(leader_index)"
    (( idx >= 0 && idx < 7 )) || {
      printf 'leader membership index out of range: %s\n' "$idx" >&2
      exit 1
    }
    mask=$((1 << idx))
    write_mask "0x$(printf '%x' "$mask")"
    printf 'protocol partition applied: leader_index=%d cut_mask=0x%x\n' "$idx" "$mask"
    ;;
  isolate-index)
    idx="${2:-}"
    [[ "$idx" =~ ^[0-6]$ ]] || {
      printf 'usage: %s isolate-index INDEX(0..6)\n' "$0" >&2
      exit 2
    }
    mask=$((1 << idx))
    write_mask "0x$(printf '%x' "$mask")"
    printf 'protocol partition applied: node_index=%d cut_mask=0x%x\n' "$idx" "$mask"
    ;;
  heal)
    write_mask '0x0'
    printf 'protocol partition healed\n'
    ;;
  status)
    if [[ -f "$CONTROL_FILE" ]]; then
      sed -n '1p' "$CONTROL_FILE"
    else
      printf 'cut_mask=0x0\n'
    fi
    ;;
  *)
    printf 'usage: %s {isolate-leader|isolate-index INDEX|heal|status}\n' "$0" >&2
    exit 2
    ;;
esac
