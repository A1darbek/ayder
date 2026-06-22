#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/e2e/exactly_once/lib.sh
source "${SCRIPT_DIR}/lib.sh"

ENABLE_PARTITION="${ENABLE_PARTITION:-0}"
ENABLE_AYDER_KILL="${ENABLE_AYDER_KILL:-0}"
DURATION_SEC="${DURATION_SEC:-90}"
PARTITION_EVERY_SEC="${PARTITION_EVERY_SEC:-20}"
PARTITION_HOLD_SEC="${PARTITION_HOLD_SEC:-6}"
AYDER_KILL_EVERY_SEC="${AYDER_KILL_EVERY_SEC:-25}"
AYDER_PID_FILE="${AYDER_PID_FILE:-}"
AYDER_PORT="${AYDER_PORT:-1109}"
SUDO_BIN="${SUDO_BIN:-sudo}"
IPTABLES_BIN="${IPTABLES_BIN:-}"
CHAIN="AYDER_E2E_EOS"

resolve_iptables_bin() {
  if [[ -n "$IPTABLES_BIN" ]]; then
    return 0
  fi
  IPTABLES_BIN="$(command -v iptables || true)"
  if [[ -z "$IPTABLES_BIN" ]]; then
    local candidate="${SCRIPT_DIR}/../../jepsen/.toolchain/bin/iptables"
    if [[ -x "$candidate" ]]; then
      IPTABLES_BIN="$candidate"
    fi
  fi
}

iptables_add_drop() {
  "$SUDO_BIN" -n "$IPTABLES_BIN" -w -N "$CHAIN" 2>/dev/null || true
  "$SUDO_BIN" -n "$IPTABLES_BIN" -w -C OUTPUT -j "$CHAIN" 2>/dev/null || "$SUDO_BIN" -n "$IPTABLES_BIN" -w -I OUTPUT 1 -j "$CHAIN"
  "$SUDO_BIN" -n "$IPTABLES_BIN" -w -A "$CHAIN" -p tcp --dport "$AYDER_PORT" -j DROP
}

iptables_clear() {
  "$SUDO_BIN" -n "$IPTABLES_BIN" -w -D OUTPUT -j "$CHAIN" 2>/dev/null || true
  "$SUDO_BIN" -n "$IPTABLES_BIN" -w -F "$CHAIN" 2>/dev/null || true
  "$SUDO_BIN" -n "$IPTABLES_BIN" -w -X "$CHAIN" 2>/dev/null || true
}

cleanup() {
  if [[ "$ENABLE_PARTITION" == "1" ]]; then
    iptables_clear || true
  fi
}
trap cleanup EXIT

if [[ "$ENABLE_PARTITION" == "1" ]]; then
  resolve_iptables_bin
  if ! "$SUDO_BIN" -n true >/dev/null 2>&1; then
    log "fault injector: sudo -n unavailable; disabling partition injection"
    ENABLE_PARTITION=0
  elif [[ -z "$IPTABLES_BIN" ]]; then
    log "fault injector: iptables not found; disabling partition injection"
    ENABLE_PARTITION=0
  fi
fi

start_ts="$(date +%s)"
next_partition="${PARTITION_EVERY_SEC}"
next_kill="${AYDER_KILL_EVERY_SEC}"

log "fault injector start duration=${DURATION_SEC}s partition=${ENABLE_PARTITION} ayder_kill=${ENABLE_AYDER_KILL}"

while true; do
  now="$(date +%s)"
  elapsed=$((now - start_ts))
  if (( elapsed >= DURATION_SEC )); then
    break
  fi

  if [[ "$ENABLE_PARTITION" == "1" ]] && (( elapsed >= next_partition )); then
    log "fault injector: apply network partition to ayder port ${AYDER_PORT}"
    iptables_add_drop
    sleep "$PARTITION_HOLD_SEC"
    iptables_clear
    log "fault injector: heal network partition"
    next_partition=$((next_partition + PARTITION_EVERY_SEC))
  fi

  if [[ "$ENABLE_AYDER_KILL" == "1" ]] && (( elapsed >= next_kill )); then
    if [[ -n "$AYDER_PID_FILE" && -f "$AYDER_PID_FILE" ]]; then
      pid="$(cat "$AYDER_PID_FILE" 2>/dev/null || true)"
      if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
        log "fault injector: SIGKILL ayder pid=${pid}"
        kill -9 "$pid" >/dev/null 2>&1 || true
      fi
    fi
    next_kill=$((next_kill + AYDER_KILL_EVERY_SEC))
  fi

  sleep 1
done

log "fault injector complete"
