#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Run full Ayder 5-node partition test suite:
#   1) Start cluster (shared RF_BEARER_TOKENS)
#   2) Probe auth across all nodes
#   3) Run partition_test.sh with sudo (iptables)
#   4) Stop cluster (optional)
#
# Prereqs:
#   - start_ayder_cluster_5.sh
#   - stop_ayder_cluster_5.sh
#   - probe_ayder_auth_all.sh
#   - partition_test.sh
#   - TOKEN and/or RF_BEARER_TOKENS set
#   - Any HA/mTLS env vars already exported in this shell (if your setup needs them)
#
# Usage:
#   export RF_BEARER_TOKENS='your-token'
#   export TOKEN='your-token'   # optional if same as RF_BEARER_TOKENS; script syncs them
#   chmod +x run_partition_suite_5.sh
#   ./run_partition_suite_5.sh
#
# Useful overrides:
#   START_SCRIPT=./start_ayder_cluster_5.sh
#   STOP_SCRIPT=./stop_ayder_cluster_5.sh
#   PROBE_SCRIPT=./probe_ayder_auth_all.sh
#   PARTITION_SCRIPT=./partition_test.sh
#   CLEAN_START=1              # default: 1 (kills previous cluster from PID file)
#   STOP_CLUSTER=1             # default: 1 (stop cluster at end)
#   KEEP_CLUSTER_UP=1          # overrides STOP_CLUSTER, leaves cluster running
#   PROBE_WITH_SUDO=1          # default: 1 (lets probe inspect /proc env fingerprints)
#   SKIP_START=1               # if cluster already running, skip launcher
#   SKIP_PROBE=1               # if you want to skip auth probe
# -----------------------------------------------------------------------------

START_SCRIPT="${START_SCRIPT:-./start_ayder_cluster_5.sh}"
STOP_SCRIPT="${STOP_SCRIPT:-./stop_ayder_cluster_5.sh}"
PROBE_SCRIPT="${PROBE_SCRIPT:-./probe_ayder_auth_all.sh}"
PARTITION_SCRIPT="${PARTITION_SCRIPT:-./partition_test.sh}"

CLEAN_START="${CLEAN_START:-1}"
STOP_CLUSTER="${STOP_CLUSTER:-1}"
KEEP_CLUSTER_UP="${KEEP_CLUSTER_UP:-0}"
PROBE_WITH_SUDO="${PROBE_WITH_SUDO:-1}"
SKIP_START="${SKIP_START:-0}"
SKIP_PROBE="${SKIP_PROBE:-0}"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log()  { echo -e "${CYAN}[$(date +%H:%M:%S)]${NC} $*"; }
ok()   { echo -e "  ${GREEN}✅ $*${NC}"; }
warn() { echo -e "  ${YELLOW}⚠️  $*${NC}"; }
die()  { echo -e "${RED}ERROR:${NC} $*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "missing dependency: $1"; }

sha_of() {
  printf '%s' "$1" | sha256sum | awk '{print $1}'
}

stop_cluster_if_needed() {
  if [[ "$KEEP_CLUSTER_UP" == "1" ]]; then
    warn "KEEP_CLUSTER_UP=1 -> leaving cluster running"
    return 0
  fi

  if [[ "$STOP_CLUSTER" != "1" ]]; then
    warn "STOP_CLUSTER=$STOP_CLUSTER -> leaving cluster running"
    return 0
  fi

  if [[ -x "$STOP_SCRIPT" ]]; then
    log "Stopping cluster..."
    "$STOP_SCRIPT" || warn "stop script returned non-zero"
  else
    warn "Stop script not executable or missing: $STOP_SCRIPT"
  fi
}

on_exit() {
  local ec=$?
  if [[ $ec -ne 0 ]]; then
    warn "Wrapper exiting with code $ec"
  fi
  stop_cluster_if_needed
  exit "$ec"
}

main() {
  need bash
  need curl
  need sha256sum
  need sudo

  [[ -f "$START_SCRIPT" ]] || die "start script not found: $START_SCRIPT"
  [[ -f "$STOP_SCRIPT" ]] || die "stop script not found: $STOP_SCRIPT"
  [[ -f "$PROBE_SCRIPT" ]] || die "probe script not found: $PROBE_SCRIPT"
  [[ -f "$PARTITION_SCRIPT" ]] || die "partition script not found: $PARTITION_SCRIPT"

  [[ -x "$START_SCRIPT" ]] || die "start script not executable: $START_SCRIPT (run: chmod +x $START_SCRIPT)"
  [[ -x "$STOP_SCRIPT" ]] || die "stop script not executable: $STOP_SCRIPT (run: chmod +x $STOP_SCRIPT)"
  [[ -x "$PROBE_SCRIPT" ]] || die "probe script not executable: $PROBE_SCRIPT (run: chmod +x $PROBE_SCRIPT)"

  # Sync TOKEN <-> RF_BEARER_TOKENS (do not print secrets)
  if [[ -z "${RF_BEARER_TOKENS:-}" && -n "${TOKEN:-}" ]]; then
    export RF_BEARER_TOKENS="$TOKEN"
  fi
  if [[ -z "${TOKEN:-}" && -n "${RF_BEARER_TOKENS:-}" ]]; then
    export TOKEN="$RF_BEARER_TOKENS"
  fi

  [[ -n "${TOKEN:-}" ]] || die "TOKEN is not set"
  [[ -n "${RF_BEARER_TOKENS:-}" ]] || die "RF_BEARER_TOKENS is not set"

  local tok_len tok_sha srv_len srv_sha
  tok_len="${#TOKEN}"
  srv_len="${#RF_BEARER_TOKENS}"
  tok_sha="$(sha_of "$TOKEN")"
  srv_sha="$(sha_of "$RF_BEARER_TOKENS")"

  echo ""
  echo -e "${BOLD}╔════════════════════════════════════════════════════════════════════╗${NC}"
  echo -e "${BOLD}║             AYDER START + VERIFY + PARTITION TEST                 ║${NC}"
  echo -e "${BOLD}╚════════════════════════════════════════════════════════════════════╝${NC}"

  log "Client TOKEN              len=${tok_len} sha256=${tok_sha}"
  log "Server RF_BEARER_TOKENS   len=${srv_len} sha256=${srv_sha}"

  if [[ "$tok_len" != "$srv_len" || "$tok_sha" != "$srv_sha" ]]; then
    die "TOKEN and RF_BEARER_TOKENS do not match in this shell (fix exports before running)"
  fi

  # Ensure cleanup happens even if partition test fails
  trap on_exit EXIT

  # Start cluster
  if [[ "$SKIP_START" == "1" ]]; then
    warn "SKIP_START=1 -> assuming cluster is already running"
  else
    log "Starting 5-node cluster..."
    CLEAN_START="$CLEAN_START" "$START_SCRIPT"
    ok "Cluster launcher completed"
  fi

  # Probe auth across all nodes
  if [[ "$SKIP_PROBE" == "1" ]]; then
    warn "SKIP_PROBE=1 -> skipping auth probe"
  else
    log "Probing auth across all nodes..."
    if [[ "$PROBE_WITH_SUDO" == "1" ]]; then
      sudo --preserve-env=TOKEN,RF_BEARER_TOKENS "$PROBE_SCRIPT"
    else
      "$PROBE_SCRIPT"
    fi
    ok "Auth probe completed"
  fi

  # Run partition test (needs root for iptables)
  log "Running partition_test.sh with sudo (preserving TOKEN)..."
  sudo --preserve-env=TOKEN bash "$PARTITION_SCRIPT"
  ok "Partition test completed successfully"

  echo ""
  ok "Full suite finished successfully"
}

main "$@"