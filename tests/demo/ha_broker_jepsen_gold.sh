#!/usr/bin/env bash
# tests/demo/ha_broker_jepsen_gold.sh
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_ROOT="${CLUSTER_ROOT:-$ROOT_DIR/cluster}"
AYDER_BIN="${AYDER_BIN:-$ROOT_DIR/ayder}"

TOKEN="${TOKEN:-dev}"
AUTH_HEADER="Authorization: Bearer ${TOKEN}"

WORKERS="${WORKERS:-4}"
START_CLUSTER="${START_CLUSTER:-1}"
STRICT_CLAIM="${STRICT_CLAIM:-1}"

RF_HA_REPLICATION_TIMEOUT_MS="${RF_HA_REPLICATION_TIMEOUT_MS:-10000}"
RF_HA_READ_INDEX_TIMEOUT_MS="${RF_HA_READ_INDEX_TIMEOUT_MS:-3000}"

JEPSEN_PROFILE="${AYDER_JEPSEN_PROFILE:-hn-ready}"
JEPSEN_WORKLOAD="${AYDER_JEPSEN_WORKLOAD:-broker-log}"
JEPSEN_RUNS_PER_CELL="${AYDER_JEPSEN_RUNS_PER_CELL:-}"
JEPSEN_DURATIONS="${AYDER_JEPSEN_DURATIONS:-}"
JEPSEN_MODES="${AYDER_JEPSEN_MODES:-}"
JEPSEN_HISTORY_MODE="${AYDER_JEPSEN_LINEARIZABLE_HISTORY_MODE:-}"
JEPSEN_MIXED_HISTORY_MODE="${AYDER_JEPSEN_MIXED_LINEARIZABLE_HISTORY_MODE:-}"
JEPSEN_RESULTS_ROOT="${AYDER_JEPSEN_RESULTS_ROOT:-$ROOT_DIR/tests/jepsen/results}"
JEPSEN_NEMESIS_INTERVAL_SEC="${AYDER_JEPSEN_NEMESIS_INTERVAL:-6}"
JEPSEN_NEMESIS_STARTUP_SEC="${AYDER_JEPSEN_NEMESIS_STARTUP_SEC:-10}"
JEPSEN_MIXED_NEMESIS_INTERVAL_SEC="${AYDER_JEPSEN_MIXED_NEMESIS_INTERVAL_SEC:-$JEPSEN_NEMESIS_INTERVAL_SEC}"
JEPSEN_MIXED_NEMESIS_STARTUP_SEC="${AYDER_JEPSEN_MIXED_NEMESIS_STARTUP_SEC:-$JEPSEN_NEMESIS_STARTUP_SEC}"
JEPSEN_MIXED_CLIENT_STAGGER_MS="${AYDER_JEPSEN_MIXED_CLIENT_STAGGER_MS:-}"

SUDO_KEEPALIVE="${AYDER_JEPSEN_SUDO_KEEPALIVE:-1}"
SUDO_KEEPALIVE_INTERVAL_SEC="${AYDER_JEPSEN_SUDO_KEEPALIVE_INTERVAL_SEC:-25}"
SUDO_KEEPALIVE_PID=""

PRE_MATRIX_RECOVERY_ENABLED="${AYDER_JEPSEN_PRE_MATRIX_RECOVERY_ENABLED:-1}"
PRE_MATRIX_RECOVERY_WAIT_SEC="${AYDER_JEPSEN_PRE_MATRIX_RECOVERY_WAIT_SEC:-180}"

STRICT_HOST_ENFORCE="${AYDER_JEPSEN_ENFORCE_HOST_RESOURCES:-0}"
STRICT_HOST_MIN_RAM_GIB="${AYDER_JEPSEN_HOST_MIN_RAM_GIB:-20}"
STRICT_HOST_MIN_SWAP_GIB="${AYDER_JEPSEN_HOST_MIN_SWAP_GIB:-8}"

RUN_ID="${RUN_ID:-gold_$(date -u +%Y%m%dT%H%M%SZ)}"
MATRIX_ID="${AYDER_JEPSEN_MATRIX_ID:-$RUN_ID}"
MATRIX_DIR="${AYDER_JEPSEN_MATRIX_DIR:-$JEPSEN_RESULTS_ROOT/$MATRIX_ID}"

URLS_6='http://127.0.0.1:7001,http://127.0.0.1:8001,http://127.0.0.1:9001,http://127.0.0.1:10001,http://127.0.0.1:11001,http://127.0.0.1:12001'

log() { printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }
die() { printf '[%s] ERROR: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "missing command: $1"; }

apply_jepsen_profile_defaults() {
  local profile_runs profile_durations profile_modes profile_history
  case "$JEPSEN_PROFILE" in
    smoke)
      profile_runs=1
      profile_durations='20'
      profile_modes='kill-only'
      profile_history='all'
      ;;
    quick)
      profile_runs=2
      profile_durations='60 120'
      profile_modes='mixed partition-only kill-only'
      profile_history='all'
      ;;
    hn-ready|hn)
      profile_runs=5
      profile_durations='120 300 600'
      profile_modes='mixed partition-only kill-only'
      profile_history='completed-only'
      ;;
    soak)
      profile_runs=10
      profile_durations='120 300 600'
      profile_modes='mixed partition-only kill-only'
      profile_history='completed-only'
      ;;
    *)
      die "unknown AYDER_JEPSEN_PROFILE='${JEPSEN_PROFILE}' (allowed: smoke, quick, hn-ready, soak)"
      ;;
  esac

  [[ -n "$JEPSEN_RUNS_PER_CELL" ]] || JEPSEN_RUNS_PER_CELL="$profile_runs"
  [[ -n "$JEPSEN_DURATIONS" ]] || JEPSEN_DURATIONS="$profile_durations"
  [[ -n "$JEPSEN_MODES" ]] || JEPSEN_MODES="$profile_modes"
  [[ -n "$JEPSEN_HISTORY_MODE" ]] || JEPSEN_HISTORY_MODE="$profile_history"
  [[ -n "$JEPSEN_MIXED_HISTORY_MODE" ]] || JEPSEN_MIXED_HISTORY_MODE="$JEPSEN_HISTORY_MODE"

  if [[ -z "$JEPSEN_MIXED_CLIENT_STAGGER_MS" ]]; then
    JEPSEN_MIXED_CLIENT_STAGGER_MS=25
    if [[ "$STRICT_CLAIM" == '1' && "$JEPSEN_PROFILE" =~ ^(hn-ready|hn|soak)$ ]]; then
      JEPSEN_MIXED_CLIENT_STAGGER_MS=40
    fi
  fi
}

normalize_history_mode_for_strict_claim() {
  if [[ "$STRICT_CLAIM" == '1' && "$JEPSEN_WORKLOAD" == 'broker-log' ]]; then
    if [[ "$JEPSEN_HISTORY_MODE" != 'completed-only' || "$JEPSEN_MIXED_HISTORY_MODE" != 'completed-only' ]]; then
      log 'strict-claim guard: forcing history(base,mixed)=completed-only'
      JEPSEN_HISTORY_MODE='completed-only'
      JEPSEN_MIXED_HISTORY_MODE='completed-only'
    fi
  fi
}

log_and_check_host_resources() {
  local mem_kib=0 swap_kib=0
  if [[ -r /proc/meminfo ]]; then
    mem_kib="$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
    swap_kib="$(awk '/^SwapTotal:/ {print $2}' /proc/meminfo)"
  fi
  [[ "$mem_kib" =~ ^[0-9]+$ ]] || mem_kib=0
  [[ "$swap_kib" =~ ^[0-9]+$ ]] || swap_kib=0

  local mem_gib swap_gib
  mem_gib="$(awk -v kib="$mem_kib" 'BEGIN { printf "%.1f", kib/1048576 }')"
  swap_gib="$(awk -v kib="$swap_kib" 'BEGIN { printf "%.1f", kib/1048576 }')"
  log "host resources detected: ram=${mem_gib}GiB swap=${swap_gib}GiB"

  if [[ "$STRICT_CLAIM" != '1' ]]; then
    return 0
  fi
  if ! grep -qE '(^|[[:space:]])mixed($|[[:space:]])' <<<"$JEPSEN_MODES"; then
    return 0
  fi

  local req_mem_kib=$((STRICT_HOST_MIN_RAM_GIB * 1024 * 1024))
  local req_swap_kib=$((STRICT_HOST_MIN_SWAP_GIB * 1024 * 1024))
  if (( mem_kib >= req_mem_kib && swap_kib >= req_swap_kib )); then
    return 0
  fi

  local msg="host resources below strict mixed recommendation (need >=${STRICT_HOST_MIN_RAM_GIB}GiB RAM and >=${STRICT_HOST_MIN_SWAP_GIB}GiB swap; got ${mem_gib}GiB/${swap_gib}GiB)"
  if [[ "$STRICT_HOST_ENFORCE" == '1' ]]; then
    die "$msg"
  else
    log "WARN: $msg"
  fi
}

configure_jepsen_http_timeouts() {
  local rep_ms socket_ms conn_ms produce_ms read_ms
  rep_ms="$RF_HA_REPLICATION_TIMEOUT_MS"

  socket_ms=$((rep_ms + 4000))
  (( socket_ms < 12000 )) && socket_ms=12000

  conn_ms=5000

  produce_ms=$((rep_ms + 4000))
  (( produce_ms < 12000 )) && produce_ms=12000

  read_ms="$RF_HA_READ_INDEX_TIMEOUT_MS"
  (( read_ms < 3000 )) && read_ms=3000

  [[ -n "${AYDER_JEPSEN_HTTP_SOCKET_TIMEOUT_MS:-}" ]] || AYDER_JEPSEN_HTTP_SOCKET_TIMEOUT_MS="$socket_ms"
  [[ -n "${AYDER_JEPSEN_HTTP_CONN_TIMEOUT_MS:-}" ]] || AYDER_JEPSEN_HTTP_CONN_TIMEOUT_MS="$conn_ms"
  [[ -n "${AYDER_JEPSEN_BROKER_PRODUCE_TIMEOUT_MS:-}" ]] || AYDER_JEPSEN_BROKER_PRODUCE_TIMEOUT_MS="$produce_ms"
  [[ -n "${AYDER_JEPSEN_BROKER_READ_TIMEOUT_MS:-}" ]] || AYDER_JEPSEN_BROKER_READ_TIMEOUT_MS="$read_ms"

  log "jepsen http timeouts: socket=${AYDER_JEPSEN_HTTP_SOCKET_TIMEOUT_MS}ms conn=${AYDER_JEPSEN_HTTP_CONN_TIMEOUT_MS}ms produce=${AYDER_JEPSEN_BROKER_PRODUCE_TIMEOUT_MS}ms read=${AYDER_JEPSEN_BROKER_READ_TIMEOUT_MS}ms"
}

format_hms() {
  local s="${1:-0}"
  printf '%02d:%02d:%02d' $((s/3600)) $(((s%3600)/60)) $((s%60))
}

log_campaign_shape() {
  local mode_count=0 duration_sum=0 d
  for _ in $JEPSEN_MODES; do mode_count=$((mode_count + 1)); done
  for d in $JEPSEN_DURATIONS; do duration_sum=$((duration_sum + d)); done
  local nominal=$((mode_count * duration_sum * JEPSEN_RUNS_PER_CELL))

  log "jepsen profile='${JEPSEN_PROFILE}' modes='${JEPSEN_MODES}' durations='${JEPSEN_DURATIONS}' runs_per_cell=${JEPSEN_RUNS_PER_CELL}"
  log "nemesis timing: base(interval=${JEPSEN_NEMESIS_INTERVAL_SEC}s startup=${JEPSEN_NEMESIS_STARTUP_SEC}s) mixed(interval=${JEPSEN_MIXED_NEMESIS_INTERVAL_SEC}s startup=${JEPSEN_MIXED_NEMESIS_STARTUP_SEC}s)"
  log "mixed client stagger: ${JEPSEN_MIXED_CLIENT_STAGGER_MS}ms"
  log "matrix nominal runtime (without startup/overhead): ${nominal}s (~$(format_hms "$nominal"))"
}

setup_partition_cmd_defaults() {
  if [[ -n "${AYDER_JEPSEN_PARTITION_CMD:-}" && -n "${AYDER_JEPSEN_HEAL_CMD:-}" ]]; then
    return 0
  fi

  local docker_names=''
  if command -v docker >/dev/null 2>&1; then
    docker_names="$(timeout 2s docker ps --format '{{.Names}}' 2>/dev/null || true)"
  fi

  if grep -qx 'ayder-node1' <<<"$docker_names"; then
    export AYDER_JEPSEN_PARTITION_CMD="cd '$ROOT_DIR' && ./scripts/chaos-ha.sh leader-minority"
    export AYDER_JEPSEN_HEAL_CMD="cd '$ROOT_DIR' && ./scripts/chaos-ha.sh heal"
    log 'using docker chaos defaults for partition/heal'
  elif [[ -f "$ROOT_DIR/scripts/chaos-ha-local.sh" ]]; then
    export AYDER_JEPSEN_PARTITION_CMD="cd '$ROOT_DIR' && sudo -n bash '$ROOT_DIR/scripts/chaos-ha-local.sh' isolate-leader"
    export AYDER_JEPSEN_HEAL_CMD="cd '$ROOT_DIR' && sudo -n bash '$ROOT_DIR/scripts/chaos-ha-local.sh' heal"
    log 'using local iptables chaos defaults for partition/heal'
  fi
}

needs_partition_modes() {
  grep -qE '(^|[[:space:]])(partition-only|mixed)($|[[:space:]])' <<<"$JEPSEN_MODES"
}

uses_local_chaos() {
  [[ "${AYDER_JEPSEN_PARTITION_CMD:-}" == *'chaos-ha-local.sh'* || "${AYDER_JEPSEN_HEAL_CMD:-}" == *'chaos-ha-local.sh'* ]]
}

preflight_fault_commands() {
  if needs_partition_modes; then
    [[ -n "${AYDER_JEPSEN_PARTITION_CMD:-}" && -n "${AYDER_JEPSEN_HEAL_CMD:-}" ]] || die 'partition modes requested but partition/heal commands are missing'
  fi

  if needs_partition_modes && uses_local_chaos; then
    (cd "$ROOT_DIR" && bash "$ROOT_DIR/scripts/chaos-ha-local.sh" check >/dev/null) || die 'local chaos script selected but prerequisites failed (sudo -n + iptables path required)'
  fi
}

stop_sudo_keepalive() {
  if [[ -n "${SUDO_KEEPALIVE_PID:-}" ]] && kill -0 "$SUDO_KEEPALIVE_PID" >/dev/null 2>&1; then
    kill "$SUDO_KEEPALIVE_PID" >/dev/null 2>&1 || true
    wait "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
  fi
  SUDO_KEEPALIVE_PID=''
}

start_sudo_keepalive() {
  [[ "$SUDO_KEEPALIVE" == '1' ]] || return 0
  needs_partition_modes || return 0
  uses_local_chaos || return 0

  need sudo

  if ! sudo -n true >/dev/null 2>&1; then
    if [[ -t 0 || -t 1 ]]; then
      log 'acquiring sudo timestamp for local chaos commands'
      sudo -v || die 'sudo credential bootstrap failed'
    else
      die 'local chaos requires non-interactive sudo (-n). configure passwordless sudo for chaos commands'
    fi
  fi

  (
    while true; do
      sleep "$SUDO_KEEPALIVE_INTERVAL_SEC"
      sudo -n true >/dev/null 2>&1 || exit 0
    done
  ) &
  SUDO_KEEPALIVE_PID=$!
  log "sudo keepalive started (pid=$SUDO_KEEPALIVE_PID interval=${SUDO_KEEPALIVE_INTERVAL_SEC}s)"
}

wait_cluster_ready() {
  local timeout_sec="${1:-180}"
  local start now healthy fq
  start="$(date +%s)"

  while true; do
    healthy=0
    fq=0
    local url m role q
    IFS=',' read -r -a urls <<<"$URLS_6"
    for url in "${urls[@]}"; do
      if curl -fsS -H "$AUTH_HEADER" "$url/health" >/dev/null 2>&1; then
        healthy=$((healthy + 1))
      fi
      if [[ "$fq" -eq 0 ]]; then
        m="$(curl -fsS -H "$AUTH_HEADER" "$url/metrics_ha" 2>/dev/null || true)"
        if [[ -n "$m" ]]; then
          role="$(awk '/^ramforge_ha_role /{print $2; exit}' <<<"$m")"
          q="$(awk '/^ramforge_ha_fresh_quorum /{print $2; exit}' <<<"$m")"
          if [[ "$role" == '0' && "$q" == '1' ]]; then
            fq=1
          fi
        fi
      fi
    done

    if (( healthy >= 4 )) && [[ "$fq" -eq 1 ]]; then
      log "cluster ready healthy=${healthy}/6 leader_fresh_quorum=1"
      return 0
    fi

    now="$(date +%s)"
    if (( now - start >= timeout_sec )); then
      die "cluster readiness timeout healthy=${healthy}/6 leader_fresh_quorum=${fq}"
    fi

    sleep 1
  done
}

ensure_jepsen_toolchain() {
  "$ROOT_DIR/tests/jepsen/bin/bootstrap-toolchain.sh" >/dev/null
}

run_matrix() {
  mkdir -p "$MATRIX_DIR"

  local pre_cell_recovery_cmd="cd '$ROOT_DIR' && FORCE_RESTART_ALL=1 WAIT_FOR_READY=1 READINESS_WAIT_SEC=120 bash ./tests/demo/ha6_recover_local_cluster.sh"

  set +e
  env \
    AYDER_JEPSEN_TOKEN="$TOKEN" \
    AYDER_JEPSEN_WORKLOAD="$JEPSEN_WORKLOAD" \
    AYDER_JEPSEN_RUNS_PER_CELL="$JEPSEN_RUNS_PER_CELL" \
    AYDER_JEPSEN_DURATIONS="$JEPSEN_DURATIONS" \
    AYDER_JEPSEN_MODES="$JEPSEN_MODES" \
    AYDER_JEPSEN_LINEARIZABLE_HISTORY_MODE="$JEPSEN_HISTORY_MODE" \
    AYDER_JEPSEN_MIXED_LINEARIZABLE_HISTORY_MODE="$JEPSEN_MIXED_HISTORY_MODE" \
    AYDER_JEPSEN_NEMESIS_INTERVAL="$JEPSEN_NEMESIS_INTERVAL_SEC" \
    AYDER_JEPSEN_NEMESIS_STARTUP_SEC="$JEPSEN_NEMESIS_STARTUP_SEC" \
    AYDER_JEPSEN_MIXED_NEMESIS_INTERVAL_SEC="$JEPSEN_MIXED_NEMESIS_INTERVAL_SEC" \
    AYDER_JEPSEN_MIXED_NEMESIS_STARTUP_SEC="$JEPSEN_MIXED_NEMESIS_STARTUP_SEC" \
    AYDER_JEPSEN_MIXED_CLIENT_STAGGER_MS="$JEPSEN_MIXED_CLIENT_STAGGER_MS" \
    AYDER_JEPSEN_HTTP_SOCKET_TIMEOUT_MS="$AYDER_JEPSEN_HTTP_SOCKET_TIMEOUT_MS" \
    AYDER_JEPSEN_HTTP_CONN_TIMEOUT_MS="$AYDER_JEPSEN_HTTP_CONN_TIMEOUT_MS" \
    AYDER_JEPSEN_BROKER_PRODUCE_TIMEOUT_MS="$AYDER_JEPSEN_BROKER_PRODUCE_TIMEOUT_MS" \
    AYDER_JEPSEN_BROKER_READ_TIMEOUT_MS="$AYDER_JEPSEN_BROKER_READ_TIMEOUT_MS" \
    AYDER_JEPSEN_NODES="$URLS_6" \
    AYDER_JEPSEN_PARTITION_CMD="${AYDER_JEPSEN_PARTITION_CMD:-}" \
    AYDER_JEPSEN_HEAL_CMD="${AYDER_JEPSEN_HEAL_CMD:-}" \
    AYDER_JEPSEN_RESULTS_ROOT="$JEPSEN_RESULTS_ROOT" \
    AYDER_JEPSEN_MATRIX_ID="$MATRIX_ID" \
    AYDER_JEPSEN_MATRIX_DIR="$MATRIX_DIR" \
    AYDER_JEPSEN_PRE_READY_TIMEOUT_SEC="${AYDER_JEPSEN_PRE_READY_TIMEOUT_SEC:-180}" \
    AYDER_JEPSEN_PRE_READY_RECOVERY_AFTER_SEC="${AYDER_JEPSEN_PRE_READY_RECOVERY_AFTER_SEC:-45}" \
    AYDER_JEPSEN_PRE_READY_RECOVERY_TIMEOUT_SEC="${AYDER_JEPSEN_PRE_READY_RECOVERY_TIMEOUT_SEC:-120}" \
    AYDER_JEPSEN_PRE_READY_RECOVERY_CMD="cd '$ROOT_DIR' && FORCE_RESTART_ALL=1 WAIT_FOR_READY=1 READINESS_WAIT_SEC=120 bash ./tests/demo/ha6_recover_local_cluster.sh" \
    AYDER_JEPSEN_PRE_CELL_RECOVERY_ENABLED="${AYDER_JEPSEN_PRE_CELL_RECOVERY_ENABLED:-1}" \
    AYDER_JEPSEN_PRE_CELL_RECOVERY_TIMEOUT_SEC="${AYDER_JEPSEN_PRE_CELL_RECOVERY_TIMEOUT_SEC:-300}" \
    AYDER_JEPSEN_PRE_CELL_RECOVERY_CMD="${AYDER_JEPSEN_PRE_CELL_RECOVERY_CMD:-$pre_cell_recovery_cmd}" \
    "$ROOT_DIR/tests/jepsen/bin/campaign-matrix.sh"
  local rc=$?
  set -e
  return "$rc"
}

main() {
  trap stop_sudo_keepalive EXIT

  need curl
  need jq
  need awk
  need bash

  [[ -x "$AYDER_BIN" ]] || die "ayder binary is not executable: $AYDER_BIN"

  apply_jepsen_profile_defaults
  normalize_history_mode_for_strict_claim
  log_and_check_host_resources
  configure_jepsen_http_timeouts
  log_campaign_shape

  setup_partition_cmd_defaults
  preflight_fault_commands
  start_sudo_keepalive

  ensure_jepsen_toolchain

  if [[ "$START_CLUSTER" == '1' ]]; then
    log 'starting/recovering local 6-node cluster'
    (cd "$ROOT_DIR" && FORCE_RESTART_ALL=1 WAIT_FOR_READY=1 READINESS_WAIT_SEC=120 bash ./tests/demo/ha6_recover_local_cluster.sh) || die 'cluster recovery failed'
  elif [[ "$PRE_MATRIX_RECOVERY_ENABLED" == '1' ]]; then
    log 'pre-matrix recovery start'
    (cd "$ROOT_DIR" && FORCE_RESTART_ALL=auto WAIT_FOR_READY=1 READINESS_WAIT_SEC="$PRE_MATRIX_RECOVERY_WAIT_SEC" bash ./tests/demo/ha6_recover_local_cluster.sh) || die 'pre-matrix recovery failed'
  fi

  wait_cluster_ready "${AYDER_JEPSEN_PRE_READY_TIMEOUT_SEC:-180}"

  log "running Jepsen matrix: modes='${JEPSEN_MODES}' durations='${JEPSEN_DURATIONS}' runs_per_cell=${JEPSEN_RUNS_PER_CELL}"
  if run_matrix; then
    log "matrix validation passed"
    log "SUCCESS: gold run complete (matrix_dir=$MATRIX_DIR)"
    echo "matrix_dir=$MATRIX_DIR"
    return 0
  fi

  die "matrix validation failed (matrix_dir=$MATRIX_DIR)"
}

main "$@"
