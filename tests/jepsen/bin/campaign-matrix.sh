#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT_DIR/tests/jepsen/bin/_toolchain.sh"

WORKLOAD="${AYDER_JEPSEN_WORKLOAD:-broker-log}"
RUNS_PER_CELL="${AYDER_JEPSEN_RUNS_PER_CELL:-10}"
DURATIONS="${AYDER_JEPSEN_DURATIONS:-120 300 600}"
MODES="${AYDER_JEPSEN_MODES:-partition-only kill-only mixed}"
BASE_CLIENT_STAGGER_MS="${AYDER_JEPSEN_CLIENT_STAGGER_MS:-10}"
BASE_HISTORY_MODE="${AYDER_JEPSEN_LINEARIZABLE_HISTORY_MODE:-all}"
BASE_LEIN_JAVA_OPTS="${AYDER_JEPSEN_LEIN_JAVA_OPTS:-${LEIN_JAVA_OPTS:-}}"
MIXED_CLIENT_STAGGER_MS="${AYDER_JEPSEN_MIXED_CLIENT_STAGGER_MS:-25}"
MIXED_HISTORY_MODE="${AYDER_JEPSEN_MIXED_LINEARIZABLE_HISTORY_MODE:-$BASE_HISTORY_MODE}"
MIXED_LEIN_JAVA_OPTS="${AYDER_JEPSEN_MIXED_LEIN_JAVA_OPTS:-$BASE_LEIN_JAVA_OPTS}"
BASE_NEMESIS_INTERVAL_SEC="${AYDER_JEPSEN_NEMESIS_INTERVAL:-6}"
BASE_NEMESIS_STARTUP_SEC="${AYDER_JEPSEN_NEMESIS_STARTUP_SEC:-10}"
MIXED_NEMESIS_INTERVAL_SEC="${AYDER_JEPSEN_MIXED_NEMESIS_INTERVAL_SEC:-$BASE_NEMESIS_INTERVAL_SEC}"
MIXED_NEMESIS_STARTUP_SEC="${AYDER_JEPSEN_MIXED_NEMESIS_STARTUP_SEC:-$BASE_NEMESIS_STARTUP_SEC}"
BASE_RUN_WALL_TIMEOUT_SEC="${AYDER_JEPSEN_RUN_WALL_TIMEOUT_SEC:-}"
MIXED_RUN_WALL_TIMEOUT_SEC="${AYDER_JEPSEN_MIXED_RUN_WALL_TIMEOUT_SEC:-$BASE_RUN_WALL_TIMEOUT_SEC}"
PRE_CELL_RECOVERY_ENABLED="${AYDER_JEPSEN_PRE_CELL_RECOVERY_ENABLED:-0}"
PRE_CELL_RECOVERY_CMD="${AYDER_JEPSEN_PRE_CELL_RECOVERY_CMD:-}"
PRE_CELL_RECOVERY_TIMEOUT_SEC="${AYDER_JEPSEN_PRE_CELL_RECOVERY_TIMEOUT_SEC:-0}"

if [[ -z "$MIXED_LEIN_JAVA_OPTS" ]]; then
  mixed_mem_kib=0
  if [[ -r /proc/meminfo ]]; then
    mixed_mem_kib="$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
  fi
  [[ "$mixed_mem_kib" =~ ^[0-9]+$ ]] || mixed_mem_kib=0

  if [[ "$MIXED_HISTORY_MODE" == 'all' ]]; then
    mixed_default_xmx_mb=8192
    if (( mixed_mem_kib >= 29360128 )); then
      mixed_default_xmx_mb=12288
    fi
    MIXED_LEIN_JAVA_OPTS="-Xms1024m -Xmx${mixed_default_xmx_mb}m -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
  else
    mixed_default_xmx_mb=4096
    if (( mixed_mem_kib >= 16777216 )); then
      mixed_default_xmx_mb=6144
    fi
    if (( mixed_mem_kib >= 25165824 )); then
      mixed_default_xmx_mb=8192
    fi
    MIXED_LEIN_JAVA_OPTS="-Xms1024m -Xmx${mixed_default_xmx_mb}m -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
  fi
fi

log() {
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

count_words() {
  local c=0 _x
  for _x in $1; do
    c=$((c + 1))
  done
  echo "$c"
}

sum_words() {
  local s=0 _x
  for _x in $1; do
    s=$((s + _x))
  done
  echo "$s"
}

fmt_hms() {
  local total="$1"
  printf '%02d:%02d:%02d' $((total/3600)) $(((total%3600)/60)) $((total%60))
}

run_pre_cell_recovery() {
  local mode="$1"
  local dur="$2"
  local idx="$3"
  local total="$4"
  local rc=0

  [[ "$PRE_CELL_RECOVERY_ENABLED" == '1' ]] || return 0

  if [[ -z "$PRE_CELL_RECOVERY_CMD" ]]; then
    echo "AYDER_JEPSEN_PRE_CELL_RECOVERY_ENABLED=1 but AYDER_JEPSEN_PRE_CELL_RECOVERY_CMD is empty" >&2
    return 1
  fi

  log "cell_recovery_start idx=$idx/$total mode=$mode duration_sec=$dur timeout_sec=${PRE_CELL_RECOVERY_TIMEOUT_SEC:-0}"

  set +e
  if [[ "$PRE_CELL_RECOVERY_TIMEOUT_SEC" =~ ^[0-9]+$ ]] && [[ "$PRE_CELL_RECOVERY_TIMEOUT_SEC" -gt 0 ]] && command -v timeout >/dev/null 2>&1; then
    timeout "${PRE_CELL_RECOVERY_TIMEOUT_SEC}s" bash -lc "$PRE_CELL_RECOVERY_CMD"
    rc=$?
  else
    bash -lc "$PRE_CELL_RECOVERY_CMD"
    rc=$?
  fi
  set -e

  if [[ "$rc" -ne 0 ]]; then
    log "cell_recovery_fail idx=$idx/$total mode=$mode duration_sec=$dur rc=$rc"
    return "$rc"
  fi

  log "cell_recovery_done idx=$idx/$total mode=$mode duration_sec=$dur"
  return 0
}

if ! jepsen_require_toolchain "$ROOT_DIR"; then
  exit 1
fi

case "$BASE_HISTORY_MODE" in
  all|completed-only) ;;
  *)
    echo "invalid AYDER_JEPSEN_LINEARIZABLE_HISTORY_MODE: $BASE_HISTORY_MODE (allowed: all, completed-only)" >&2
    exit 2
    ;;
esac

case "$MIXED_HISTORY_MODE" in
  all|completed-only) ;;
  *)
    echo "invalid AYDER_JEPSEN_MIXED_LINEARIZABLE_HISTORY_MODE: $MIXED_HISTORY_MODE (allowed: all, completed-only)" >&2
    exit 2
    ;;
esac

RESULTS_ROOT="${AYDER_JEPSEN_RESULTS_ROOT:-$ROOT_DIR/tests/jepsen/results}"
MATRIX_ID="${AYDER_JEPSEN_MATRIX_ID:-matrix_$(date +%Y%m%dT%H%M%S%N)}"
MATRIX_DIR="${AYDER_JEPSEN_MATRIX_DIR:-$RESULTS_ROOT/$MATRIX_ID}"
mkdir -p "$MATRIX_DIR"

select_chaos_defaults() {
  local token="${AYDER_JEPSEN_TOKEN:-dev}"
  local docker_names=""

  if command -v docker >/dev/null 2>&1; then
    docker_names="$(timeout 2s docker ps --format '{{.Names}}' 2>/dev/null || true)"
  fi

  if grep -qx 'ayder-node1' <<<"$docker_names"; then
    DEFAULT_PARTITION_CMD="cd '$ROOT_DIR' && ./scripts/chaos-ha.sh leader-minority"
    DEFAULT_HEAL_CMD="cd '$ROOT_DIR' && ./scripts/chaos-ha.sh heal"
  elif [[ -f "$ROOT_DIR/scripts/chaos-ha-local.sh" ]]; then
    DEFAULT_PARTITION_CMD="cd '$ROOT_DIR' && sudo -n bash '$ROOT_DIR/scripts/chaos-ha-local.sh' isolate-leader"
    DEFAULT_HEAL_CMD="cd '$ROOT_DIR' && sudo -n bash '$ROOT_DIR/scripts/chaos-ha-local.sh' heal"
  else
    DEFAULT_PARTITION_CMD="cd '$ROOT_DIR' && ./scripts/chaos-ha.sh leader-minority"
    DEFAULT_HEAL_CMD="cd '$ROOT_DIR' && ./scripts/chaos-ha.sh heal"
  fi
}
select_chaos_defaults

DEFAULT_KILL_CMD='pids=$(pgrep -f "[a]yder --port 9001" || true); for p in $pids; do kill -9 "$p" 2>/dev/null || true; done; true'
DEFAULT_RESTART_CMD="cd '$ROOT_DIR/cluster/node3' && source '$ROOT_DIR/tests/demo/ha6_stability_env_preset.sh' node3 && setsid '$ROOT_DIR/ayder' --port 9001 --workers 4 > /tmp/ayder-node3-jepsen.log 2>&1 < /dev/null & disown || true"

{
  echo "matrix_id=$MATRIX_ID"
  echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "workload=$WORKLOAD"
  echo "runs_per_cell=$RUNS_PER_CELL"
  echo "durations=$DURATIONS"
  echo "modes=$MODES"
  echo "base_client_stagger_ms=$BASE_CLIENT_STAGGER_MS"
  echo "base_linearizable_history_mode=$BASE_HISTORY_MODE"
  echo "mixed_client_stagger_ms=$MIXED_CLIENT_STAGGER_MS"
  echo "mixed_linearizable_history_mode=$MIXED_HISTORY_MODE"
  echo "base_lein_java_opts=$BASE_LEIN_JAVA_OPTS"
  echo "mixed_lein_java_opts=$MIXED_LEIN_JAVA_OPTS"
  echo "base_nemesis_interval_sec=$BASE_NEMESIS_INTERVAL_SEC"
  echo "base_nemesis_startup_sec=$BASE_NEMESIS_STARTUP_SEC"
  echo "mixed_nemesis_interval_sec=$MIXED_NEMESIS_INTERVAL_SEC"
  echo "mixed_nemesis_startup_sec=$MIXED_NEMESIS_STARTUP_SEC"
  echo "base_run_wall_timeout_sec=$BASE_RUN_WALL_TIMEOUT_SEC"
  echo "mixed_run_wall_timeout_sec=$MIXED_RUN_WALL_TIMEOUT_SEC"
  echo "pre_cell_recovery_enabled=$PRE_CELL_RECOVERY_ENABLED"
  echo "pre_cell_recovery_timeout_sec=$PRE_CELL_RECOVERY_TIMEOUT_SEC"
  echo "pre_cell_recovery_cmd=$PRE_CELL_RECOVERY_CMD"
  echo "http_socket_timeout_ms=${AYDER_JEPSEN_HTTP_SOCKET_TIMEOUT_MS:-}"
  echo "http_conn_timeout_ms=${AYDER_JEPSEN_HTTP_CONN_TIMEOUT_MS:-}"
  echo "broker_produce_timeout_ms=${AYDER_JEPSEN_BROKER_PRODUCE_TIMEOUT_MS:-}"
  echo "broker_read_timeout_ms=${AYDER_JEPSEN_BROKER_READ_TIMEOUT_MS:-}"
  echo "matrix_dir=$MATRIX_DIR"
  echo "default_partition_cmd=$DEFAULT_PARTITION_CMD"
  echo "default_heal_cmd=$DEFAULT_HEAL_CMD"
} > "$MATRIX_DIR/manifest.env"

mode_count="$(count_words "$MODES")"
duration_count="$(count_words "$DURATIONS")"
duration_sum="$(sum_words "$DURATIONS")"
total_cells=$((mode_count * duration_count))
nominal_runtime_sec=$((RUNS_PER_CELL * duration_sum * mode_count))

log "matrix_start id=$MATRIX_ID workload=$WORKLOAD cells=$total_cells runs_per_cell=$RUNS_PER_CELL"
log "matrix_nominal_runtime_sec=$nominal_runtime_sec (~$(fmt_hms "$nominal_runtime_sec"))"
log "matrix_dir=$MATRIX_DIR"

total_pass=0
total_fail=0
cell_idx=0

cell_summary="$MATRIX_DIR/cells.csv"
echo 'mode,duration_sec,campaign_dir,exit_code' > "$cell_summary"

for mode in $MODES; do
  for dur in $DURATIONS; do
    cell_idx=$((cell_idx + 1))

    partition_cmd="$DEFAULT_PARTITION_CMD"
    heal_cmd="$DEFAULT_HEAL_CMD"
    kill_cmd="$DEFAULT_KILL_CMD"
    restart_cmd="$DEFAULT_RESTART_CMD"

    case "$mode" in
      partition-only)
        kill_cmd=':'
        restart_cmd=':'
        ;;
      kill-only)
        partition_cmd=':'
        heal_cmd=':'
        ;;
      mixed)
        ;;
      *)
        echo "unknown mode: $mode" >&2
        exit 2
        ;;
    esac

    cell_client_stagger_ms="$BASE_CLIENT_STAGGER_MS"
    cell_history_mode="$BASE_HISTORY_MODE"
    cell_lein_java_opts="$BASE_LEIN_JAVA_OPTS"
    cell_nemesis_interval_sec="$BASE_NEMESIS_INTERVAL_SEC"
    cell_nemesis_startup_sec="$BASE_NEMESIS_STARTUP_SEC"
    cell_run_wall_timeout_sec="$BASE_RUN_WALL_TIMEOUT_SEC"

    if [[ "$mode" == 'mixed' ]]; then
      cell_client_stagger_ms="$MIXED_CLIENT_STAGGER_MS"
      cell_history_mode="$MIXED_HISTORY_MODE"
      cell_lein_java_opts="$MIXED_LEIN_JAVA_OPTS"
      cell_nemesis_interval_sec="$MIXED_NEMESIS_INTERVAL_SEC"
      cell_nemesis_startup_sec="$MIXED_NEMESIS_STARTUP_SEC"
      cell_run_wall_timeout_sec="$MIXED_RUN_WALL_TIMEOUT_SEC"
    fi

    cell_campaign_dir="$MATRIX_DIR/${mode}_${dur}s"

    if ! run_pre_cell_recovery "$mode" "$dur" "$cell_idx" "$total_cells"; then
      total_fail=$((total_fail + 1))
      echo "$mode,$dur,$cell_campaign_dir,1" >> "$cell_summary"
      log "cell_done idx=$cell_idx/$total_cells mode=$mode duration_sec=$dur rc=1 elapsed_sec=0 pass_cells=$total_pass fail_cells=$total_fail reason=pre_cell_recovery_failed"
      continue
    fi

    log "cell_start idx=$cell_idx/$total_cells mode=$mode duration_sec=$dur runs=$RUNS_PER_CELL workload=$WORKLOAD stagger_ms=$cell_client_stagger_ms history_mode=$cell_history_mode nemesis_interval_sec=$cell_nemesis_interval_sec nemesis_startup_sec=$cell_nemesis_startup_sec run_wall_timeout_sec=${cell_run_wall_timeout_sec:-default}"

    cell_start="$(date +%s)"
    set +e
    env \
      AYDER_JEPSEN_WORKLOAD="$WORKLOAD" \
      AYDER_JEPSEN_TIME="$dur" \
      AYDER_JEPSEN_RUNS="$RUNS_PER_CELL" \
      AYDER_JEPSEN_RUN_WALL_TIMEOUT_SEC="$cell_run_wall_timeout_sec" \
      AYDER_JEPSEN_PARTITION_CMD="$partition_cmd" \
      AYDER_JEPSEN_HEAL_CMD="$heal_cmd" \
      AYDER_JEPSEN_KILL_CMD="$kill_cmd" \
      AYDER_JEPSEN_RESTART_CMD="$restart_cmd" \
      AYDER_JEPSEN_CLIENT_STAGGER_MS="$cell_client_stagger_ms" \
      AYDER_JEPSEN_LINEARIZABLE_HISTORY_MODE="$cell_history_mode" \
      AYDER_JEPSEN_NEMESIS_INTERVAL="$cell_nemesis_interval_sec" \
      AYDER_JEPSEN_NEMESIS_STARTUP_SEC="$cell_nemesis_startup_sec" \
      LEIN_JAVA_OPTS="$cell_lein_java_opts" \
      AYDER_JEPSEN_CAMPAIGN_ID="${MATRIX_ID}_${mode}_${dur}s" \
      AYDER_JEPSEN_CAMPAIGN_DIR="$cell_campaign_dir" \
      "$ROOT_DIR/tests/jepsen/bin/campaign.sh"
    rc=$?
    set -e

    if [[ "$rc" -eq 0 ]]; then
      total_pass=$((total_pass + 1))
    else
      total_fail=$((total_fail + 1))
    fi

    echo "$mode,$dur,$cell_campaign_dir,$rc" >> "$cell_summary"

    cell_elapsed=$(( $(date +%s) - cell_start ))
    log "cell_done idx=$cell_idx/$total_cells mode=$mode duration_sec=$dur rc=$rc elapsed_sec=$cell_elapsed pass_cells=$total_pass fail_cells=$total_fail"
  done
done

log "matrix_done pass_cells=$total_pass fail_cells=$total_fail"
echo "pass_cells=$total_pass" >> "$MATRIX_DIR/manifest.env"
echo "fail_cells=$total_fail" >> "$MATRIX_DIR/manifest.env"

auto_bundle="${AYDER_JEPSEN_AUTO_BUNDLE:-1}"
if [[ "$auto_bundle" == '1' ]]; then
  set +e
  "$ROOT_DIR/tests/jepsen/bin/bundle-artifacts.sh" "$MATRIX_DIR"
  bundle_rc=$?
  set -e
  echo "bundle_rc=$bundle_rc" >> "$MATRIX_DIR/manifest.env"
fi

if [[ "$total_fail" -gt 0 ]]; then
  exit 1
fi