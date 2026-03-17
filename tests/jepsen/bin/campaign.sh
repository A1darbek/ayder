#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT_DIR/tests/jepsen/bin/_toolchain.sh"

RUNS="${AYDER_JEPSEN_RUNS:-30}"
TIME="${AYDER_JEPSEN_TIME:-120}"
WORKLOAD="${AYDER_JEPSEN_WORKLOAD:-broker-log}"
RUN_HEARTBEAT_SEC="${AYDER_JEPSEN_RUN_HEARTBEAT_SEC:-30}"
RUN_WALL_TIMEOUT_SEC="${AYDER_JEPSEN_RUN_WALL_TIMEOUT_SEC:-$((TIME + 900))}"
INTER_RUN_SLEEP_SEC="${AYDER_JEPSEN_INTER_RUN_SLEEP_SEC:-3}"
RUN_OOM_FAST_FAIL="${AYDER_JEPSEN_RUN_OOM_FAST_FAIL:-1}"
RUN_OOM_ABORT_THRESHOLD="${AYDER_JEPSEN_RUN_OOM_ABORT_THRESHOLD:-8}"
RUN_OOM_PATTERN="${AYDER_JEPSEN_RUN_OOM_PATTERN:-knossos.search Out of memory; aborting search}"

if ! jepsen_require_toolchain "$ROOT_DIR"; then
  exit 1
fi

if ! [[ "$RUN_WALL_TIMEOUT_SEC" =~ ^[0-9]+$ ]]; then
  echo "invalid AYDER_JEPSEN_RUN_WALL_TIMEOUT_SEC=$RUN_WALL_TIMEOUT_SEC (must be integer >= 0)" >&2
  exit 2
fi

if ! [[ "$INTER_RUN_SLEEP_SEC" =~ ^[0-9]+$ ]]; then
  echo "invalid AYDER_JEPSEN_INTER_RUN_SLEEP_SEC=$INTER_RUN_SLEEP_SEC (must be integer >= 0)" >&2
  exit 2
fi

if ! [[ "$RUN_OOM_ABORT_THRESHOLD" =~ ^[0-9]+$ ]]; then
  echo "invalid AYDER_JEPSEN_RUN_OOM_ABORT_THRESHOLD=$RUN_OOM_ABORT_THRESHOLD (must be integer >= 0)" >&2
  exit 2
fi

RESULTS_ROOT="${AYDER_JEPSEN_RESULTS_ROOT:-$ROOT_DIR/tests/jepsen/results}"
CAMPAIGN_ID="${AYDER_JEPSEN_CAMPAIGN_ID:-campaign_$(date +%Y%m%dT%H%M%S%N)}"
CAMPAIGN_DIR="${AYDER_JEPSEN_CAMPAIGN_DIR:-$RESULTS_ROOT/$CAMPAIGN_ID}"
mkdir -p "$CAMPAIGN_DIR"

GIT_SHA="$(git -C "$ROOT_DIR" rev-parse --verify HEAD 2>/dev/null || echo unknown)"
if GIT_DIRTY_RAW="$(git -C "$ROOT_DIR" status --porcelain 2>/dev/null)"; then
  GIT_DIRTY_COUNT="$(awk 'NF{c++} END{print c+0}' <<<"$GIT_DIRTY_RAW")"
else
  GIT_DIRTY_COUNT="unknown"
fi

{
  echo "campaign_id=$CAMPAIGN_ID"
  echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "workload=$WORKLOAD"
  echo "runs=$RUNS"
  echo "time_limit_sec=$TIME"
  echo "run_wall_timeout_sec=$RUN_WALL_TIMEOUT_SEC"
  echo "inter_run_sleep_sec=$INTER_RUN_SLEEP_SEC"
  echo "root_dir=$ROOT_DIR"
  echo "campaign_dir=$CAMPAIGN_DIR"
  echo "git_sha=$GIT_SHA"
  echo "git_dirty_count=$GIT_DIRTY_COUNT"
} > "$CAMPAIGN_DIR/manifest.env"

summary_csv="$CAMPAIGN_DIR/summary.csv"
echo "run,workload,time_sec,topic,group,key,exit_code" > "$summary_csv"

pass=0
fail=0

proc_start_jiffies() {
  local pid="$1"
  [[ -r "/proc/$pid/stat" ]] || return 1
  awk '{print $22}' "/proc/$pid/stat" 2>/dev/null
}

same_process_alive() {
  local pid="$1"
  local expected_start="$2"
  local current_start=""

  if [[ -n "$expected_start" ]]; then
    current_start="$(proc_start_jiffies "$pid" || true)"
    [[ -n "$current_start" && "$current_start" == "$expected_start" ]]
    return $?
  fi

  kill -0 "$pid" 2>/dev/null
}

collect_descendants() {
  local pid="$1"
  local child

  while IFS= read -r child; do
    [[ -n "$child" ]] || continue
    collect_descendants "$child"
    echo "$child"
  done < <(pgrep -P "$pid" 2>/dev/null || true)
}

kill_run_process_tree() {
  local pid="$1"
  local descendants=()

  mapfile -t descendants < <(collect_descendants "$pid")

  if (( ${#descendants[@]} > 0 )); then
    kill -TERM "${descendants[@]}" 2>/dev/null || true
  fi
  kill "$pid" 2>/dev/null || true

  sleep 2

  if (( ${#descendants[@]} > 0 )); then
    kill -KILL "${descendants[@]}" 2>/dev/null || true
  fi
  kill -9 "$pid" 2>/dev/null || true
}

oom_marker_count() {
  local log_path="$1"
  [[ -f "$log_path" ]] || { echo 0; return 0; }
  grep -F -c -- "$RUN_OOM_PATTERN" "$log_path" 2>/dev/null || echo 0
}

run_with_heartbeat() {
  local label="$1"
  shift

  local start now elapsed timed_out child_pid child_start
  local oom_fastfail oom_hits
  start="$(date +%s)"
  timed_out=0
  oom_fastfail=0

  "$@" &
  child_pid=$!

  child_start="$(proc_start_jiffies "$child_pid" || true)"

  while same_process_alive "$child_pid" "$child_start"; do
    sleep "$RUN_HEARTBEAT_SEC"
    if ! same_process_alive "$child_pid" "$child_start"; then
      break
    fi
    now="$(date +%s)"
    elapsed=$((now - start))
    echo "run_heartbeat label=$label elapsed_sec=$elapsed"

    if [[ "$RUN_OOM_FAST_FAIL" == "1" && -n "${RUN_HEARTBEAT_MONITOR_LOG:-}" ]]; then
      oom_hits="$(oom_marker_count "$RUN_HEARTBEAT_MONITOR_LOG")"
      if [[ "$oom_hits" =~ ^[0-9]+$ ]] && (( oom_hits >= RUN_OOM_ABORT_THRESHOLD )); then
        echo "run_oom_fastfail label=$label oom_events=$oom_hits threshold=$RUN_OOM_ABORT_THRESHOLD"
        kill_run_process_tree "$child_pid"
        oom_fastfail=1
        break
      fi
    fi

    if [[ "$RUN_WALL_TIMEOUT_SEC" -gt 0 && "$elapsed" -ge "$RUN_WALL_TIMEOUT_SEC" ]]; then
      echo "run_timeout label=$label elapsed_sec=$elapsed timeout_sec=$RUN_WALL_TIMEOUT_SEC"
      kill_run_process_tree "$child_pid"
      timed_out=1
      break
    fi
  done

  local rc=0
  wait "$child_pid" || rc=$?
  if [[ "$timed_out" -eq 1 ]]; then
    rc=124
  elif [[ "$oom_fastfail" -eq 1 ]]; then
    rc=137
  fi
  return "$rc"
}

for i in $(seq 1 "$RUNS"); do
  echo "=== Jepsen run $i/$RUNS (workload=$WORKLOAD) ==="
  run_dir="$CAMPAIGN_DIR/run_$i"

  topic=""
  group=""
  key=""

  if [[ "$WORKLOAD" == "broker-log" ]]; then
    topic="jepsen_log_${CAMPAIGN_ID}_$i"
    group="jepsen_g_${CAMPAIGN_ID}_$i"

    set +e
    RUN_HEARTBEAT_MONITOR_LOG="$run_dir/runner.log" run_with_heartbeat "$i/$RUNS" \
      env \
      "AYDER_JEPSEN_WORKLOAD=$WORKLOAD" \
      "AYDER_JEPSEN_TOPIC=$topic" \
      "AYDER_JEPSEN_GROUP=$group" \
      "AYDER_JEPSEN_TIME=$TIME" \
      "AYDER_JEPSEN_RUN_ID=${CAMPAIGN_ID}_$i" \
      "AYDER_JEPSEN_ARTIFACT_DIR=$run_dir" \
      "$ROOT_DIR/tests/jepsen/bin/run-local.sh"
    rc=$?
    set -e
  else
    key="reg_${CAMPAIGN_ID}_$i"

    set +e
    RUN_HEARTBEAT_MONITOR_LOG="$run_dir/runner.log" run_with_heartbeat "$i/$RUNS" \
      env \
      "AYDER_JEPSEN_WORKLOAD=$WORKLOAD" \
      "AYDER_JEPSEN_KEY=$key" \
      "AYDER_JEPSEN_TIME=$TIME" \
      "AYDER_JEPSEN_RUN_ID=${CAMPAIGN_ID}_$i" \
      "AYDER_JEPSEN_ARTIFACT_DIR=$run_dir" \
      "$ROOT_DIR/tests/jepsen/bin/run-local.sh"
    rc=$?
    set -e
  fi

  if [[ "$rc" -eq 0 ]]; then
    pass=$((pass + 1))
  else
    fail=$((fail + 1))
  fi

  echo "$i,$WORKLOAD,$TIME,$topic,$group,$key,$rc" >> "$summary_csv"
  echo "run=$i pass=$pass fail=$fail"

  if [[ "$i" -lt "$RUNS" && "$INTER_RUN_SLEEP_SEC" -gt 0 ]]; then
    echo "inter_run_pause run=$i sleep_sec=$INTER_RUN_SLEEP_SEC"
    sleep "$INTER_RUN_SLEEP_SEC"
  fi
done

echo "campaign_pass=$pass campaign_fail=$fail"
echo "campaign_pass=$pass" >> "$CAMPAIGN_DIR/manifest.env"
echo "campaign_fail=$fail" >> "$CAMPAIGN_DIR/manifest.env"

auto_bundle="${AYDER_JEPSEN_AUTO_BUNDLE:-1}"
if [[ "$auto_bundle" == "1" ]]; then
  set +e
  "$ROOT_DIR/tests/jepsen/bin/bundle-artifacts.sh" "$CAMPAIGN_DIR"
  bundle_rc=$?
  set -e
  echo "bundle_rc=$bundle_rc" >> "$CAMPAIGN_DIR/manifest.env"
fi

if [[ "$fail" -gt 0 ]]; then
  exit 1
fi


