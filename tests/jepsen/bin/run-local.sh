#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
JEPSEN_DIR="$ROOT_DIR/tests/jepsen"
# shellcheck disable=SC1091
source "$JEPSEN_DIR/bin/_toolchain.sh"

log() {
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

handle_interrupt() {
  local sig="${1:-TERM}"
  log "run_local interrupted by signal=$sig"
  if [[ -n "${ART_DIR:-}" ]]; then
    mkdir -p "$ART_DIR"
    [[ -f "$ART_DIR/exit_code.txt" ]] || echo "124" > "$ART_DIR/exit_code.txt"
    if [[ -f "$ART_DIR/manifest.env" ]] && ! grep -q '^run_status=' "$ART_DIR/manifest.env"; then
      echo 'run_status=interrupted' >> "$ART_DIR/manifest.env"
    fi
  fi
  exit 124
}

if ! jepsen_require_toolchain "$ROOT_DIR"; then
  exit 1
fi

csv_to_lines() {
  tr ',' '\n' <<<"$1" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e '/^$/d'
}

curl_auth() {
  local url="$1"
  local timeout_sec="$2"
  if [[ -n "${AYDER_JEPSEN_TOKEN:-}" ]]; then
    curl -fsS --connect-timeout "$timeout_sec" --max-time "$timeout_sec" \
      -H "Authorization: Bearer ${AYDER_JEPSEN_TOKEN}" "$url"
  else
    curl -fsS --connect-timeout "$timeout_sec" --max-time "$timeout_sec" "$url"
  fi
}

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

resolve_pre_heal_cmd() {
  local configured_heal="${AYDER_JEPSEN_HEAL_CMD:-}"
  if [[ -n "${AYDER_JEPSEN_PRE_HEAL_CMD:-}" ]]; then
    PRE_HEAL_CMD="$AYDER_JEPSEN_PRE_HEAL_CMD"
  elif [[ -n "$configured_heal" ]]; then
    PRE_HEAL_CMD="$configured_heal"
  else
    PRE_HEAL_CMD="$DEFAULT_HEAL_CMD"
  fi
}

run_pre_heal() {
  local cmd="$1"
  local enabled="${AYDER_JEPSEN_PRE_HEAL_ENABLED:-1}"

  if [[ "$enabled" != '1' ]]; then
    log 'pre-run heal disabled (AYDER_JEPSEN_PRE_HEAL_ENABLED!=1)'
    return 0
  fi

  if [[ "${AYDER_JEPSEN_NEMESIS:-true}" != 'true' ]]; then
    log 'pre-run heal skipped (nemesis disabled)'
    return 0
  fi

  if [[ -z "$cmd" || "$cmd" == ':' ]]; then
    log 'pre-run heal skipped (no-op command)'
    return 0
  fi

  log "pre-run heal start"
  log "pre-run heal cmd: $cmd"

  set +e
  bash -lc "$cmd"
  local rc=$?
  set -e

  if [[ "$rc" -ne 0 ]]; then
    log "ERROR: pre-run heal failed rc=$rc"
    return "$rc"
  fi

  log 'pre-run heal done'
  return 0
}

run_pre_ready_gate() {
  local enabled="${AYDER_JEPSEN_PRE_READY_ENABLED:-1}"
  local nodes_csv="${AYDER_JEPSEN_NODES:-http://127.0.0.1:7001,http://127.0.0.1:8001,http://127.0.0.1:9001,http://127.0.0.1:10001,http://127.0.0.1:11001}"
  local timeout_sec="${AYDER_JEPSEN_PRE_READY_TIMEOUT_SEC:-90}"
  local poll_sec="${AYDER_JEPSEN_PRE_READY_POLL_SEC:-1}"
  local curl_timeout_sec="${AYDER_JEPSEN_PRE_READY_CURL_TIMEOUT_SEC:-1}"
  local require_fq="${AYDER_JEPSEN_PRE_READY_REQUIRE_FRESH_QUORUM:-1}"
  local recovery_cmd="${AYDER_JEPSEN_PRE_READY_RECOVERY_CMD:-}"
  local recovery_after_sec="${AYDER_JEPSEN_PRE_READY_RECOVERY_AFTER_SEC:-45}"
  local recovery_timeout_sec="${AYDER_JEPSEN_PRE_READY_RECOVERY_TIMEOUT_SEC:-120}"
  local recovery_attempted=0

  if [[ "$enabled" != '1' ]]; then
    log 'pre-run readiness disabled (AYDER_JEPSEN_PRE_READY_ENABLED!=1)'
    return 0
  fi

  [[ "$timeout_sec" =~ ^[0-9]+$ ]] || timeout_sec=90
  (( timeout_sec > 0 )) || timeout_sec=90
  [[ "$poll_sec" =~ ^[0-9]+$ ]] || poll_sec=1
  (( poll_sec > 0 )) || poll_sec=1
  [[ "$curl_timeout_sec" =~ ^[0-9]+$ ]] || curl_timeout_sec=1
  (( curl_timeout_sec > 0 )) || curl_timeout_sec=1

  local node_count=0
  while IFS= read -r _u; do
    node_count=$((node_count + 1))
  done < <(csv_to_lines "$nodes_csv")

  if (( node_count == 0 )); then
    log 'pre-run readiness skipped (no nodes configured)'
    return 0
  fi

  local default_min=$((node_count / 2 + 1))
  local min_healthy="${AYDER_JEPSEN_PRE_READY_MIN_HEALTHY_NODES:-$default_min}"
  [[ "$min_healthy" =~ ^[0-9]+$ ]] || min_healthy="$default_min"
  (( min_healthy < 1 )) && min_healthy=1
  (( min_healthy > node_count )) && min_healthy="$node_count"

  log "pre-run readiness start timeout=${timeout_sec}s min_healthy=${min_healthy}/${node_count} require_fresh_quorum=${require_fq}"

  local start now last_log
  start="$(date +%s)"
  last_log=$((start - 5))

  while true; do
    local healthy=0
    local leader_fq=0

    while IFS= read -r url; do
      local base="${url%/}"
      if curl_auth "$base/health" "$curl_timeout_sec" >/dev/null 2>&1; then
        healthy=$((healthy + 1))
        if [[ "$require_fq" == '1' && "$leader_fq" -eq 0 ]]; then
          local metrics role fq
          metrics="$(curl_auth "$base/metrics_ha" "$curl_timeout_sec" 2>/dev/null || true)"
          if [[ -n "$metrics" ]]; then
            role="$(awk '/^ramforge_ha_role /{print $2; exit}' <<<"$metrics")"
            fq="$(awk '/^ramforge_ha_fresh_quorum /{print $2; exit}' <<<"$metrics")"
            if [[ "$role" == '0' && "$fq" == '1' ]]; then
              leader_fq=1
            fi
          fi
        fi
      fi
    done < <(csv_to_lines "$nodes_csv")

    now="$(date +%s)"

    if (( healthy >= min_healthy )); then
      if [[ "$require_fq" != '1' || "$leader_fq" -eq 1 ]]; then
        log "pre-run readiness ok healthy=${healthy}/${node_count} leader_fresh_quorum=${leader_fq}"
        return 0
      fi
    fi

    if [[ -n "$recovery_cmd" && "$recovery_attempted" -eq 0 && $((now - start)) -ge "$recovery_after_sec" ]]; then
      local recovery_reason=""
      if (( healthy < min_healthy )); then
        recovery_reason='healthy_below_min'
      elif [[ "$require_fq" == '1' && "$leader_fq" -eq 0 ]]; then
        recovery_reason='leader_fresh_quorum_zero'
      fi

      if [[ -n "$recovery_reason" ]]; then
        log "pre-run readiness recovery start reason=$recovery_reason cmd=$recovery_cmd"
        set +e
        if command -v timeout >/dev/null 2>&1; then
          timeout "${recovery_timeout_sec}s" bash -lc "$recovery_cmd"
          recovery_rc=$?
        else
          bash -lc "$recovery_cmd"
          recovery_rc=$?
        fi
        set -e
        recovery_attempted=1

        if [[ "$recovery_rc" -ne 0 ]]; then
          log "ERROR: pre-run readiness recovery failed rc=$recovery_rc"
        else
          log 'pre-run readiness recovery done'
        fi
      fi
    fi

    if (( now - start >= timeout_sec )); then
      log "ERROR: pre-run readiness timeout healthy=${healthy}/${node_count} leader_fresh_quorum=${leader_fq}"
      return 1
    fi

    if (( now - last_log >= 5 )); then
      log "pre-run readiness pending healthy=${healthy}/${node_count} leader_fresh_quorum=${leader_fq}"
      last_log="$now"
    fi

    sleep "$poll_sec"
  done
}

DEFAULT_KILL_CMD='pids=$(pgrep -f "[a]yder --port 9001" || true); for p in $pids; do kill -9 "$p" 2>/dev/null || true; done; true'
DEFAULT_RESTART_CMD="cd '$ROOT_DIR/cluster/node3' && source '$ROOT_DIR/tests/demo/ha6_stability_env_preset.sh' node3 && setsid '$ROOT_DIR/ayder' --port 9001 --workers 4 > /tmp/ayder-node3-jepsen.log 2>&1 < /dev/null & disown || true"

RUN_ID="${AYDER_JEPSEN_RUN_ID:-$(date +%Y%m%dT%H%M%S%N)}"
RESULTS_ROOT="${AYDER_JEPSEN_RESULTS_ROOT:-$ROOT_DIR/tests/jepsen/results}"
ART_DIR="${AYDER_JEPSEN_ARTIFACT_DIR:-$RESULTS_ROOT/run_$RUN_ID}"
RUNNER_STREAM_STDOUT="${AYDER_JEPSEN_RUNNER_STREAM_STDOUT:-0}"

resolve_pre_heal_cmd
mkdir -p "$ART_DIR"
trap 'handle_interrupt TERM' TERM
trap 'handle_interrupt INT' INT

GIT_SHA="$(git -C "$ROOT_DIR" rev-parse --verify HEAD 2>/dev/null || echo unknown)"
if GIT_DIRTY_RAW="$(git -C "$ROOT_DIR" status --porcelain 2>/dev/null)"; then
  GIT_DIRTY_COUNT="$(awk 'NF{c++} END{print c+0}' <<<"$GIT_DIRTY_RAW")"
else
  GIT_DIRTY_COUNT='unknown'
fi

{
  echo "run_id=$RUN_ID"
  echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "root_dir=$ROOT_DIR"
  echo "artifact_dir=$ART_DIR"
  echo "git_sha=$GIT_SHA"
  echo "git_dirty_count=$GIT_DIRTY_COUNT"
  echo "workload=${AYDER_JEPSEN_WORKLOAD:-broker-log}"
  echo "nodes=${AYDER_JEPSEN_NODES:-http://127.0.0.1:7001,http://127.0.0.1:8001,http://127.0.0.1:9001,http://127.0.0.1:10001,http://127.0.0.1:11001}"
  echo "direct_url=${AYDER_JEPSEN_DIRECT_URL:-}"
  echo "http_socket_timeout_ms=${AYDER_JEPSEN_HTTP_SOCKET_TIMEOUT_MS:-}"
  echo "http_conn_timeout_ms=${AYDER_JEPSEN_HTTP_CONN_TIMEOUT_MS:-}"
  echo "broker_produce_timeout_ms=${AYDER_JEPSEN_BROKER_PRODUCE_TIMEOUT_MS:-}"
  echo "broker_read_timeout_ms=${AYDER_JEPSEN_BROKER_READ_TIMEOUT_MS:-}"
  echo "nemesis_interval_sec=${AYDER_JEPSEN_NEMESIS_INTERVAL:-6}"
  echo "nemesis_startup_sec=${AYDER_JEPSEN_NEMESIS_STARTUP_SEC:-10}"
  echo "default_partition_cmd=$DEFAULT_PARTITION_CMD"
  echo "default_heal_cmd=$DEFAULT_HEAL_CMD"
  echo "pre_heal_enabled=${AYDER_JEPSEN_PRE_HEAL_ENABLED:-1}"
  echo "pre_heal_cmd=$PRE_HEAL_CMD"
  echo "pre_ready_enabled=${AYDER_JEPSEN_PRE_READY_ENABLED:-1}"
  echo "pre_ready_timeout_sec=${AYDER_JEPSEN_PRE_READY_TIMEOUT_SEC:-90}"
  echo "pre_ready_poll_sec=${AYDER_JEPSEN_PRE_READY_POLL_SEC:-1}"
  echo "pre_ready_curl_timeout_sec=${AYDER_JEPSEN_PRE_READY_CURL_TIMEOUT_SEC:-1}"
  echo "pre_ready_require_fresh_quorum=${AYDER_JEPSEN_PRE_READY_REQUIRE_FRESH_QUORUM:-1}"
  echo "pre_ready_min_healthy_nodes=${AYDER_JEPSEN_PRE_READY_MIN_HEALTHY_NODES:-}"
  echo "pre_ready_recovery_after_sec=${AYDER_JEPSEN_PRE_READY_RECOVERY_AFTER_SEC:-45}"
  echo "pre_ready_recovery_timeout_sec=${AYDER_JEPSEN_PRE_READY_RECOVERY_TIMEOUT_SEC:-120}"
  echo "pre_ready_recovery_cmd=${AYDER_JEPSEN_PRE_READY_RECOVERY_CMD:-}"
  echo "runner_stream_stdout=$RUNNER_STREAM_STDOUT"
} > "$ART_DIR/manifest.env"

set +e
run_pre_heal "$PRE_HEAL_CMD"
pre_heal_rc=$?
set -e

echo "pre_heal_rc=$pre_heal_rc" >> "$ART_DIR/manifest.env"
if [[ "$pre_heal_rc" -ne 0 ]]; then
  echo "$pre_heal_rc" > "$ART_DIR/exit_code.txt"
  echo 'run_status=fail_pre_heal' >> "$ART_DIR/manifest.env"
  log "run_local done run_id=$RUN_ID rc=$pre_heal_rc artifact_dir=$ART_DIR"
  echo "artifact_dir=$ART_DIR"
  exit "$pre_heal_rc"
fi

set +e
run_pre_ready_gate
pre_ready_rc=$?
set -e

echo "pre_ready_rc=$pre_ready_rc" >> "$ART_DIR/manifest.env"
if [[ "$pre_ready_rc" -ne 0 ]]; then
  echo "$pre_ready_rc" > "$ART_DIR/exit_code.txt"
  echo 'run_status=fail_pre_ready' >> "$ART_DIR/manifest.env"
  log "run_local done run_id=$RUN_ID rc=$pre_ready_rc artifact_dir=$ART_DIR"
  echo "artifact_dir=$ART_DIR"
  exit "$pre_ready_rc"
fi

cd "$JEPSEN_DIR"

cmd=(
  lein run
  --
  --nodes "${AYDER_JEPSEN_NODES:-http://127.0.0.1:7001,http://127.0.0.1:8001,http://127.0.0.1:9001,http://127.0.0.1:10001,http://127.0.0.1:11001}"
  --token "${AYDER_JEPSEN_TOKEN:-dev}"
  --workload "${AYDER_JEPSEN_WORKLOAD:-broker-log}"
  --topic "${AYDER_JEPSEN_TOPIC:-jepsen_log_$RUN_ID}"
  --group "${AYDER_JEPSEN_GROUP:-jepsen_g_$RUN_ID}"
  --partition "${AYDER_JEPSEN_PARTITION:-0}"
  --topic-partitions "${AYDER_JEPSEN_TOPIC_PARTITIONS:-1}"
  --direct-url "${AYDER_JEPSEN_DIRECT_URL:-}"
  --client-stagger-ms "${AYDER_JEPSEN_CLIENT_STAGGER_MS:-10}"
  --linearizable-history-mode "${AYDER_JEPSEN_LINEARIZABLE_HISTORY_MODE:-all}"
  --namespace "${AYDER_JEPSEN_NS:-jepsen}"
  --key "${AYDER_JEPSEN_KEY:-reg}"
  --time-limit-sec "${AYDER_JEPSEN_TIME:-120}"
  --nemesis-enabled "${AYDER_JEPSEN_NEMESIS:-true}"
  --nemesis-interval-sec "${AYDER_JEPSEN_NEMESIS_INTERVAL:-6}"
  --nemesis-startup-sec "${AYDER_JEPSEN_NEMESIS_STARTUP_SEC:-10}"
  --partition-cmd "${AYDER_JEPSEN_PARTITION_CMD:-$DEFAULT_PARTITION_CMD}"
  --heal-cmd "${AYDER_JEPSEN_HEAL_CMD:-$DEFAULT_HEAL_CMD}"
  --kill-cmd "${AYDER_JEPSEN_KILL_CMD:-$DEFAULT_KILL_CMD}"
  --restart-cmd "${AYDER_JEPSEN_RESTART_CMD:-$DEFAULT_RESTART_CMD}"
  --artifact-dir "$ART_DIR"
  --run-id "$RUN_ID"
)

printf '%q ' "${cmd[@]}" > "$ART_DIR/command.sh"
printf '\n' >> "$ART_DIR/command.sh"
chmod +x "$ART_DIR/command.sh"

runner_cmd=("${cmd[@]}")
if command -v stdbuf >/dev/null 2>&1; then
  runner_cmd=(stdbuf -oL -eL "${cmd[@]}")
fi

log "run_local start run_id=$RUN_ID workload=${AYDER_JEPSEN_WORKLOAD:-broker-log} artifact_dir=$ART_DIR"
log "runner command recorded at $ART_DIR/command.sh"

set +e
if [[ "$RUNNER_STREAM_STDOUT" == '1' ]]; then
  "${runner_cmd[@]}" 2>&1 | tee "$ART_DIR/runner.log"
  rc=${PIPESTATUS[0]}
else
  "${runner_cmd[@]}" > "$ART_DIR/runner.log" 2>&1
  rc=$?
fi
set -e

echo "$rc" > "$ART_DIR/exit_code.txt"

if [[ "$rc" -eq 0 ]]; then
  echo 'run_status=pass' >> "$ART_DIR/manifest.env"
elif [[ "$rc" -eq 137 ]]; then
  echo 'run_status=fail_oom' >> "$ART_DIR/manifest.env"
  echo 'oom_suspected=1' >> "$ART_DIR/manifest.env"
else
  echo 'run_status=fail' >> "$ART_DIR/manifest.env"
fi

log "run_local done run_id=$RUN_ID rc=$rc artifact_dir=$ART_DIR"
echo "artifact_dir=$ART_DIR"
exit "$rc"
