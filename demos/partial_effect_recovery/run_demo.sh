#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

AYDER_PORT="${AYDER_PORT:-1126}"
export AYDER_BASE="${AYDER_BASE:-http://127.0.0.1:${AYDER_PORT}}"

# shellcheck source=demos/partial_effect_recovery/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd bash
require_cmd curl
require_cmd jq
require_cmd "$DOCKER_BIN"

SCENARIO="${SCENARIO:-partial_missing_notification}"
RUN_ID="partial_effect_recovery_${SCENARIO}_$(date -u +%Y%m%dT%H%M%SZ)"
ARTIFACT_DIR="${ARTIFACT_DIR:-${REPO_ROOT}/artifacts/partial_effect_recovery/${RUN_ID}}"
mkdir -p "$ARTIFACT_DIR"

START_AYDER="${START_AYDER:-1}"
AYDER_BIN="${AYDER_BIN:-${REPO_ROOT}/ayder}"
AYDER_USE_DOCKER="${AYDER_USE_DOCKER:-0}"
AYDER_DOCKER_IMAGE="${AYDER_DOCKER_IMAGE:-}"
AYDER_DOCKER_CONTAINER="${AYDER_DOCKER_CONTAINER:-}"
AYDER_WORKERS="${AYDER_WORKERS:-2}"
AYDER_AOF="${AYDER_AOF-}"
AYDER_PID_FILE="${ARTIFACT_DIR}/ayder.pid"
AYDER_STDOUT_LOG="${ARTIFACT_DIR}/ayder.log"
AYDER_STOP_FILE="${ARTIFACT_DIR}/ayder.stop"
export ARTIFACT_DIR OUT_DIR="${ARTIFACT_DIR}/receipt" SCENARIO

cleanup() {
  set +e
  : >"$AYDER_STOP_FILE" 2>/dev/null || true
  if [[ -n "${AYDER_MGR_PID:-}" ]]; then kill "$AYDER_MGR_PID" >/dev/null 2>&1 || true; fi
  if [[ -f "$AYDER_PID_FILE" ]]; then
    pid="$(cat "$AYDER_PID_FILE" 2>/dev/null || true)"
    if [[ -n "$pid" ]]; then kill "$pid" >/dev/null 2>&1 || true; fi
  fi
  if [[ -n "${AYDER_DOCKER_CONTAINER:-}" ]]; then
    "$DOCKER_BIN" rm -f "$AYDER_DOCKER_CONTAINER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_ayder() {
  local retries="${1:-120}"
  local i
  for ((i = 1; i <= retries; i++)); do
    if curl -fsS "${AYDER_BASE}/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  return 1
}

ensure_ayder_runtime() {
  [[ "$START_AYDER" == "1" ]] || return 0
  [[ -x "$AYDER_BIN" ]] && return 0

  if [[ "${BOOTSTRAP_AYDER_DOCKER:-1}" != "1" ]]; then
    echo "ayder binary not executable: $AYDER_BIN" >&2
    echo "Run 'make' or set BOOTSTRAP_AYDER_DOCKER=1 after 'docker compose up -d --build'." >&2
    exit 1
  fi

  log "local ayder binary missing; using Docker image for demo broker"
  AYDER_USE_DOCKER=1

  local root_compose="${REPO_ROOT}/docker-compose.yml"
  local compose_project="${COMPOSE_PROJECT_NAME:-$(basename "$REPO_ROOT")}"
  local compose_image="${compose_project}-ayder:latest"
  AYDER_DOCKER_IMAGE="$("$DOCKER_BIN" compose -f "$root_compose" images -q ayder 2>/dev/null | head -n 1 || true)"
  if [[ -z "$AYDER_DOCKER_IMAGE" ]] && "$DOCKER_BIN" image inspect "$compose_image" >/dev/null 2>&1; then
    AYDER_DOCKER_IMAGE="$compose_image"
  fi
  if [[ -z "$AYDER_DOCKER_IMAGE" ]]; then
    log "building Docker Compose ayder image"
    "$DOCKER_BIN" compose -f "$root_compose" build ayder >/dev/null
    AYDER_DOCKER_IMAGE="$("$DOCKER_BIN" compose -f "$root_compose" images -q ayder 2>/dev/null | head -n 1 || true)"
    if [[ -z "$AYDER_DOCKER_IMAGE" ]] && "$DOCKER_BIN" image inspect "$compose_image" >/dev/null 2>&1; then
      AYDER_DOCKER_IMAGE="$compose_image"
    fi
  fi

  if [[ -z "$AYDER_DOCKER_IMAGE" ]]; then
    echo "could not find or build Docker image for service 'ayder'" >&2
    exit 1
  fi
}

start_ayder_manager() {
  if [[ "$AYDER_USE_DOCKER" == "1" ]]; then
    local cname
    cname="ayder-partial-effect-${RUN_ID//[^A-Za-z0-9_.-]/-}"
    AYDER_DOCKER_CONTAINER="$cname"
    mkdir -p "${ARTIFACT_DIR}/ayder_data"
    "$DOCKER_BIN" rm -f "$cname" >/dev/null 2>&1 || true
    "$DOCKER_BIN" run -d --rm \
      --name "$cname" \
      --shm-size 2g \
      --ipc shareable \
      --ulimit memlock=-1:-1 \
      --security-opt seccomp=unconfined \
      -e RF_BEARER_TOKENS="${RF_BEARER_TOKENS:-dev@55555555555555:11111111111111111:111111111111111111111}" \
      -e RF_HTTP_DISABLE_RL="${RF_HTTP_DISABLE_RL:-1}" \
      -v "${ARTIFACT_DIR}/ayder_data:/data" \
      -p "127.0.0.1:${AYDER_PORT}:1109" \
      "$AYDER_DOCKER_IMAGE" >"$AYDER_PID_FILE"
    log "started dockerized ayder container=${cname} host_port=${AYDER_PORT}"
    return 0
  fi

  [[ -x "$AYDER_BIN" ]] || { echo "ayder binary not executable: $AYDER_BIN" >&2; exit 1; }
  local ayder_dir="${ARTIFACT_DIR}/ayder_data"
  mkdir -p "$ayder_dir"
  (
    while true; do
      [[ -f "$AYDER_STOP_FILE" ]] && exit 0
      (
        cd "$ayder_dir" || exit 1
        export RF_BEARER_TOKENS="${RF_BEARER_TOKENS:-dev@55555555555555:11111111111111111:111111111111111111111}"
        export RF_HTTP_DISABLE_RL="${RF_HTTP_DISABLE_RL:-1}"
        args=(--port "$AYDER_PORT" --workers "$AYDER_WORKERS")
        [[ -n "$AYDER_AOF" ]] && args+=(--aof "$AYDER_AOF")
        "$AYDER_BIN" "${args[@]}" >>"$AYDER_STDOUT_LOG" 2>&1
      ) &
      child=$!
      echo "$child" >"$AYDER_PID_FILE"
      wait "$child"
      [[ -f "$AYDER_STOP_FILE" ]] && exit 0
      sleep 1
    done
  ) &
  AYDER_MGR_PID=$!
}

expected_for_scenario() {
  case "$SCENARIO" in
    all_confirmed) echo "GREEN 0" ;;
    partial_missing_notification) echo "YELLOW 10" ;;
    ambiguous_ledger) echo "YELLOW 10" ;;
    fully_missing_event) echo "RED 20" ;;
    duplicate_risk) echo "RED 20" ;;
    *) echo "unknown SCENARIO=${SCENARIO}" >&2; exit 2 ;;
  esac
}

validate_receipt() {
  local receipt="${OUT_DIR}/receipt.json"
  local expected_verdict expected_rc
  read -r expected_verdict expected_rc < <(expected_for_scenario)
  [[ -s "$receipt" ]] || { echo "receipt missing: $receipt" >&2; return 1; }
  [[ "$(jq -r '.scenario' "$receipt")" == "$SCENARIO" ]] || return 1
  [[ "$(jq -r '.verdict' "$receipt")" == "$expected_verdict" ]] || return 1
  [[ "$(jq -r '.ayder_evidence.topic' "$receipt")" == "$TOPIC" ]] || return 1
  [[ "$(jq -r '.ayder_evidence.deterministic_replay_available' "$receipt")" == "true" ]] || return 1

  case "$SCENARIO" in
    all_confirmed)
      [[ "$(jq -r '.effect_summary.confirmed_effects' "$receipt")" == "3" ]] || return 1
      [[ "$(jq -r '.recovery_guidance.safe_next_action' "$receipt")" == "close incident" ]] || return 1
      ;;
    fully_missing_event)
      [[ "$(jq -r '.effect_summary.confirmed_effects' "$receipt")" == "0" ]] || return 1
      [[ "$(jq -r '.recovery_guidance.blind_replay_whole_event' "$receipt")" == "true" ]] || return 1
      ;;
    partial_missing_notification)
      [[ "$(jq -r '.effect_summary.confirmed_effects' "$receipt")" == "2" ]] || return 1
      [[ "$(jq -r '.effect_summary.missing_effects' "$receipt")" == "1" ]] || return 1
      [[ "$(jq -r '.recovery_guidance.repair_only_missing_effect' "$receipt")" == "true" ]] || return 1
      [[ "$(jq -r '.effects[] | select(.name=="customer_notification_sent").safe_to_reapply' "$receipt")" == "true" ]] || return 1
      ;;
    ambiguous_ledger)
      [[ "$(jq -r '.effect_summary.ambiguous_effects' "$receipt")" == "1" ]] || return 1
      [[ "$(jq -r '.recovery_guidance.manual_review_required' "$receipt")" == "true" ]] || return 1
      ;;
    duplicate_risk)
      [[ "$(jq -r '.effect_summary.duplicate_effects' "$receipt")" == "1" ]] || return 1
      [[ "$(jq -r '.recovery_guidance.manual_review_required' "$receipt")" == "true" ]] || return 1
      ;;
  esac
}

log "partial-effect-recovery demo scenario=${SCENARIO} artifact_dir=${ARTIFACT_DIR}"

if [[ "$START_AYDER" == "1" ]]; then
  ensure_ayder_runtime
  start_ayder_manager
  wait_ayder 120 || { echo "ayder did not become healthy at ${AYDER_BASE}" >&2; exit 1; }
fi

"${SCRIPT_DIR}/init.sh" >"${ARTIFACT_DIR}/init.log" 2>&1
"${SCRIPT_DIR}/producer.sh" >"${ARTIFACT_DIR}/producer.log" 2>&1
"${SCRIPT_DIR}/effect_worker.sh" >"${ARTIFACT_DIR}/effect_worker.log" 2>&1

set +e
"${SCRIPT_DIR}/recovery_receipt.sh" >"${ARTIFACT_DIR}/recovery_receipt.log" 2>&1
receipt_rc=$?
set -e

read -r expected_verdict expected_rc < <(expected_for_scenario)
[[ "$receipt_rc" == "$expected_rc" ]] || {
  echo "expected receipt rc=${expected_rc} for ${expected_verdict}, got ${receipt_rc}" >&2
  exit 1
}

validate_receipt

cat >"${ARTIFACT_DIR}/summary.txt" <<EOF
run_id=${RUN_ID}
artifact_dir=${ARTIFACT_DIR}
scenario=${SCENARIO}
topic=${TOPIC}
group=${GROUP}
partition=${PARTITION}
ayder_base=${AYDER_BASE}
receipt_json=${OUT_DIR}/receipt.json
receipt_txt=${OUT_DIR}/receipt.txt
status=pass
EOF

cat "${OUT_DIR}/receipt.txt"
log "partial-effect-recovery demo success"
log "artifacts: ${ARTIFACT_DIR}"
