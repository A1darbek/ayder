#!/usr/bin/env bash
set -euo pipefail

# ------------------------------
# Config (override via env vars)
# ------------------------------
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.ha5.yml}"
CHAOS_SCRIPT="${CHAOS_SCRIPT:-./scripts/chaos-ha.sh}"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-./artifacts}"
TOPIC="${TOPIC:-np_test}"
PARTITION="${PARTITION:-0}"
TOPIC_PARTITIONS="${TOPIC_PARTITIONS:-1}"

# Auth header for your API (adjust if needed)
AUTH_HEADER="${AUTH_HEADER:-Authorization: Bearer dev}"

# Producer behavior
PRODUCER_INTERVAL_SEC="${PRODUCER_INTERVAL_SEC:-0.20}"
PRODUCER_TIMEOUT_MS="${PRODUCER_TIMEOUT_MS:-200}"
# Default target is node1 so you can clearly observe leader-minority behavior
# You can set PRODUCER_TARGET_PORTS="7001,8001,9001,10001,11001" for round-robin
PRODUCER_TARGET_PORTS="${PRODUCER_TARGET_PORTS:-7001}"

# Timing
BOOT_WAIT_SEC="${BOOT_WAIT_SEC:-90}"
READY_WAIT_SEC="${READY_WAIT_SEC:-120}"
WARMUP_SEC="${WARMUP_SEC:-8}"
PARTITION_HOLD_SEC="${PARTITION_HOLD_SEC:-15}"
POST_HEAL_SEC="${POST_HEAL_SEC:-15}"

# Build behavior
BUILD_ON_RUN="${BUILD_ON_RUN:-0}"

# If set to 1, bring cluster down after suite finishes
DOWN_AFTER="${DOWN_AFTER:-0}"

# ------------------------------
# Fixed ports/nodes
# ------------------------------
HTTP_PORTS=(7001 8001 9001 10001 11001)
NODES=(node1 node2 node3 node4 node5)
CONTAINERS=(ayder-node1 ayder-node2 ayder-node3 ayder-node4 ayder-node5)

# ------------------------------
# Globals
# ------------------------------
RUN_TS="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="${ARTIFACT_ROOT}/chaos_${RUN_TS}"
LOG_DIR="${OUT_DIR}/logs"
MET_DIR="${OUT_DIR}/metrics"
SYS_DIR="${OUT_DIR}/system"
PRODUCER_DIR="${OUT_DIR}/producer"
SUITE_LOG="${OUT_DIR}/suite.log"
PRODUCER_PID=""

mkdir -p "$LOG_DIR" "$MET_DIR" "$SYS_DIR" "$PRODUCER_DIR"

# ------------------------------
# Helpers
# ------------------------------
log() {
  local msg="[$(date '+%F %T')] $*"
  echo "$msg" | tee -a "$SUITE_LOG"
}

die() {
  log "ERROR: $*"
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing command: $1"
}

curl_json() {
  # usage: curl_json URL [extra curl args...]
  local url="$1"; shift || true
  curl -sS --max-time 3 "$url" "$@"
}

wait_http_ok() {
  local port="$1"
  local deadline=$(( $(date +%s) + BOOT_WAIT_SEC ))
  while (( $(date +%s) < deadline )); do
    if curl -fsS --max-time 2 "http://127.0.0.1:${port}/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_ready_true() {
  local port="$1"
  local deadline=$(( $(date +%s) + READY_WAIT_SEC ))
  while (( $(date +%s) < deadline )); do
    local body
    body="$(curl -sS --max-time 2 "http://127.0.0.1:${port}/ready" 2>/dev/null || true)"
    if echo "$body" | grep -Eq '"ready"[[:space:]]*:[[:space:]]*true'; then
      return 0
    fi
    sleep 1
  done
  return 1
}

assert_ha_peering() {
  # Best-effort warning check: at least one follower should show HB received > 0
  local ok=0
  local p hb
  for p in 8001 9001 10001 11001; do
    hb="$(curl -sS --max-time 2 "http://127.0.0.1:${p}/metrics_ha" 2>/dev/null \
      | awk '/^ramforge_ha_heartbeats_received_total / {print $2; exit}' || true)"
    hb="${hb:-0}"
    if [[ "$hb" != "0" && "$hb" != "0.0" ]]; then
      ok=1
      break
    fi
  done
  [[ $ok -eq 1 ]]
}

snapshot_http() {
  # usage: snapshot_http <label>
  local label="$1"
  local dir="${MET_DIR}/${label}"
  local topic_safe
  topic_safe="$(printf '%s' "$TOPIC" | sed 's/[^A-Za-z0-9_.-]/_/g')"
  mkdir -p "$dir"

  log "Collecting HTTP snapshots: ${label}"
  for p in "${HTTP_PORTS[@]}"; do
    local ndir="${dir}/port_${p}"
    mkdir -p "$ndir"

    {
      echo "GET /health"
      curl_json "http://127.0.0.1:${p}/health" || true
      echo
    } > "${ndir}/health.json"

    {
      echo "GET /ready"
      curl_json "http://127.0.0.1:${p}/ready" || true
      echo
    } > "${ndir}/ready.json"

    {
      echo "GET /admin/sealed/status"
      curl_json "http://127.0.0.1:${p}/admin/sealed/status" || true
      echo
    } > "${ndir}/sealed_status.json"

    {
      echo "GET /metrics_ha"
      curl_json "http://127.0.0.1:${p}/metrics_ha" || true
      echo
    } > "${ndir}/metrics_ha.txt"

    # Topic/partition-related snapshots (best effort; endpoints may vary by build/version)
    {
      echo "GET /broker/topics"
      curl_json "http://127.0.0.1:${p}/broker/topics" -H "$AUTH_HEADER" || true
      echo
    } > "${ndir}/topics_list.json"

    {
      echo "GET /broker/topics/${TOPIC}"
      curl_json "http://127.0.0.1:${p}/broker/topics/${TOPIC}" -H "$AUTH_HEADER" || true
      echo
    } > "${ndir}/topic_${topic_safe}.json"

    {
      echo "GET /broker/topics/${TOPIC}/partitions/${PARTITION}"
      curl_json "http://127.0.0.1:${p}/broker/topics/${TOPIC}/partitions/${PARTITION}" -H "$AUTH_HEADER" || true
      echo
    } > "${ndir}/topic_${topic_safe}_partition_${PARTITION}.json"

  done
}

collect_docker_logs() {
  log "Collecting docker logs"
  for c in "${CONTAINERS[@]}"; do
    docker logs --timestamps "$c" > "${LOG_DIR}/${c}.log" 2>&1 || true
  done
}

collect_system_state() {
  log "Collecting docker/system state"

  {
    echo "# date"
    date -Is
    echo
    echo "# docker compose ps"
    docker compose -f "$COMPOSE_FILE" ps || true
    echo
    echo "# docker ps"
    docker ps || true
  } > "${SYS_DIR}/docker_state.txt"

  {
    echo "# chaos status"
    "$CHAOS_SCRIPT" status || true
  } > "${SYS_DIR}/chaos_status.txt" 2>&1

  {
    echo "# chaos metrics helper"
    "$CHAOS_SCRIPT" metrics || true
  } > "${SYS_DIR}/chaos_metrics_helper.txt" 2>&1

  {
    echo "# docker network inspect (compose-created network for node1)"
    NET_NAME="$(
      docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{println $k}}{{end}}' ayder-node1 2>/dev/null \
        | head -n1
    )"
    echo "resolved_network=${NET_NAME:-<none>}"
    if [[ -n "${NET_NAME:-}" ]]; then
      docker network inspect "$NET_NAME" || true
    else
      echo "node1 container not found or no network attached"
    fi
  } > "${SYS_DIR}/network_inspect_compose_network.json" 2>&1
}

create_topic() {
  log "Creating topic '${TOPIC}' (partitions=${TOPIC_PARTITIONS})"
  local body
  body="$(curl -sS -X POST "http://127.0.0.1:7001/broker/topics" \
    -H "$AUTH_HEADER" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"${TOPIC}\",\"partitions\":${TOPIC_PARTITIONS}}" || true)"

  echo "$body" > "${OUT_DIR}/create_topic_response.json"
  log "Create topic response: $body"

  # don't fail hard if topic already exists / different response format
  if [[ -z "$body" ]]; then
    die "No response from topic creation endpoint"
  fi
}

start_producer_loop() {
  local outfile="${PRODUCER_DIR}/producer_responses.ndjson"
  local errfile="${PRODUCER_DIR}/producer_errors.log"
  local meta="${PRODUCER_DIR}/producer_meta.txt"

  IFS=',' read -r -a TARGET_PORTS <<< "$PRODUCER_TARGET_PORTS"

  {
    echo "started_at=$(date -Is)"
    echo "topic=${TOPIC}"
    echo "partition=${PARTITION}"
    echo "producer_interval_sec=${PRODUCER_INTERVAL_SEC}"
    echo "producer_timeout_ms=${PRODUCER_TIMEOUT_MS}"
    echo "target_ports=${PRODUCER_TARGET_PORTS}"
    echo "auth_header=${AUTH_HEADER}"
  } > "$meta"

  log "Starting producer loop in background -> ${outfile}"
  (
    set +e
    idx=0
    while true; do
      local_ts_ms="$(date +%s%3N)"
      local_ts_iso="$(date -Is)"
      count="${#TARGET_PORTS[@]}"
      port="${TARGET_PORTS[$((idx % count))]}"
      idx=$((idx + 1))

      payload="msg-${local_ts_ms}"
      url="http://127.0.0.1:${port}/broker/topics/${TOPIC}/produce?partition=${PARTITION}&timeout_ms=${PRODUCER_TIMEOUT_MS}&timing=1"

      # capture HTTP code + body
      resp_file="$(mktemp)"
      code="$(curl -sS -o "$resp_file" -w '%{http_code}' \
        -X POST "$url" \
        -H "$AUTH_HEADER" \
        --data "$payload" 2>>"$errfile")"
      rc=$?

      if [[ $rc -ne 0 ]]; then
        printf '{"ts":"%s","ts_ms":%s,"target_port":%s,"curl_rc":%s,"http_code":null,"ok":false,"error":"curl_failed"}\n' \
          "$local_ts_iso" "$local_ts_ms" "$port" "$rc" >> "$outfile"
      else
        # Write valid NDJSON (one JSON object per line), embedding parsed response if JSON
        python3 - "$local_ts_iso" "$local_ts_ms" "$port" "$code" "$payload" "$resp_file" >> "$outfile" <<'PY'
import json, sys

ts, ts_ms, port, code, payload, resp_path = sys.argv[1:]
try:
    body_raw = open(resp_path, "r", encoding="utf-8", errors="replace").read()
except Exception:
    body_raw = ""

obj = {
    "ts": ts,
    "ts_ms": int(ts_ms),
    "target_port": int(port),
    "curl_rc": 0,
    "http_code": int(code) if code.isdigit() else code,
    "payload": payload,
    "ok": False,
    "response_raw": body_raw,
}

try:
    body_json = json.loads(body_raw)
    obj["response"] = body_json
    if isinstance(body_json, dict) and "ok" in body_json:
        obj["ok"] = bool(body_json["ok"])
    else:
        obj["ok"] = isinstance(obj["http_code"], int) and (200 <= obj["http_code"] < 300)
except Exception:
    obj["ok"] = isinstance(obj["http_code"], int) and (200 <= obj["http_code"] < 300)

print(json.dumps(obj, separators=(",", ":")))
PY
      fi

      rm -f "$resp_file"
      sleep "$PRODUCER_INTERVAL_SEC"
    done
  ) &
  PRODUCER_PID="$!"

  echo "$PRODUCER_PID" > "${PRODUCER_DIR}/producer.pid"
  log "Producer PID=${PRODUCER_PID}"
}

stop_producer_loop() {
  if [[ -n "${PRODUCER_PID:-}" ]] && kill -0 "$PRODUCER_PID" 2>/dev/null; then
    log "Stopping producer loop PID=${PRODUCER_PID}"
    kill "$PRODUCER_PID" 2>/dev/null || true
    wait "$PRODUCER_PID" 2>/dev/null || true
  fi
}

cleanup() {
  local rc=$?
  log "Cleanup (exit code=${rc})"
  stop_producer_loop || true
  collect_docker_logs || true
  collect_system_state || true
  if [[ "$DOWN_AFTER" == "1" ]]; then
    log "Bringing cluster down (DOWN_AFTER=1)"
    docker compose -f "$COMPOSE_FILE" down || true
  fi
  if [ -x ./scripts/render-chaos-report.sh ]; then
    ./scripts/render-chaos-report.sh "$OUT_DIR" || true
  fi
  log "Artifacts saved to: ${OUT_DIR}"
  exit "$rc"
}
trap cleanup EXIT INT TERM

# ------------------------------
# Preflight
# ------------------------------
need_cmd docker
need_cmd curl
need_cmd bash
need_cmd python3

[[ -f "$COMPOSE_FILE" ]] || die "Compose file not found: $COMPOSE_FILE"
[[ -x "$CHAOS_SCRIPT" ]] || die "Chaos script not found/executable: $CHAOS_SCRIPT"

# ------------------------------
# Run suite
# ------------------------------
log "=== Ayder HA chaos suite start ==="
log "COMPOSE_FILE=${COMPOSE_FILE}"
log "CHAOS_SCRIPT=${CHAOS_SCRIPT}"
log "OUT_DIR=${OUT_DIR}"

# Make sure no stale chaos rules remain from previous runs
log "Healing any previous chaos state"
"$CHAOS_SCRIPT" heal || true

log "Booting cluster"
if [[ "$BUILD_ON_RUN" == "1" ]]; then
  docker compose -f "$COMPOSE_FILE" up --build -d | tee -a "$SUITE_LOG"
else
  docker compose -f "$COMPOSE_FILE" up -d | tee -a "$SUITE_LOG"
fi

# Wait for health on all ports
for p in "${HTTP_PORTS[@]}"; do
  log "Waiting /health on :${p}"
  wait_http_ok "$p" || die "Timeout waiting for /health on port ${p}"
done

# Wait for ready on all ports (best effort but fail if never ready)
for p in "${HTTP_PORTS[@]}"; do
  log "Waiting /ready on :${p}"
  wait_ready_true "$p" || die "Timeout waiting for /ready on port ${p}"
done

collect_system_state
snapshot_http "pre_topic"

create_topic

start_producer_loop

log "Warmup for ${WARMUP_SEC}s"
sleep "$WARMUP_SEC"

if ! assert_ha_peering; then
  log "WARNING: followers still show zero heartbeats_received_total before partition"
fi

snapshot_http "pre_partition"
collect_system_state

log "Applying leader-minority partition (node1,node2 vs node3,node4,node5)"
"$CHAOS_SCRIPT" leader-minority | tee -a "$SUITE_LOG"

log "Holding partition for ${PARTITION_HOLD_SEC}s"
sleep "$PARTITION_HOLD_SEC"

snapshot_http "during_partition"
collect_system_state

log "Healing partition"
"$CHAOS_SCRIPT" heal | tee -a "$SUITE_LOG"

log "Post-heal stabilization for ${POST_HEAL_SEC}s"
sleep "$POST_HEAL_SEC"

snapshot_http "post_heal"
collect_system_state

# Optional simple summary from producer results (valid NDJSON-aware)
log "Generating quick producer summary"
python3 - "${PRODUCER_DIR}/producer_responses.ndjson" > "${PRODUCER_DIR}/summary.txt" 2>/dev/null <<'PY' || true
import json, sys
from collections import Counter
p = sys.argv[1]
total = ok = fail = parse_err = 0
http_codes = Counter()
ports = Counter()
try:
    with open(p, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                obj = json.loads(line)
            except Exception:
                parse_err += 1
                continue
            if isinstance(obj, dict):
                if obj.get("ok") is True:
                    ok += 1
                elif obj.get("ok") is False:
                    fail += 1
                hc = obj.get("http_code")
                if hc is not None:
                    http_codes[str(hc)] += 1
                tp = obj.get("target_port")
                if tp is not None:
                    ports[str(tp)] += 1
except FileNotFoundError:
    pass

print(f"total_lines={total}")
print(f"ok_true={ok}")
print(f"ok_false={fail}")
print(f"json_parse_errors={parse_err}")
if http_codes:
    print("http_code_counts=" + ",".join(f"{k}:{v}" for k, v in sorted(http_codes.items())))
if ports:
    print("target_port_counts=" + ",".join(f"{k}:{v}" for k, v in sorted(ports.items(), key=lambda kv: int(kv[0]))))
PY

log "=== Ayder HA chaos suite completed ==="