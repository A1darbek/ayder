#!/usr/bin/env bash
set -Eeuo pipefail

APP_BIN=${APP_BIN:-"./ramforge"}
PORT=${PORT:-1109}
BASE_URL="http://127.0.0.1:${PORT}"

# server auth/rate – inherited by the child process we spawn
export RF_BEARER_TOKENS="${RF_BEARER_TOKENS:-dev@5000:5000:100000}"

# ---------- client auth ----------
TOKEN="${TOKEN:-dev}"
TOKENS="${TOKENS:-}"
first_token(){ [[ -n "${TOKENS}" ]] && printf '%s' "$TOKENS" | tr ';' ',' | cut -d',' -f1 || printf '%s' "$TOKEN"; }
FIRST="$(first_token)"; HDR_AUTH=(); [[ -n "$FIRST" ]] && HDR_AUTH=(-H "Authorization: Bearer ${FIRST}")

# ---------- test params ----------
TOPIC="orders-both-replay-batch-idemp-$RANDOM-$RANDOM"
PART=${PART:-0}
GROUP=${GROUP:-g}

TTL_MS=${TTL_MS:-500}
SLEEP_AFTER_S=${SLEEP_AFTER_S:-0.7}
VIS_TRIES=${VIS_TRIES:-30}
VIS_DELAY_S=${VIS_DELAY_S:-0.02}
GC_TICK_S=${GC_TICK_S:-0.05}
DURABLE_TIMEOUT_MS=${DURABLE_TIMEOUT_MS:-5000}
LOG_FILE=${LOG_FILE:-"./retention_both_replay_batch_idemp.log"}

# idempotency keys scoped to this run's topic (avoid stale collisions)
IDK1="BK1-${TOPIC}"
IDK2="BK2-${TOPIC}"

post(){ curl -fsS -H 'Content-Type: application/json' "${HDR_AUTH[@]}" -X POST "$BASE_URL$1" -d "$2"; }
get(){  curl -fsS "${HDR_AUTH[@]}" "$BASE_URL$1"; }
die(){  echo "❌ $*" >&2; exit 1; }

APP_PID=""; PGID=""
start_cluster(){ setsid "${APP_BIN}" --port "${PORT}" > "${LOG_FILE}" 2>&1 & APP_PID=$!; PGID="$(ps -o pgid= -p "${APP_PID}"|tr -d ' '||true)"; echo "🟢 started cluster pid=${APP_PID} pgid=${PGID}"; }
kill_cluster_hard(){ [[ -n "${PGID:-}" ]] && kill -9 -"${PGID}" 2>/dev/null || true; sleep 0.2; }
trap 'kill_cluster_hard || true' EXIT

sum_payload_bytes(){ jq '[.messages[].value | length] | add // 0'; }
last_value(){ jq -r 'if (.messages|length)>0 then .messages[-1].value else "" end'; }

poll_sum_leq_and_last_is(){
  local max_bytes="$1" expect_last="$2" resp="{}"
  for _ in $(seq 1 "$VIS_TRIES"); do
    resp="$(get "/broker/consume/$TOPIC/$GROUP/$PART?offset=-1&limit=100")"
    local sum="$(echo "$resp" | sum_payload_bytes)"
    local last="$(echo "$resp" | last_value)"
    if [[ "$sum" -le "$max_bytes" && "$last" == "$expect_last" ]]; then echo "$resp"; return 0; fi
    sleep "$VIS_DELAY_S"
  done; echo "$resp"; return 1
}

poll_zero(){
  local resp="{}"
  for _ in $(seq 1 "$VIS_TRIES"); do
    resp="$(get "/broker/consume/$TOPIC/$GROUP/$PART?offset=-1&limit=100")"
    local c; c="$(jq -r '.count' <<<"$resp" 2>/dev/null || echo null)"
    [[ "$c" == "0" ]] && { echo "$resp"; return 0; }
    sleep "$VIS_DELAY_S"
  done; echo "$resp"; return 1
}

# wait until fsync thread has durably synced >= batch_id
wait_sealed_bid_synced(){
  local want="$1" tout_ms="$2"
  local start_ms=$(python3 - <<'PY'
import time; print(int(time.monotonic()*1000))
PY
)
  while :; do
    local last
    last="$(get "/admin/sealed/status" | jq -r '.last_synced_batch_id // 0')"
    [[ "$last" -ge "$want" ]] && return 0
    local now_ms=$(python3 - <<'PY'
import time; print(int(time.monotonic()*1000))
PY
)
    (( now_ms - start_ms > tout_ms )) && return 1
    usleep 50000 2>/dev/null || sleep 0.05
  done
}

# ---------- run ----------
rm -f ./append.aof ./append.aof.sealed ./append.aof.sealed.lock ./zp_dump.rdb || true

start_cluster
until curl -fsS "${BASE_URL}/health" | grep -q '"ok":1'; do sleep 0.05; done

echo "➕ topic $TOPIC"
post "/broker/topics" "{\"name\":\"$TOPIC\",\"partitions\":8}" >/dev/null || die "topic create failed"

echo "⏱️ + 📏 ttl=${TTL_MS}ms, max_bytes=6"
resp="$(post "/broker/retention" "{\"topic\":\"$TOPIC\",\"partition\":$PART,\"ttl_ms\":$TTL_MS,\"max_bytes\":6}")"
jq -e '.ttl_applied==true and .size_applied==true' <<<"$resp" >/dev/null || { echo "$resp"; die "apply retention failed"; }

echo "📦 BATCH BEFORE restart (${IDK1}): [m1 m2] (durable)"
nd=$'{ "value":"m1" }\n{ "value":"m2" }\n'
rb1=$(printf "%s" "$nd" | curl -fsS -X POST \
  "${BASE_URL}/broker/topics/${TOPIC}/produce-ndjson?partition=${PART}&idempotency_key=${IDK1}&durable=1&timeout_ms=${DURABLE_TIMEOUT_MS}" \
  -H 'content-type: application/x-ndjson' "${HDR_AUTH[@]}" --data-binary @-)
echo "  → $rb1"

# first call must not be a duplicate
jq -e '(.duplicate|not) or (.duplicate==false)' <<<"$rb1" >/dev/null \
   || { echo "$rb1"; die "expected duplicate=false (or omitted)"; }

# if a batch id was returned and not explicitly synced, wait until fsynced
bid="$(jq -r '.batch_id // 0' <<<"$rb1")"
synced="$(jq -r '.synced // empty' <<<"$rb1")"
if [[ "$bid" != "0" && "$synced" != "true" ]]; then
  wait_sealed_bid_synced "$bid" "$DURABLE_TIMEOUT_MS" || { echo "❌ durable sync timeout for batch_id=$bid"; exit 1; }
fi
fo1="$(jq -r '.first_offset // 0' <<<"$rb1")"

echo "🔁 hard restart"
kill_cluster_hard
echo "💥 crashed"
start_cluster
until curl -fsS "${BASE_URL}/health" | grep -q '"ok":1'; do sleep 0.05; done

echo "🔁 duplicate ${IDK1} after restart"
rb2=$(printf "%s" "$nd" | curl -fsS -X POST \
  "${BASE_URL}/broker/topics/${TOPIC}/produce-ndjson?partition=${PART}&idempotency_key=${IDK1}&durable=1&timeout_ms=${DURABLE_TIMEOUT_MS}" \
  -H 'content-type: application/x-ndjson' "${HDR_AUTH[@]}" --data-binary @-)
echo "  → $rb2"

jq -e '.duplicate==true' <<<"$rb2" >/dev/null || { echo "$rb2"; die "expected duplicate=true"; }
fo2="$(jq -r '.first_offset // 0' <<<"$rb2")"
[[ "$fo2" == "$fo1" ]] || { echo "fo1=$fo1 fo2=$fo2"; die "first_offset mismatch"; }

echo "📦 new BATCH (${IDK2}): [m3]"
printf '%s\n' '{"value":"m3"}' | curl -fsS -X POST \
  "${BASE_URL}/broker/topics/${TOPIC}/produce-ndjson?partition=${PART}&idempotency_key=${IDK2}&durable=1&timeout_ms=${DURABLE_TIMEOUT_MS}" \
  -H 'content-type: application/x-ndjson' "${HDR_AUTH[@]}" --data-binary @- >/dev/null

echo "⏲️ GC tick ${GC_TICK_S}s"; sleep "$GC_TICK_S"
echo "🔎 verify size ≤6 and last==m3"
im="$(poll_sum_leq_and_last_is 6 "m3")" || { echo "   resp: $im"; die "size not enforced (batch)"; }
echo "   resp: $im"

echo "⏲️ sleep ${SLEEP_AFTER_S}s (> ${TTL_MS}ms)"; sleep "$SLEEP_AFTER_S"
echo "🧹 verify TTL expired all"
fn="$(poll_zero)" || { echo "   resp: $fn"; die "TTL not enforced (batch)"; }
echo "   resp: $fn"
echo "✅ PASS (batch v2 + auth + idempotent replay)"
