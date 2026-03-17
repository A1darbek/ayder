#!/usr/bin/env bash
set -Eeuo pipefail

APP_BIN=${APP_BIN:-"./ramforge"}
PORT=${PORT:-1109}
BASE_URL="http://127.0.0.1:${PORT}"

export RF_BEARER_TOKENS="dev@50000:50000:1000000"

TOKEN="${TOKEN:-dev}"
TOKENS="${TOKENS:-}"
first_token(){ [[ -n "${TOKENS}" ]] && printf '%s' "$TOKENS" | tr ';' ',' | cut -d',' -f1 || printf '%s' "$TOKEN"; }
FIRST="$(first_token)"; HDR_AUTH=(); [[ -n "$FIRST" ]] && HDR_AUTH=(-H "Authorization: Bearer ${FIRST}")

TOPIC="orders-both-replay-idemp-$RANDOM-$RANDOM"
PART=${PART:-0}
GROUP=${GROUP:-g}
TTL_MS=${TTL_MS:-500}
SLEEP_AFTER_S=${SLEEP_AFTER_S:-0.7}
VIS_TRIES=${VIS_TRIES:-25}
VIS_DELAY_S=${VIS_DELAY_S:-0.01}
GC_TICK_S=${GC_TICK_S:-0.05}
LOG_FILE=${LOG_FILE:-"./retention_both_replay_idemp.log"}

post(){ curl -fsS -H 'Content-Type: application/json' "${HDR_AUTH[@]}" -X POST "$BASE_URL$1" -d "$2"; }
get(){  curl -fsS "$BASE_URL$1" "${HDR_AUTH[@]}"; }
die(){  echo "❌ $*" >&2; exit 1; }

APP_PID=""; PGID=""
start_cluster(){ setsid "${APP_BIN}" --port "${PORT}" > "${LOG_FILE}" 2>&1 & APP_PID=$!; PGID="$(ps -o pgid= -p "${APP_PID}"|tr -d ' '||true)"; echo "🟢 started cluster pid=${APP_PID} pgid=${PGID}"; }
kill_cluster_hard(){ [[ -n "${PGID:-}" ]] && kill -9 -"${PGID}" 2>/dev/null||true; sleep 0.2; }
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

rm -f ./append.aof ./append.aof.sealed ./append.aof.sealed.lock ./zp_dump.rdb || true
start_cluster
until curl -fsS "${BASE_URL}/health" | grep -q '"ok":1'; do sleep 0.05; done

echo "➕ topic $TOPIC"; post "/broker/topics" "{\"name\":\"$TOPIC\",\"partitions\":8}" >/dev/null || die "topic fail"
echo "⏱️ + 📏 ttl=${TTL_MS}ms, max_bytes=6"
resp="$(post "/broker/retention" "{\"topic\":\"$TOPIC\",\"partition\":$PART,\"ttl_ms\":$TTL_MS,\"max_bytes\":6}")"
jq -e '.ttl_applied==true and .size_applied==true' <<<"$resp" >/dev/null || { echo "$resp"; die "apply retention failed"; }

echo "📨 BEFORE restart: produce m1 (K1)"
r1=$(curl -fsS -X POST \
  "${BASE_URL}/broker/topics/${TOPIC}/produce?partition=${PART}&idempotency_key=K1" \
  -H 'content-type: text/plain' "${HDR_AUTH[@]}" --data-binary 'm1')
o1="$(jq -r '.offset' <<<"$r1")"; jq -e '.duplicate==false' <<<"$r1" >/dev/null || { echo "$r1"; die "expected duplicate=false"; }

echo "🔁 hard restart"; kill_cluster_hard; echo "💥 crashed"; start_cluster
until curl -fsS "${BASE_URL}/health" | grep -q '"ok":1'; do sleep 0.05; done

echo "🔁 duplicate after restart: m1 (K1)"
r2=$(curl -fsS -X POST \
  "${BASE_URL}/broker/topics/${TOPIC}/produce?partition=${PART}&idempotency_key=K1" \
  -H 'content-type: text/plain' "${HDR_AUTH[@]}" --data-binary 'm1')
jq -e '.duplicate==true' <<<"$r2" >/dev/null || { echo "$r2"; die "expected duplicate=true"; }
o2="$(jq -r '.offset' <<<"$r2")"; [[ "$o1" == "$o2" ]] || { echo "o1=$o1 o2=$o2"; die "offset mismatch"; }

echo "📨 new: m2(K2), m3(K3)"
curl -fsS -X POST "${BASE_URL}/broker/topics/${TOPIC}/produce?partition=${PART}&idempotency_key=K2" -H 'content-type: text/plain' "${HDR_AUTH[@]}" --data-binary 'm2' >/dev/null
curl -fsS -X POST "${BASE_URL}/broker/topics/${TOPIC}/produce?partition=${PART}&idempotency_key=K3" -H 'content-type: text/plain' "${HDR_AUTH[@]}" --data-binary 'm3' >/dev/null

echo "⏲️ GC tick ${GC_TICK_S}s"; sleep "$GC_TICK_S"
echo "🔎 verify size ≤6 and last==m3"
im="$(poll_sum_leq_and_last_is 6 "m3")" || { echo "   resp: $im"; die "size not enforced"; }
echo "   resp: $im"

echo "⏲️ sleep ${SLEEP_AFTER_S}s (> ${TTL_MS}ms)"; sleep "$SLEEP_AFTER_S"
echo "🧹 verify TTL expired all"
final="$(poll_zero)" || { echo "   resp: $final"; die "TTL not enforced"; }
echo "   resp: $final"
echo "✅ PASS (single)"
