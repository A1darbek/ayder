#!/usr/bin/env bash
# tests/demo/protected_replay.sh
# Proves: commit-aware fence lets a protected group replay after TTL expiry.
# - set ttl=500ms + max_bytes=6 + protect_group="payments" on part 0
# - produce m1
# - crash before commit
# - restart, wait > TTL
# - protected group can still read m1
# - commit -> after a GC tick it's gone

set -Eeuo pipefail

APP_BIN="${APP_BIN:-./ramforge}"
PORT="${PORT:-1109}"
BURL="http://127.0.0.1:${PORT}"
TOPIC="orders-payments-$RANDOM"
GROUP="payments"
LOG_FILE=${LOG_FILE:-"./protected_replay.log"}

need() { command -v "$1" >/dev/null 2>&1 || { echo "❌ missing: $1" >&2; exit 1; }; }
need curl
need jq
need setsid

die(){ echo "❌ $*" >&2; exit 1; }

APP_PID=""; PGID=""

start_cluster() {
  # start server detached, collect PID/PGID, and disown so bash won’t print “Killed”
  setsid "${APP_BIN}" --port "${PORT}" >> "${LOG_FILE}" 2>&1 &
  APP_PID=$!
  PGID="$(ps -o pgid= -p "${APP_PID}" | tr -d ' ' || true)"
  disown || true
  echo "🟢 started cluster pid=${APP_PID} pgid=${PGID} (logs: ${LOG_FILE})"
}

kill_cluster_hard() {
  if [[ -n "${PGID:-}" ]]; then
    kill -9 -"${PGID}" 2>/dev/null || true
  elif [[ -n "${APP_PID:-}" ]]; then
    kill -9 "${APP_PID}" 2>/dev/null || true
  fi
  sleep 0.2
}

wait_health() {
  local deadline=$((SECONDS+20))
  until curl -fsS "${BURL}/health" 2>/dev/null | grep -q '"ok":1'; do
    (( SECONDS > deadline )) && die "health check timeout"
    sleep 0.05
  done
  echo "✅ /health OK"
}

jqget(){ curl -fsS "$@"; }
post(){ curl -fsS -H 'Content-Type: application/json' -X POST "$BURL$1" -d "$2"; }

cleanup() { kill_cluster_hard || true; }
trap cleanup EXIT

# fresh state
rm -f ./append.aof ./append.aof.sealed ./append.aof.sealed.lock ./zp_dump.rdb || true

start_cluster
wait_health

# topic + retention (TTL+size+protect fence)
post /broker/topics    "{\"name\":\"$TOPIC\",\"partitions\":8}" >/dev/null \
  || die "topic create failed"

post /broker/retention "{\"topic\":\"$TOPIC\",\"partition\":0,\"ttl_ms\":500,\"max_bytes\":6,\"protect_group\":\"$GROUP\"}" >/dev/null \
  || die "retention set failed"

# produce one message at part 0
post /broker/produce-sealed "{\"topic\":\"$TOPIC\",\"partition\":0,\"value\":\"m1\"}" >/dev/null \
  || die "produce m1 failed"

# simulate crash before commit
kill_cluster_hard
echo "💥 crashed cluster (SIGKILL group ${PGID:-?})"

# restart
start_cluster
wait_health

# wait > TTL so normal consumers would see nothing
sleep 0.7

# protected group can still read it
resp="$(jqget "$BURL/broker/consume/$TOPIC/$GROUP/0?offset=-1&limit=10")"
echo "$resp" | jq -e '.messages[0].value=="m1"' >/dev/null || {
  echo "   resp: $resp"
  die "protected group failed to replay m1 after TTL"
}

# commit the consumed offset
off="$(echo "$resp" | jq -r '.messages[0].offset')"
post /broker/commit "{\"topic\":\"$TOPIC\",\"group\":\"$GROUP\",\"partition\":0,\"offset\":$off}" >/dev/null \
  || die "commit failed"

# nudge GC; now it should be gone
sleep 0.15
jqget "$BURL/broker/consume/$TOPIC/$GROUP/0?offset=$off&limit=10" \
  | jq -e '.count==0' >/dev/null || die "message still visible after commit+GC"

echo "✅ protected replay after restart PASS (topic=$TOPIC group=$GROUP)"
