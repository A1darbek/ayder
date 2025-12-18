#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-1109}"
BASE="http://$HOST:$PORT"

TOPIC="${TOPIC:-orders}"
PART="${PART:-0}"
GROUP="${GROUP:-bench}"

PT="${PT:-12}"   # producer threads
PC="${PC:-64}"   # producer connections
PD="${PD:-20s}"  # producer duration

CT="${CT:-2}"    # consumer threads
CC="${CC:-2}"    # consumer connections
CD="${CD:-20s}"  # consumer duration
LIMIT="${LIMIT:-200}"

# 1) create topic (idempotent)
echo "→ ensure topic '$TOPIC' with 8 partitions"
curl -sS -X POST "$BASE/broker/topics" -H 'Content-Type: application/json' \
  -d "{\"name\":\"$TOPIC\",\"partitions\":8}" | sed 's/.*/  &/'

# 2) kick off producer wrk (single messages) in background
echo "→ start producer wrk (${PT}t/${PC}c for ${PD}) on partition $PART"
PROD_OUT="$(mktemp)"
TOPIC="$TOPIC" PART="$PART" wrk -t"$PT" -c"$PC" -d"$PD" --latency \
  -s scripts/produce_single_sealed.lua "$BASE" >"$PROD_OUT" 2>&1 &
PROD_PID=$!

# 3) kick off consumer wrk in background
echo "→ start consumer wrk (${CT}t/${CC}c for ${CD}) group=$GROUP part=$PART limit=$LIMIT"
CONS_OUT="$(mktemp)"
TOPIC="$TOPIC" GROUP="$GROUP" PART="$PART" LIMIT="$LIMIT" wrk -t"$CT" -c"$CC" -d"$CD" --latency \
  -s scripts/consume.lua "$BASE" >"$CONS_OUT" 2>&1 &
CONS_PID=$!

# 4) wait for both
wait "$PROD_PID" || true
wait "$CONS_PID" || true

# 5) print summaries
echo
echo "===== PRODUCER WRK SUMMARY ====="
cat "$PROD_OUT"
echo
echo "===== CONSUMER WRK SUMMARY ====="
cat "$CONS_OUT"

# 6) quick peek at current offsets
echo
echo "→ sample consume after bench"
curl -sS "$BASE/broker/consume/$TOPIC/$GROUP/$PART?limit=10" | sed 's/.*/  &/'

# cleanup
rm -f "$PROD_OUT" "$CONS_OUT"
