#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-10.114.0.3}"
PORT="${PORT:-7001}"
BASE="http://$HOST:$PORT"
TOKEN="${TOKEN:-dev}"
TOKENS="${TOKENS:-}"

TOPIC="${TOPIC:-orders}"
PART="${PART:-0}"
GROUP="${GROUP:-bench}"

PT="${PT:-12}"   # producer threads
PC="${PC:-64}"   # producer connections
PD="${PD:-360s}"  # producer duration
PAYLOAD="${PAYLOAD:-64}"  # bytes per message

CT="${CT:-2}"    # consumer threads
CC="${CC:-2}"    # consumer connections
CD="${CD:-20s}"  # consumer duration
LIMIT="${LIMIT:-200}"
B64="${B64:-0}"

first_token() {
  if [ -n "${TOKENS}" ]; then
    printf '%s' "$TOKENS" | tr ';' ',' | cut -d',' -f1
  else
    printf '%s' "$TOKEN"
  fi
}
FIRST="$(first_token)"
HDR_AUTH=()
[ -n "$FIRST" ] && HDR_AUTH=(-H "Authorization: Bearer ${FIRST}")

echo "→ ensure topic '$TOPIC' with 8 partitions"
curl -sS -X POST "$BASE/broker/topics" -H 'Content-Type: application/json' \
  "${HDR_AUTH[@]}" \
  -d "{\"name\":\"$TOPIC\",\"partitions\":8}" | sed 's/.*/  &/'

echo "→ start PRODUCER single (${PT}t/${PC}c for ${PD}), payload=${PAYLOAD}B partition=$PART"
PROD_OUT="$(mktemp)"
TOKEN="$TOKEN" TOKENS="$TOKENS" TOPIC="$TOPIC" PART="$PART" PAYLOAD="$PAYLOAD" \
  wrk -t"$PT" -c"$PC" -d"$PD" --latency \
  -s scripts/broker/produce_single_raw_v2.lua "$BASE" >"$PROD_OUT" 2>&1 & PROD_PID=$!

echo "→ start CONSUMER (${CT}t/${CC}c for ${CD}) group=$GROUP part=$PART limit=$LIMIT b64=$B64"
CONS_OUT="$(mktemp)"
TOKEN="$TOKEN" TOKENS="$TOKENS" TOPIC="$TOPIC" GROUP="$GROUP" PART="$PART" LIMIT="$LIMIT" B64="$B64" \
  wrk -t"$CT" -c"$CC" -d"$CD" --latency \
  -s scripts/broker/consume_v2.lua "$BASE" >"$CONS_OUT" 2>&1 & CONS_PID=$!

wait "$PROD_PID" || true
wait "$CONS_PID" || true

echo
echo "===== PRODUCER WRK SUMMARY ====="
cat "$PROD_OUT"
echo
echo "===== CONSUMER WRK SUMMARY ====="
cat "$CONS_OUT"

echo
echo "→ sample consume after bench"
curl -sS "$BASE/broker/consume/$TOPIC/$GROUP/$PART?limit=10&offset=-1${B64:+&encoding=b64}" \
  "${HDR_AUTH[@]}" | sed 's/.*/  &/'

rm -f "$PROD_OUT" "$CONS_OUT"
