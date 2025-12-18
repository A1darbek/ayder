#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-1109}"
BASE="http://$HOST:$PORT"
TOKEN="${TOKEN:-dev}"
TOKENS="${TOKENS:-}"

TOPIC="${TOPIC:-orders}"
PART="${PART:-0}"          # partition to target (empty => server decides)
GROUP="${GROUP:-bench}"

# Producer knobs
PT="${PT:-12}"             # threads
PC="${PC:-64}"             # connections
PD="${PD:-20s}"            # duration
BATCH="${BATCH:-16}"       # NDJSON lines per request
PAYLOAD="${PAYLOAD:-64}"   # bytes in the "pad" field
MIX="${MIX:-0}"            # 0 objects-only; 1 mix strings every 8th line
BLANK_EVERY="${BLANK_EVERY:-0}"  # 0 never; N every Nth line
IDEM="${IDEM:-0}"          # 1 attach idempotency_key per request
DURABLE="${DURABLE:-0}"    # durable=1 query flag

# Consumer knobs (reuse your existing consumer wrk script)
CT="${CT:-2}"
CC="${CC:-2}"
CD="${CD:-20s}"
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

echo "→ start PRODUCER NDJSON (${PT}t/${PC}c for ${PD}) batch=${BATCH} lines, payload=${PAYLOAD}B, part=${PART:-<auto>}"
PROD_OUT="$(mktemp)"
TOPIC="$TOPIC" PART="${PART:-}" PAYLOAD="$PAYLOAD" BATCH="$BATCH" MIX="$MIX" BLANK_EVERY="$BLANK_EVERY" IDEM="$IDEM" \
TOKEN="$TOKEN" TOKENS="$TOKENS" \
  wrk -t"$PT" -c"$PC" -d"$PD" --latency \
  -s scripts/broker/produce_batch_ndjson_v2.lua "$BASE" >"$PROD_OUT" 2>&1 & PROD_PID=$!

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
echo "→ sample v2 consume after bench"
curl -sS "$BASE/broker/consume/$TOPIC/$GROUP/${PART:-0}?limit=10&offset=-1${B64:+&encoding=b64}" \
  "${HDR_AUTH[@]}" | sed 's/.*/  &/'

rm -f "$PROD_OUT" "$CONS_OUT"
