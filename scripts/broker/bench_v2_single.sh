#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-127.0.0.1}" # WSL2: 172.31.76.127
PORT="${PORT:-7001}"
BASE="http://$HOST:$PORT"
TOKEN="${TOKEN:-dev}"
TOKENS="${TOKENS:-}"

TOPIC="${TOPIC:-bench}"
PART="${PART:-0}"
GROUP="${GROUP:-bench}"

PT="${PT:-4}"     # producer threads
PC="${PC:-64}"    # producer connections
PD="${PD:-30s}"    # producer duration
PAYLOAD="${PAYLOAD:-64}"  # bytes per message

PROD_RATE="${PROD_RATE:-0}"   # 0 => wrk (max), else wrk2 (rate-limited)

# NEW: enable server timing collection in response JSON
TIMING="${TIMING:-0}"         # set TIMING=1 to append &timing=1
SERVER_TOPK="${SERVER_TOPK:-256}"  # per-thread topK tail samples to keep

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

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║           AYDER BENCHMARK (wrk / wrk2)                        ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""
echo "Target:     $BASE"
echo "Duration:   Producer=$PD"
echo "Payload:    ${PAYLOAD}B"
if [ "$PROD_RATE" = "0" ]; then
  echo "Mode:       MAX THROUGHPUT (wrk - no rate limit)"
else
  echo "Mode:       CONTROLLED RATE (wrk2 - ${PROD_RATE} req/s)"
fi
echo "Timing:     ${TIMING}  (server_us in JSON when TIMING=1)"
echo "SERVER_TOPK:${SERVER_TOPK}"
echo ""

echo "→ ensure topic '$TOPIC' with 8 partitions"
curl -sS -X POST "$BASE/broker/topics" -H 'Content-Type: application/json' \
  "${HDR_AUTH[@]}" \
  -d "{\"name\":\"$TOPIC\",\"partitions\":8}" | sed 's/.*/  &/'

echo "→ start PRODUCER single (${PT}t/${PC}c for ${PD}), payload=${PAYLOAD}B partition=$PART"
PROD_OUT="$(mktemp)"

ENVVARS=(
  "TOKEN=$TOKEN"
  "TOKENS=$TOKENS"
  "TOPIC=$TOPIC"
  "PART=$PART"
  "PAYLOAD=$PAYLOAD"
  "TIMING=$TIMING"
  "SERVER_TOPK=$SERVER_TOPK"
)

if [ "$PROD_RATE" = "0" ]; then
  echo "  Using: wrk (unlimited rate)"
  env "${ENVVARS[@]}" \
    wrk -t"$PT" -c"$PC" -d"$PD" --latency \
    -s scripts/broker/produce_single_raw_v2.lua "$BASE" >"$PROD_OUT" 2>&1 &
  PROD_PID=$!
else
  echo "  Using: wrk2 -R${PROD_RATE} (rate limited)"
  env "${ENVVARS[@]}" \
    wrk2 -t"$PT" -c"$PC" -d"$PD" -R"$PROD_RATE" --latency \
    -s scripts/broker/produce_single_raw_v2.lua "$BASE" >"$PROD_OUT" 2>&1 &
  PROD_PID=$!
fi

wait "$PROD_PID" || true

echo
echo "===== PRODUCER WRK SUMMARY ====="
cat "$PROD_OUT"

echo
echo "→ sample consume after bench"
curl -sS "$BASE/broker/consume/$TOPIC/$GROUP/$PART?limit=10&offset=-1${B64:+&encoding=b64}" \
  "${HDR_AUTH[@]}" | sed 's/.*/  &/'

rm -f "$PROD_OUT"