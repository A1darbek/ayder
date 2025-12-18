#!/usr/bin/env bash
set -euo pipefail

# demo creds (adjust as you like)
export RF_BEARER_TOKENS='dev@50000:50000:1000000'

BASE='http://127.0.0.1:1109'
PORT=1109
TOKEN=${TOKEN:-dev}
AUTH=(-H "Authorization: Bearer ${TOKEN}")

RAMFORGE_BIN=${RAMFORGE_BIN:-./ramforge}
LOG_FILE=${LOG_FILE:-./ramforge_demo.log}

# If a broker is already running, restart *that exact* binary (keeps feature set stable).
if pid="$(pgrep -xo ramforge 2>/dev/null)"; then
  running_bin="$(readlink -f "/proc/${pid}/exe" 2>/dev/null || true)"
  if [[ -n "$running_bin" && -x "$running_bin" ]]; then
    RAMFORGE_BIN="$running_bin"
  fi
fi

# ───────────── helpers ─────────────

# Pretty printer:
# - Prefer server-side decoded .doc if present.
# - Else try to parse .value as JSON.
# - Else show raw text/bytes in {raw: ...}.

pp() {
  jq -c '
    .messages[] | {offset,partition}
    + (
        (try .doc catch null)
        // (try (.value | fromjson) catch null)
        // {raw: .value}
      )
  '
}

wait_up() {
  for _ in {1..200}; do
    if out="$(curl -fsS "$BASE/health" 2>/dev/null || true)"; then
      if jq -e '(.ok==1) or (.ok==true)' >/dev/null 2>&1 <<<"$out"; then
        return 0
      fi
    fi
    sleep 0.05
  done
  echo "❌ ramforge did not become healthy on $BASE" >&2
  exit 1
}

hard_restart() {
  echo "🔁 hard restart: pkill -9 ramforge"
  pkill -9 ramforge 2>/dev/null || true
  sleep 0.2
  echo "🚀 starting $RAMFORGE_BIN --port $PORT"
  "$RAMFORGE_BIN" --port "$PORT" >"$LOG_FILE" 2>&1 &
  sleep 0.1
  wait_up
  echo "🟢 broker is up"
}

# small assert helpers for NDJSON part
assert_json_true() {
  local expr="$1" json="$2" msg="${3:-assert failed}"
  jq -e "$expr" >/dev/null <<<"$json" || { echo "❌ $msg"; echo "$json"; exit 1; }
}
assert_json_eq() {
  local expr="$1" expect="$2" json="$3" msg="${4:-assert failed}"
  local got; got="$(jq -r "$expr" <<<"$json")"
  [[ "$got" == "$expect" ]] || { echo "❌ $msg (got=$got expect=$expect)"; echo "$json"; exit 1; }
}

# Autostart if needed
if ! curl -fsS "$BASE/health" >/dev/null 2>&1; then
  echo "ℹ️ broker not running, starting it first..."
  "$RAMFORGE_BIN" --port "$PORT" >"$LOG_FILE" 2>&1 &
  wait_up
fi

# ──────────────────────────────────────────────────────────────────────────────
# ORDERS (partition 0) — single JSON produces
# ──────────────────────────────────────────────────────────────────────────────
curl -sS -X POST "$BASE/broker/topics" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders","partitions":8}'

curl -sS -X POST \
  "$BASE/broker/v2/topics/orders/produce?partition=0&durable=1" "${AUTH[@]}" \
  -H 'Content-Type: application/json' -d '{
    "event":"order.created","order_id":"o-1001",
    "customer":{"id":"cus_A12","email":"sam@example.com"},
    "amount":1299,"currency":"USD",
    "items":[{"sku":"tee-black-s","qty":1},{"sku":"cap-blue","qty":1}],
    "created_at":"2025-03-24T10:12:00Z"
  }'

curl -sS -X POST \
  "$BASE/broker/v2/topics/orders/produce?partition=0&durable=1" "${AUTH[@]}" \
  -H 'Content-Type: application/json' -d '{
    "event":"order.item_added","order_id":"o-1001",
    "item":{"sku":"sticker-pack","qty":1},"ts":"2025-03-24T10:13:10Z"
  }'

curl -sS -X POST \
  "$BASE/broker/v2/topics/orders/produce?partition=0&durable=1" "${AUTH[@]}" \
  -H 'Content-Type: application/json' -d '{
    "event":"order.address_updated","order_id":"o-1001",
    "shipping_address":{"city":"Austin","state":"TX","country":"US"},
    "ts":"2025-03-24T10:13:40Z"
  }'

curl -sS -X POST \
  "$BASE/broker/v2/topics/orders/produce?partition=0&durable=1" "${AUTH[@]}" \
  -H 'Content-Type: application/json' -d '{
    "event":"order.packed","order_id":"o-1001","warehouse":"AUS-1",
    "ts":"2025-03-24T10:15:00Z"
  }'

curl -sS -X POST \
  "$BASE/broker/v2/topics/orders/produce?partition=0&durable=1" "${AUTH[@]}" \
  -H 'Content-Type: application/json' -d '{
    "event":"order.shipped","order_id":"o-1001","carrier":"UPS",
    "tracking":"1Z999AA10123456784","ts":"2025-03-24T10:16:00Z"
  }'

curl -sS -X POST \
  "$BASE/broker/v2/topics/orders/produce?partition=0&durable=1" "${AUTH[@]}" \
  -H 'Content-Type: application/json' -d '{
    "event":"order.delivered","order_id":"o-1001",
    "delivered_at":"2025-03-26T14:21:00Z"
  }'

# consume first 3, commit to offset 2
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/orders/group-a/0?limit=3&unwrap=json&fields=offset,partition,doc" | pp
curl -sS -X POST "$BASE/broker/commit" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"topic":"orders","group":"group-a","partition":0,"offset":2}'

# read next 2
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/orders/group-a/0?limit=2&unwrap=json&fields=offset,partition,doc" | pp

# crash/restart (hard)
hard_restart

# read after restart
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/orders/group-a/0?limit=5&unwrap=json&fields=offset,partition,doc" | pp

# commit after delivered (offset 5)
curl -sS -X POST "$BASE/broker/commit" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"topic":"orders","group":"group-a","partition":0,"offset":5}'

# no new messages
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/orders/group-a/0?limit=10&unwrap=json&fields=offset,partition,doc" | pp

# rewind earliest
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/orders/group-a/0?offset=-1&limit=2&unwrap=json&fields=offset,partition,doc" | pp
curl -sS -X POST "$BASE/broker/commit" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"topic":"orders","group":"group-a","partition":0,"offset":1}'

# new group-b from start
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/orders/group-b/0?offset=-1&limit=4&unwrap=json&fields=offset,partition,doc" | pp
curl -sS -X POST "$BASE/broker/commit" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"topic":"orders","group":"group-b","partition":0,"offset":3}'

# spot checks
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/orders/group-a/0?limit=1&unwrap=json&fields=offset,partition,doc" | pp
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/orders/group-b/0?limit=1&unwrap=json&fields=offset,partition,doc" | pp

# bulk progress events (07..26 seconds)
for i in $(seq -w 07 26); do
  step="${i#0}"
  secs="${i}"
  curl -sS -X POST \
    "$BASE/broker/v2/topics/orders/produce?partition=0&durable=1" "${AUTH[@]}" \
    -H 'Content-Type: application/json' \
    -d "{
      \"event\":\"order.progress\",\"order_id\":\"o-1001\",
      \"stage\":\"step-${step}\",\"ts\":\"2025-03-24T10:20:${secs}Z\"
    }" >/dev/null
done

# processor group consumes w/ commits (pretty)
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/orders/processor/0?offset=-1&limit=5&unwrap=json&fields=offset,partition,doc" | pp
curl -sS -X POST "$BASE/broker/commit" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d '{"topic":"orders","group":"processor","partition":0,"offset":4}'
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/orders/processor/0?limit=5&unwrap=json&fields=offset,partition,doc" | pp
curl -sS -X POST "$BASE/broker/commit" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d '{"topic":"orders","group":"processor","partition":0,"offset":9}'
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/orders/processor/0?limit=5&unwrap=json&fields=offset,partition,doc" | pp
curl -sS -X POST "$BASE/broker/commit" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d '{"topic":"orders","group":"processor","partition":0,"offset":14}'
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/orders/processor/0?limit=5&unwrap=json&fields=offset,partition,doc" | pp

# restart then re-check cursor
hard_restart
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/orders/processor/0?limit=3&unwrap=json&fields=offset,partition,doc" | pp

# ──────────────────────────────────────────────────────────────────────────────
# PAYMENTS — idempotency on single events
# ──────────────────────────────────────────────────────────────────────────────
curl -sS -X POST "$BASE/broker/topics" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"name":"payments","partitions":1}'

curl -sS -X POST \
  "$BASE/broker/v2/topics/payments/produce?partition=0&durable=1&idempotency_key=tx-001" \
  "${AUTH[@]}" -H 'Content-Type: application/json' -d '{
    "event":"payment.initiated","payment_id":"pay_1","order_id":"o-1001",
    "amount":1299,"currency":"USD","method":"card","card_last4":"4242"
  }'

curl -sS -X POST \
  "$BASE/broker/v2/topics/payments/produce?partition=0&durable=1&idempotency_key=tx-002" \
  "${AUTH[@]}" -H 'Content-Type: application/json' -d '{
    "event":"payment.authorized","payment_id":"pay_2","order_id":"o-1002",
    "amount":4599,"currency":"USD","auth_code":"A1B2C3"
  }'

# duplicate of tx-001 → de-duplicated
curl -sS -X POST \
  "$BASE/broker/v2/topics/payments/produce?partition=0&durable=1&idempotency_key=tx-001" \
  "${AUTH[@]}" -H 'Content-Type: application/json' -d '{
    "event":"payment.initiated","payment_id":"pay_1","order_id":"o-1001",
    "amount":1299,"currency":"USD","method":"card","card_last4":"4242"
  }'

curl -sS -X POST \
  "$BASE/broker/v2/topics/payments/produce?partition=0&durable=1&idempotency_key=tx-003" \
  "${AUTH[@]}" -H 'Content-Type: application/json' -d '{
    "event":"payment.captured","payment_id":"pay_3","order_id":"o-1003",
    "amount":9900,"currency":"USD","captured_at":"2025-03-24T10:18:00Z"
  }'

# consume/commit (pretty)
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/payments/test/0?offset=-1&limit=10&unwrap=json&fields=offset,partition,doc" | pp
curl -sS -X POST "$BASE/broker/commit" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d '{"topic":"payments","group":"payment-processor","partition":0,"offset":4}'
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/payments/payment-processor/0?limit=2&unwrap=json&fields=offset,partition,doc" | pp

# restart and verify dedupe remains
hard_restart
curl -sS -X POST \
  "$BASE/broker/v2/topics/payments/produce?partition=0&durable=1&idempotency_key=payment-uuid-ccc" \
  "${AUTH[@]}" -H 'Content-Type: application/json' -d '{
    "event":"charge.created","charge_id":"ch_300","amount":30000,"currency":"USD"
  }'
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/payments/payment-processor/0?offset=-1&limit=10&unwrap=json&fields=offset,partition,doc" | pp

# ──────────────────────────────────────────────────────────────────────────────
# BATCH-PAYMENTS — real CHUNKED upload; fallback to Content-Length if needed
# ──────────────────────────────────────────────────────────────────────────────
curl -sS -X POST "$BASE/broker/topics" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"name":"batch-payments","partitions":1}'

# send NDJSON using HTTP/1.1 TE: chunked.
# we explicitly use: --http1.1 + -T -  (stdin) + no Expect: 100-continue
post_ndjson() {
  local idem="$1"
  local payload="$2"

  # First attempt: true HTTP/1.1 chunked via --data-binary @- (no manual TE header).
  # We also kill 100-continue and cap max time to avoid hangs.
  local resp
  resp=$(printf "%s" "$payload" | curl -sS --http1.1 -X POST \
    "$BASE/broker/v2/topics/batch-payments/produce-ndjson?partition=0&durable=1&idempotency_key=$idem&timeout_ms=200" \
    "${AUTH[@]}" \
    -H 'Content-Type: application/x-ndjson' \
    -H 'Expect:' \
    --no-buffer \
    --fail-with-body \
    --connect-timeout 2 --max-time 10 \
    --data-binary @-)

  # Fallback: send with Content-Length (tmp file) if server still says no messages
  if jq -e '.error=="No valid messages"' >/dev/null 2>&1 <<<"$resp"; then
    local tmp; tmp="$(mktemp)"; printf "%s" "$payload" > "$tmp"
    resp=$(curl -sS --http1.1 -X POST \
      "$BASE/broker/v2/topics/batch-payments/produce-ndjson?partition=0&durable=1&idempotency_key=$idem&timeout_ms=200" \
      "${AUTH[@]}" \
      -H 'Content-Type: application/x-ndjson' \
      --no-buffer \
      --fail-with-body \
      --connect-timeout 2 --max-time 10 \
      --data-binary @"$tmp")
    rm -f "$tmp"
  fi

  printf "%s" "$resp"
}


# NOTE: you already defined assert_json_true/assert_json_eq above; reuse them here.

# batch #1 (intentionally includes a blank line)
nd1_payload=$'{"event":"charge.created","user_id":"user-100","amount":5000,"currency":"USD"}\n\n{"event":"charge.created","user_id":"user-101","amount":7500,"currency":"USD"}\n{"event":"charge.created","user_id":"user-102","amount":10000,"currency":"USD"}\n'
resp_nd1="$(post_ndjson "batch-uuid-001" "$nd1_payload")"
echo "$resp_nd1"
assert_json_true '.ok==true' "$resp_nd1" "ndjson batch#1 .ok"
assert_json_eq   '.count'    '3'         "$resp_nd1" "ndjson batch#1 count"

# batch #2
nd2_payload=$'{"event":"charge.created","user_id":"user-103","amount":20000,"currency":"USD"}\n{"event":"charge.created","user_id":"user-104","amount":15000,"currency":"USD"}\n'
resp_nd2="$(post_ndjson "batch-uuid-002" "$nd2_payload")"
echo "$resp_nd2"
assert_json_true '.ok==true' "$resp_nd2" "ndjson batch#2 .ok"
assert_json_eq   '.count'    '2'         "$resp_nd2" "ndjson batch#2 count"

# replay batch #1 → duplicate=true
resp_nd1_dup="$(post_ndjson "batch-uuid-001" "$nd1_payload")"
echo "$resp_nd1_dup"
assert_json_true '.ok==true'       "$resp_nd1_dup" "ndjson batch#1 duplicate .ok"
assert_json_true '.duplicate==true' "$resp_nd1_dup" "ndjson batch#1 duplicate flag"

# consume checks (unchanged)
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/batch-payments/processor/0?offset=-1&limit=10&unwrap=json&fields=offset,partition,doc" | pp
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/batch-payments/processor/0?offset=-1&limit=3&unwrap=json&fields=offset,partition,doc" | pp
curl -sS -X POST "$BASE/broker/commit" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d '{"topic":"batch-payments","group":"processor","partition":0,"offset":2}'
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/batch-payments/processor/0?limit=2&unwrap=json&fields=offset,partition,doc" | pp

# restart and replay duplicate batch-uuid-002 (should still dedupe the whole batch)
hard_restart
resp_nd2_dup="$(post_ndjson "batch-uuid-002" "$nd2_payload")"
echo "$resp_nd2_dup"
assert_json_true '.ok==true'       "$resp_nd2_dup" "ndjson batch#2 duplicate .ok"
assert_json_true '.duplicate==true' "$resp_nd2_dup" "ndjson batch#2 duplicate flag"
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/batch-payments/processor/0?offset=-1&limit=10&unwrap=json&fields=offset,partition,doc" | pp



# ──────────────────────────────────────────────────────────────────────────────
# TRANSACTIONS — multiple consumer groups + delete-before compaction
# ──────────────────────────────────────────────────────────────────────────────
curl -sS -X POST "$BASE/broker/topics" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"name":"transactions","partitions":1}'

# burst (10 tx events) with valid minute range 21..30
for n in {1..10}; do
  amt=$((n*100))
  MIN=$(printf '%02d' $((20 + n)))   # 21..30
  ik=$(printf "txn-uuid-%03d" "$n")
  curl -sS -X POST \
    "$BASE/broker/v2/topics/transactions/produce?partition=0&durable=1&idempotency_key=$ik" \
    "${AUTH[@]}" -H 'Content-Type: application/json' \
    -d "{
      \"event\":\"transaction.logged\",
      \"txn_id\":\"txn_${n}\",
      \"account_id\":\"acct_42\",
      \"amount_cents\":${amt}00,
      \"currency\":\"USD\",
      \"ts\":\"2025-03-24T10:${MIN}:00Z\"
    }" >/dev/null
done

# three consumer groups (pretty)
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/transactions/payment-processor/0?offset=-1&limit=5&unwrap=json&fields=offset,partition,doc" | pp
curl -sS -X POST "$BASE/broker/commit" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d '{"topic":"transactions","group":"payment-processor","partition":0,"offset":4}'

curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/transactions/fraud-detection/0?offset=-1&limit=3&unwrap=json&fields=offset,partition,doc" | pp
curl -sS -X POST "$BASE/broker/commit" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d '{"topic":"transactions","group":"fraud-detection","partition":0,"offset":2}'

curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/transactions/audit-log/0?offset=-1&limit=5&unwrap=json&fields=offset,partition,doc" | pp
curl -sS -X POST "$BASE/broker/commit" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d '{"topic":"transactions","group":"audit-log","partition":0,"offset":4}'

# compaction
curl -sS -X POST "$BASE/broker/delete-before" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"topic":"transactions","partition":0,"before_offset":2}'

curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/transactions/verify/0?offset=-1&limit=20&unwrap=json&fields=offset,partition,doc" | pp

curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/transactions/fraud-detection/0?limit=5&unwrap=json&fields=offset,partition,doc" | pp
curl -sS -X POST "$BASE/broker/commit" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d '{"topic":"transactions","group":"fraud-detection","partition":0,"offset":7}'

curl -sS -X POST "$BASE/broker/delete-before" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"topic":"transactions","partition":0,"before_offset":5}'

# restart, then verify state
hard_restart
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/transactions/audit-log/0?limit=3&unwrap=json&fields=offset,partition,doc" | pp

# prove idempotency after compaction (replay first txn)
curl -sS -X POST \
  "$BASE/broker/v2/topics/transactions/produce?partition=0&durable=1&idempotency_key=txn-uuid-001" \
  "${AUTH[@]}" -H 'Content-Type: application/json' -d '{
    "event":"transaction.logged","txn_id":"txn_1","account_id":"acct_42",
    "amount_cents":10000,"currency":"USD","ts":"2025-03-24T10:21:00Z"
  }'
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/transactions/verify/0?offset=-1&limit=20&unwrap=json&fields=offset,partition,doc" | pp

# ──────────────────────────────────────────────────────────────────────────────
# EXTRA: “rocket” (non-durable) mode
# ──────────────────────────────────────────────────────────────────────────────
curl -sS -X POST "$BASE/broker/topics" "${AUTH[@]}" \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders-rocket","partitions":8}'

for n in {1..6}; do
  curl -sS -X POST \
    "$BASE/broker/v2/topics/orders-rocket/produce?partition=0&durable=0" \
    "${AUTH[@]}" -H 'Content-Type: application/json' \
    -d "{
      \"event\":\"rocket.test\",\"seq\":${n},
      \"note\":\"non-durable example\"
    }" >/dev/null
done

curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/orders-rocket/group-a/0?limit=3&unwrap=json&fields=offset,partition,doc" | pp
curl -sS -X POST "$BASE/broker/commit" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d '{"topic":"orders-rocket","group":"group-a","partition":0,"offset":2}'
curl -sS "${AUTH[@]}" \
  "$BASE/broker/v2/consume/orders-rocket/group-a/0?limit=2&unwrap=json&fields=offset,partition,doc" | pp
