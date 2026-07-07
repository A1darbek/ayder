# Ayder Quick Start

Ayder is an HTTP-native durable event log and message bus. It is designed for
correctness-sensitive integration paths where operators need to know what was
produced, replayed, consumed, committed, and recovered.

This guide is for design partners, HN readers, and anyone evaluating the API.
It covers every Ayder endpoint family with curl examples.

## 1. Start Ayder

With Docker:

```bash
git clone https://github.com/A1darbek/ayder.git
cd ayder
docker compose up -d --build
```

From source:

```bash
make clean && make
RF_BEARER_TOKENS='dev@5000:5000:1000000' ./ayder --port 1109
```

Set your shell helpers:

```bash
BASE='http://127.0.0.1:1109'
AUTH='Authorization: Bearer dev'
JSON='Content-Type: application/json'
```

Public endpoints:

```bash
curl -fsS "$BASE/health"
curl -fsS "$BASE/ready"
curl -fsS "$BASE/metrics" | head
curl -fsS "$BASE/metrics_ha" | head
```

All other endpoints require `Authorization: Bearer <token>`.

## 2. Broker: Topic, Produce, Consume, Commit

Create a topic:

```bash
curl -fsS -X POST "$BASE/broker/topics" \
  -H "$AUTH" -H "$JSON" \
  -d '{"name":"orders","partitions":1}' | jq
```

Produce one durable event with an idempotency key:

```bash
cat > /tmp/order-confirmed.json <<'JSON'
{
  "event_type": "order.confirmed",
  "event_id": "evt_order_001",
  "order_id": "ord_123",
  "amount": "42.00",
  "currency": "USD"
}
JSON

curl -fsS -X POST \
  "$BASE/broker/topics/orders/produce?partition=0&timeout_ms=5000&idempotency_key=evt_order_001" \
  -H "$AUTH" -H "$JSON" \
  --data-binary @/tmp/order-confirmed.json | jq
```

Produce multiple NDJSON messages:

```bash
cat > /tmp/orders.ndjson <<'NDJSON'
{"event_id":"evt_order_002","kind":"order","amount":12}
{"event_id":"evt_order_003","kind":"order","amount":27}
NDJSON

curl -fsS -X POST \
  "$BASE/broker/topics/orders/produce-ndjson?partition=0&timeout_ms=5000&idempotency_key=batch_orders_001" \
  -H "$AUTH" -H 'Content-Type: application/x-ndjson' \
  --data-binary @/tmp/orders.ndjson | jq
```

Consume from offset 0. Use `encoding=b64` for binary-safe values:

```bash
curl -fsS \
  "$BASE/broker/consume/orders/order_worker/0?offset=0&limit=10&encoding=b64" \
  -H "$AUTH" | jq
```

Decode the first consumed message:

```bash
curl -fsS \
  "$BASE/broker/consume/orders/order_worker/0?offset=0&limit=1&encoding=b64" \
  -H "$AUTH" |
  jq -r '.messages[0].value_b64' |
  base64 -d
```

Commit the highest processed offset after your application effect/audit write:

```bash
curl -fsS -X POST "$BASE/broker/commit" \
  -H "$AUTH" -H "$JSON" \
  -d '{"topic":"orders","group":"order_worker","partition":0,"offset":0}' | jq
```

Operator rule: commit only after the business effect decision is durably
recorded by your application.

## 3. Broker Query

`/broker/query` scans JSON messages from a topic, filters them, optionally emits
projected rows, and counts aggregates.

Ayder supports RFC 10008 `QUERY /broker/query` for safe, idempotent query
requests with JSON request content. `POST /broker/query` remains available for
older clients.

```bash
curl -fsS -X QUERY "$BASE/broker/query" \
  -H "$AUTH" -H "$JSON" \
  -d '{
    "source": {
      "topic": "orders",
      "group": "query_worker",
      "partition": 0,
      "offset": 0,
      "limit": 100
    },
    "filter": [
      {"field": "kind", "op": "eq", "value": "order"}
    ],
    "aggregate": {
      "group_by": ["kind"]
    },
    "transform": {
      "left_fields": ["event_id", "kind", "amount"],
      "emit_rows": 10
    }
  }' | jq
```

Supported filter operators are `eq`, `ne`, `gt`, `ge`, `lt`, and `le`.

Demo:

```bash
./demos/http_query_method/run_demo.sh
```

## 4. Broker Join

Create a second topic and produce data to join:

```bash
curl -fsS -X POST "$BASE/broker/topics" \
  -H "$AUTH" -H "$JSON" \
  -d '{"name":"payments","partitions":1}' | jq

printf '%s\n' \
  '{"event_id":"pay_001","order_id":"ord_123","status":"SUCCESS"}' \
  > /tmp/payments.ndjson

curl -fsS -X POST \
  "$BASE/broker/topics/payments/produce-ndjson?partition=0&timeout_ms=5000" \
  -H "$AUTH" -H 'Content-Type: application/x-ndjson' \
  --data-binary @/tmp/payments.ndjson | jq
```

Join orders to payments by `order_id`:

```bash
curl -fsS -X POST "$BASE/broker/join" \
  -H "$AUTH" -H "$JSON" \
  -d '{
    "left": {
      "topic": "orders",
      "group": "join_left",
      "partition": 0,
      "offset": 0,
      "limit": 100
    },
    "right": {
      "topic": "payments",
      "group": "join_right",
      "partition": 0,
      "offset": 0,
      "limit": 100
    },
    "on": {
      "left_key": "order_id",
      "right_key": "order_id"
    },
    "window": {
      "size_ms": 600000,
      "allowed_lateness_ms": 0
    },
    "emit": {
      "left_fields": ["order_id", "amount"],
      "right_fields": ["status"],
      "max_rows": 20
    },
    "join": {
      "type": "inner"
    }
  }' | jq
```

Join types: `inner`, `left`, `right`, and `full`.

## 5. Retention And Delete-Before

Apply topic retention:

```bash
curl -fsS -X POST "$BASE/broker/retention" \
  -H "$AUTH" -H "$JSON" \
  -d '{"topic":"orders","partition":0,"ttl_ms":600000,"max_bytes":1048576}' | jq
```

Apply global retention:

```bash
curl -fsS -X POST "$BASE/broker/retention" \
  -H "$AUTH" -H "$JSON" \
  -d '{"topic":"*","ttl_ms":3600000,"max_bytes":104857600}' | jq
```

Delete messages before an offset:

```bash
curl -fsS -X POST "$BASE/broker/delete-before" \
  -H "$AUTH" -H "$JSON" \
  -d '{"topic":"orders","partition":0,"before_offset":1}' | jq
```

## 6. KV Store

Ayder also exposes a small namespaced KV API with CAS and TTL.

Put a value:

```bash
curl -fsS -X POST "$BASE/kv/demo/customer/123?ttl_ms=600000&timeout_ms=5000" \
  -H "$AUTH" \
  --data-binary 'customer-state-v1' | jq
```

Get a value. Values are returned as base64:

```bash
curl -fsS "$BASE/kv/demo/customer/123" -H "$AUTH" | jq
```

Get metadata:

```bash
curl -fsS "$BASE/kv/demo/customer/123/meta" -H "$AUTH" | jq
```

CAS update:

```bash
CAS="$(curl -fsS "$BASE/kv/demo/customer/123/meta" -H "$AUTH" | jq -r '.cas')"

curl -fsS -X POST "$BASE/kv/demo/customer/123?cas=$CAS&timeout_ms=5000" \
  -H "$AUTH" \
  --data-binary 'customer-state-v2' | jq
```

CAS delete:

```bash
CAS="$(curl -fsS "$BASE/kv/demo/customer/123/meta" -H "$AUTH" | jq -r '.cas')"

curl -fsS -X DELETE "$BASE/kv/demo/customer/123?cas=$CAS&timeout_ms=5000" \
  -H "$AUTH" | jq
```

## 7. Admin And Sealed Log

Start compaction:

```bash
curl -fsS -X POST "$BASE/admin/compact" -H "$AUTH" -d '{}' | jq
```

Check the sealed sync watermark:

```bash
curl -fsS "$BASE/admin/sealed/status" -H "$AUTH" | jq
```

Run sealed-file garbage collection:

```bash
curl -fsS -X POST "$BASE/admin/sealed/gc" \
  -H "$AUTH" -H "$JSON" \
  -d '{"max_age_ms":0,"max_bytes":0}' | jq
```

Release the sealed test hold, if a test has enabled it:

```bash
curl -fsS -X POST "$BASE/admin/sealed/release" -H "$AUTH" -d '{}' | jq
```

## 8. HA Membership

These routes are useful when running Ayder in HA mode. On a normal single-node
process, membership calls may return `ha_not_enabled`.

```bash
curl -fsS "$BASE/ha/membership" -H "$AUTH" | jq

curl -fsS -X POST "$BASE/ha/membership/add" \
  -H "$AUTH" -H "$JSON" \
  -d '{"node_id":"node-4","advertise_addr":"127.0.0.1:7704","http_addr":"http://127.0.0.1:1104","priority":10}' | jq

curl -fsS -X POST "$BASE/ha/membership/promote" \
  -H "$AUTH" -H "$JSON" \
  -d '{"node_id":"node-4"}' | jq

curl -fsS -X POST "$BASE/ha/membership/remove" \
  -H "$AUTH" -H "$JSON" \
  -d '{"node_id":"node-4"}' | jq
```

Membership mutations are leader-only.

## 9. Recovery Evidence Demos

The API above is broker-first. The recovery-receipt demos show how applications
write effect audit evidence on top of Ayder:

```bash
./examples/payment-ambiguity-paypal/run_all.sh
./examples/payment-webhook-ordering/run_demo.sh
./examples/data-pipeline-recovery/run_all.sh
./demos/partial_effect_recovery/run_all.sh
```

## 10. Verify Every Route

Run the smoke test that exercises every registered HTTP route:

```bash
START_SERVER=1 SERVER_BIN="$PWD/ayder" PORT=1109 tests/smoke/api_smoke.sh
```

The smoke test checks route coverage against `src/app_routes.c`.
