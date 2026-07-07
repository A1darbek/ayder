# Ayder API Reference

This reference documents the HTTP API registered in `src/app_routes.c`.

Base URL examples use:

```bash
BASE='http://127.0.0.1:1109'
AUTH='Authorization: Bearer dev'
```

## Authentication

Public endpoints:

- `GET /health`
- `GET /ready`
- `GET /metrics`
- `GET /metrics_ha`

All other endpoints require:

```http
Authorization: Bearer <token>
```

Tokens are configured with `RF_BEARER_TOKENS`:

```bash
RF_BEARER_TOKENS='dev@5000:5000:1000000' ./ayder --port 1109
```

The token format is:

```text
token@rps:burst:daily
```

For local demos, `dev` is the conventional token.

## Response Conventions

Most successful JSON endpoints include `ok:true`, but older utility endpoints
may return fields such as `{"ready":true}` or Prometheus text.

Common error fields:

```json
{
  "ok": false,
  "error": "machine_code",
  "message": "human readable detail"
}
```

Write paths can return `503` when:

- persistence is not ready: `sealed_only`
- HA leader lacks quorum: `no_quorum`
- the node is a follower without a usable redirect: `read_only_follower`

In HA mode, follower writes can return a redirect body with `leader_url`.

## Endpoint Index

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Liveness |
| GET | `/ready` | Persistence readiness |
| GET | `/metrics` | Prometheus metrics |
| GET | `/metrics_ha` | HA Prometheus metrics |
| POST | `/admin/compact` | Start async compaction |
| GET | `/admin/sealed/status` | Last synced sealed batch |
| POST | `/admin/sealed/release` | Release sealed test hold |
| POST | `/admin/sealed/gc` | Sealed log garbage collection |
| GET | `/ha/membership` | HA membership view |
| POST | `/ha/membership/add` | Add learner |
| POST | `/ha/membership/promote` | Promote learner to voter |
| POST | `/ha/membership/remove` | Remove member |
| POST | `/broker/topics` | Create or ensure topic |
| POST | `/broker/topics/:topic/produce` | Produce one message |
| POST | `/broker/topics/:topic/produce-ndjson` | Produce NDJSON batch |
| GET | `/broker/consume/:topic/:group/:partition` | Consume messages |
| POST | `/broker/commit` | Commit consumer offset |
| POST | `/broker/query` | Scan/filter/project/aggregate JSON events |
| QUERY | `/broker/query` | Safe/idempotent query with JSON request content |
| POST | `/broker/join` | Windowed join over two topics |
| POST | `/broker/retention` | Set broker retention |
| POST | `/broker/delete-before` | Delete messages before offset |
| POST | `/kv/:ns/:key+` | Put KV value |
| GET | `/kv/:ns/:key+` | Get KV value |
| GET | `/kv/:ns/:key+/meta` | Get KV metadata |
| DELETE | `/kv/:ns/:key+` | Delete KV value |

## Health And Metrics

### `GET /health`

Returns liveness:

```json
{"ok":1}
```

### `GET /ready`

Returns whether persistence is ready for writes:

```json
{"ready":true}
```

### `GET /metrics`

Returns Prometheus text metrics.

### `GET /metrics_ha`

Returns HA-specific Prometheus text metrics.

## Admin

### `POST /admin/compact`

Starts persistence compaction asynchronously.

Response:

```json
{"result":"compaction_started","async":true}
```

### `GET /admin/sealed/status`

Returns the last sealed batch known to be synced:

```json
{"ok":true,"last_synced_batch_id":12}
```

Use this to verify a produced `batch_id` reached the sync watermark.

### `POST /admin/sealed/release`

Releases the sealed test hold used by tests.

Response:

```json
{"ok":true,"released":1}
```

### `POST /admin/sealed/gc`

Request body:

```json
{
  "max_age_ms": 0,
  "max_bytes": 0
}
```

Response:

```json
{"ok":true,"sealed_bytes_after":0}
```

## Broker

### `POST /broker/topics`

Creates or ensures a topic.

Request:

```json
{
  "name": "orders",
  "partitions": 1
}
```

`partitions` is optional. Default is `8`. Values outside `1..256` fall back to
`8`.

Response:

```json
{"ok":true,"topic":"orders","partitions":1}
```

### `POST /broker/topics/:topic/produce`

Produces one message to a topic.

Query parameters:

| Name | Required | Description |
|---|---:|---|
| `partition` | recommended | Target partition. If omitted, Ayder may choose default routing. |
| `key` | no | Optional message key. URL-decoded. |
| `idempotency_key` | no | Deduplicates repeated produce attempts. URL-decoded. |
| `timeout_ms` | no | Wait up to this many milliseconds for sealed sync. |
| `timing=1` | no | Include server timing fields for non-idempotent produce. |

Request body is the raw message bytes. It can be JSON, text, or binary.

Example:

```bash
curl -fsS -X POST \
  "$BASE/broker/topics/orders/produce?partition=0&timeout_ms=5000&idempotency_key=evt_001" \
  -H "$AUTH" -H 'Content-Type: application/json' \
  --data-binary '{"event_id":"evt_001"}'
```

Non-idempotent response:

```json
{
  "ok": true,
  "offset": 0,
  "partition": 0,
  "batch_id": 1,
  "sealed": true,
  "durable": true,
  "mode": "sealed",
  "synced": true
}
```

Idempotent first-write response:

```json
{
  "ok": true,
  "offset": 0,
  "partition": 0,
  "batch_id": 1,
  "sealed": true,
  "synced": true,
  "duplicate": false
}
```

Idempotent duplicate response:

```json
{
  "ok": true,
  "offset": 0,
  "partition": 0,
  "sealed": true,
  "synced": null,
  "duplicate": true
}
```

Operational note: `idempotency_key` deduplicates the broker write. It does not
replace application-level idempotency for external side effects.

### `POST /broker/topics/:topic/produce-ndjson`

Produces multiple messages from newline-delimited input.

Query parameters:

| Name | Required | Description |
|---|---:|---|
| `partition` | recommended | Target partition. |
| `idempotency_key` | no | Deduplicates the whole batch. |
| `timeout_ms` | no | Wait for sealed sync. |

Input rules:

- blank lines are ignored
- up to 1000 nonblank lines are processed
- JSON string lines are unwrapped, so `"hello"` becomes `hello`
- object/array/numeric/bool/null lines are stored as their original bytes

Response:

```json
{
  "ok": true,
  "first_offset": 0,
  "count": 2,
  "partition": 0,
  "batch_id": 2,
  "sealed": true,
  "durable": true,
  "mode": "sealed",
  "synced": true
}
```

### `GET /broker/consume/:topic/:group/:partition`

Consumes messages for a consumer group.

Query parameters:

| Name | Default | Description |
|---|---:|---|
| `offset` | committed offset + 1, or 0 if no commit | Client-facing start offset. `offset=0` returns offset 0. |
| `limit` | `100` | Max messages, capped at `1000`. |
| `encoding=b64` | text mode | Return `value_b64` and `key_b64` for binary-safe payloads. |
| `linearizable_timeout_ms` | `3000` in HA | HA read-index timeout. |
| `timeout_ms` | `3000` in HA | Alias for HA read-index timeout. |

Response with base64:

```json
{
  "messages": [
    {
      "offset": 0,
      "partition": 0,
      "value_b64": "eyJldmVudF9pZCI6ImV2dF8wMDEifQ=="
    }
  ],
  "count": 1,
  "next_offset": 1,
  "committed_offset": null,
  "truncated": false
}
```

If `encoding=b64` is omitted, values are JSON-escaped strings and may be
truncated to fit internal response buffers. Prefer `encoding=b64`.

### `POST /broker/commit`

Commits a consumer offset.

Request:

```json
{
  "topic": "orders",
  "group": "order_worker",
  "partition": 0,
  "offset": 0
}
```

Response:

```json
{"ok":true}
```

Commit discipline for recovery-sensitive paths:

1. Consume event.
2. Apply or decide application effects.
3. Write the application effect audit.
4. Commit the Ayder offset.

### `POST /broker/query`

Legacy-compatible query endpoint. Scans JSON messages from a topic, filters
them, optionally emits projected rows, and counts aggregates.

### `QUERY /broker/query`

RFC 10008-style query endpoint. This is the preferred HTTP-native shape for
safe, idempotent read-only queries with request content.

Requirements:

- `Content-Type: application/json`
- request body must be a JSON object

Responses include:

```http
Accept-Query: application/json
```

Example:

```bash
curl -fsS -X QUERY "$BASE/broker/query" \
  -H "$AUTH" -H 'Content-Type: application/json' \
  --data-binary @query.json
```

Request:

```json
{
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
}
```

Source fields:

- `topic`: string, required
- `partition`: number, required
- `group`: string, optional
- `offset`: number, optional
- `limit`: integer, optional, default `200`, max `2000`

Filter operators:

- `eq`
- `ne`
- `gt`
- `ge`
- `lt`
- `le`

Response:

```json
{
  "ok": true,
  "rows": [],
  "aggregates": [
    {"key": "kind=order", "count": 3}
  ],
  "source": {
    "topic": "orders",
    "group": "query_worker",
    "partition": 0,
    "next_offset": 3,
    "consumed": 3
  }
}
```

### `POST /broker/join`

Performs a bounded windowed join over JSON messages from two topics.

Request:

```json
{
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
    "max_rows": 20,
    "missing_as": "empty"
  },
  "join": {
    "type": "inner"
  },
  "dedupe": {
    "mode": "once"
  }
}
```

Join key options:

- single key: `left_key`, `right_key`
- composite key: `left_keys`, `right_keys`

Join types:

- `inner`
- `left`
- `right`
- `full`

Response:

```json
{
  "ok": true,
  "rows": [
    {
      "key": "ord_123",
      "left_ts_ms": 123,
      "right_ts_ms": 124,
      "left": {"order_id": "ord_123", "amount": "42.00"},
      "right": {"status": "SUCCESS"}
    }
  ],
  "next_offsets": {
    "left": 1,
    "right": 1
  },
  "stats": {
    "left_scanned": 1,
    "right_scanned": 1,
    "rows": 1,
    "watermark_ms": 123,
    "outer_left": 0,
    "outer_right": 0,
    "dedupe_once": true
  }
}
```

### `POST /broker/retention`

Sets TTL and/or max-bytes retention.

Request:

```json
{
  "topic": "orders",
  "partition": 0,
  "ttl_ms": 600000,
  "max_bytes": 1048576,
  "sealed": {
    "max_age_ms": 86400000,
    "max_bytes": 1073741824
  }
}
```

Notes:

- `topic` can be a topic name or `"*"`.
- `partition` is optional. `-1` means all partitions for the topic.
- `ttl_ms` and `max_bytes` are optional, but at least one retention field or
  `sealed` action should be meaningful.

Response:

```json
{
  "ok": true,
  "ttl_applied": true,
  "size_applied": true,
  "sealed_bytes_after": 0
}
```

### `POST /broker/delete-before`

Deletes messages before an offset.

Request:

```json
{
  "topic": "orders",
  "partition": 0,
  "before_offset": 100
}
```

Response:

```json
{
  "ok": true,
  "deleted_count": 100,
  "freed_bytes": 12345
}
```

## KV

Paths use `:key+`, so keys may contain slashes:

```text
/kv/app/customer/123
```

Here `ns=app` and `key=customer/123`.

### `POST /kv/:ns/:key+`

Puts a binary value.

Query parameters:

| Name | Description |
|---|---|
| `cas` | Optional compare-and-set guard. |
| `ttl_ms` | Optional value TTL. |
| `timeout_ms` | Optional sealed sync wait. |

Response:

```json
{
  "ok": true,
  "cas": 1,
  "sealed": true,
  "durable": true,
  "mode": "sealed",
  "synced": true,
  "batch_id": 3
}
```

CAS mismatch:

```json
{
  "ok": false,
  "error": "cas_mismatch",
  "expected": 1,
  "current": 2
}
```

### `GET /kv/:ns/:key+`

Returns a base64 value and CAS:

```json
{
  "value": "Y3VzdG9tZXItc3RhdGU=",
  "cas": 1
}
```

### `GET /kv/:ns/:key+/meta`

Returns CAS and remaining TTL:

```json
{
  "cas": 1,
  "ttl_ms": 599000
}
```

### `DELETE /kv/:ns/:key+`

Deletes a value by writing a tombstone.

Query parameters:

| Name | Description |
|---|---|
| `cas` | Optional compare-and-set guard. |
| `timeout_ms` | Optional sealed sync wait. |

Response:

```json
{
  "ok": true,
  "deleted": true,
  "sealed": true,
  "durable": true,
  "mode": "sealed",
  "synced": true,
  "batch_id": 4
}
```

If the key is absent:

```json
{"ok":true,"deleted":false}
```

## HA Membership

These routes require HA mode. On a single non-HA node they can return:

```json
{"ok":false,"error":"ha_not_enabled","message":"HA membership is not available on this node"}
```

### `GET /ha/membership`

Returns membership epoch, masks, and node list.

### `POST /ha/membership/add`

Adds a learner.

Request:

```json
{
  "node_id": "node-4",
  "advertise_addr": "127.0.0.1:7704",
  "http_addr": "http://127.0.0.1:1104",
  "priority": 10
}
```

`http_addr` is optional. If omitted, Ayder derives it from `advertise_addr`
using `port + 1`.

Response:

```json
{"ok":true,"index":3,"node_id":"node-4","role":"learner"}
```

### `POST /ha/membership/promote`

Promotes a learner to voter.

Request:

```json
{"node_id":"node-4"}
```

Response:

```json
{"ok":true,"node_id":"node-4","role":"voter"}
```

### `POST /ha/membership/remove`

Removes a member.

Request:

```json
{"node_id":"node-4"}
```

Response:

```json
{"ok":true,"node_id":"node-4","removed":true}
```

Common HA mutation errors:

- `change_in_progress`
- `no_quorum`
- `not_leader`
- `commit_timeout`

## Verification

Run the route-coverage smoke test:

```bash
START_SERVER=1 SERVER_BIN="$PWD/ayder" PORT=1109 tests/smoke/api_smoke.sh
```

It verifies every route registered in `src/app_routes.c`.
