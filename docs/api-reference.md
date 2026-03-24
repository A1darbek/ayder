# API Reference

Base URL examples:

- single node: `http://127.0.0.1:1109`
- HA node: `http://127.0.0.1:7001`

Auth header for most endpoints:

```http
Authorization: Bearer dev
```

Public endpoints:

- `GET /health`
- `GET /ready`
- `GET /metrics`
- `GET /metrics_ha`

## Health And Readiness

### `GET /health`

Returns fast liveness only.

Response:

```json
{"ok":1}
```

### `GET /ready`

Returns whether persistence is ready for writes.

Responses:

```json
{"ready":true}
```

```json
{"ready":false}
```

## Broker

### `POST /broker/topics`

Create a topic.

Request:

```bash
curl -fsS -X POST "$BASE/broker/topics" \
  -H "$AUTH" \
  -H 'Content-Type: application/json' \
  -d '{"name":"events","partitions":1}'
```

Success:

```json
{"ok":true,"topic":"events","partitions":1}
```

Notes:

- `partitions` defaults to `8` if omitted or invalid.
- practical accepted range in code: `1..256`

### `POST /broker/topics/{topic}/produce?partition={p}`

Append one message.

Example:

```bash
curl -fsS -X POST "$BASE/broker/topics/events/produce?partition=0&timeout_ms=5000" \
  -H "$AUTH" \
  -H 'Content-Type: text/plain' \
  --data-binary 'hello world'
```

Typical success:

```json
{"ok":true,"offset":0,"partition":0,"batch_id":123,"sealed":true,"durable":true,"mode":"sealed","synced":true}
```

Useful query params:

- `partition`: partition number
- `timeout_ms`: wait for local sync up to this timeout
- `idempotency_key`: dedupe repeated writes for the same key
- `key`: optional message key
- `timing=1`: include server timing fields in the response

HA behavior:

- on non-leader: can return `not_leader` and a `leader_url`
- without fresh quorum: can reject the write

### `POST /broker/topics/{topic}/produce-ndjson?partition={p}`

Append a batch from newline-delimited input.

Example:

```bash
printf '%s\n' 'one' 'two' 'three' | \
curl -fsS -X POST "$BASE/broker/topics/events/produce-ndjson?partition=0&timeout_ms=5000" \
  -H "$AUTH" \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @-
```

Typical success:

```json
{"ok":true,"first_offset":0,"count":3,"partition":0,"batch_id":123,"sealed":true,"durable":true,"mode":"sealed","synced":true}
```

Notes:

- generic path caps parsing at about `1000` lines per request in the handler
- whole-batch idempotency is supported through `idempotency_key`

### `GET /broker/consume/{topic}/{group}/{partition}`

Read messages.

Example:

```bash
curl -fsS "$BASE/broker/consume/events/mygroup/0?offset=0&limit=10&encoding=b64" \
  -H "$AUTH"
```

Typical success:

```json
{"messages":[{"offset":0,"partition":0,"value_b64":"aGVsbG8gd29ybGQ="}],"count":1,"next_offset":1,"committed_offset":null,"truncated":false}
```

Useful query params:

- `offset`: inclusive starting offset; `0` starts at the beginning
- `limit`: max messages, up to `1000`
- `encoding=b64`: safer for binary payloads

Notes:

- if `offset` is omitted, Ayder starts from the committed offset for that group when one exists
- in HA mode, consume is leader-only and linearizable

### `POST /broker/commit`

Store a consumer offset.

Request:

```bash
curl -fsS -X POST "$BASE/broker/commit" \
  -H "$AUTH" \
  -H 'Content-Type: application/json' \
  -d '{"topic":"events","group":"mygroup","partition":0,"offset":0}'
```

Success:

```json
{"ok":true}
```

Required JSON fields:

- `topic` string
- `group` string
- `partition` int
- `offset` int

## KV

### `POST /kv/{namespace}/{key}?ttl_ms={n}&cas={n}`

Write a KV value.

Example:

```bash
printf 'value-1' | \
curl -fsS -X POST "$BASE/kv/app/config?ttl_ms=60000&timeout_ms=5000" \
  -H "$AUTH" \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @-
```

Typical success:

```json
{"ok":true,"cas":1,"sealed":true,"durable":true,"mode":"sealed","synced":true,"batch_id":123}
```

Notes:

- CAS is optional but strongly recommended for concurrent writers
- oversized values return `payload_too_large`

### `GET /kv/{namespace}/{key}`

Read a KV value.

Example:

```bash
curl -fsS "$BASE/kv/app/config" -H "$AUTH"
```

Success:

```json
{"value":"dmFsdWUtMQ==","cas":1}
```

Notes:

- values are returned as base64
- expired or deleted keys return `not_found`

### `GET /kv/{namespace}/{key}/meta`

Read CAS and TTL metadata.

Example:

```bash
curl -fsS "$BASE/kv/app/config/meta" -H "$AUTH"
```

Success:

```json
{"cas":1,"ttl_ms":58234}
```

### `DELETE /kv/{namespace}/{key}?cas={n}`

Delete a key with an optional CAS precondition.

Example:

```bash
curl -fsS -X DELETE "$BASE/kv/app/config?cas=1&timeout_ms=5000" -H "$AUTH"
```

Success:

```json
{"ok":true,"deleted":true,"sealed":true,"durable":true,"mode":"sealed","synced":true,"batch_id":124}
```

## Metrics

### `GET /metrics`

Prometheus metrics for HTTP, storage, fsync, and durability counters.

### `GET /metrics_ha`

HA-specific metrics used by the local runbooks and Jepsen pre-flight checks.

## Common Errors

- `401 invalid_token`: missing or invalid bearer token
- `503 sealed_only`: writes disabled until persistence becomes ready
- `not_leader`: send the broker request to the reported leader URL
- `cas_mismatch`: the KV value changed since your last read
- `produce_failed` or `commit_failed`: topic, partition, or storage issue
