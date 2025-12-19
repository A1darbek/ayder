# Ayder

**HTTP-native durable event log / message bus — written in C**

A single-binary event streaming system where `curl` is your client. No JVM, no ZooKeeper, no thick client libraries.

```bash
# Produce
curl -X POST 'localhost:1109/broker/topics/orders/produce' \
  -H 'Authorization: Bearer dev' \
  -d '{"item":"widget"}'

# Consume
curl 'localhost:1109/broker/consume/orders/mygroup/0?encoding=b64' \
  -H 'Authorization: Bearer dev'
```

---

## Why Ayder?

| | Kafka | Redis Streams | Ayder |
|---|-------|---------------|-------|
| **Protocol** | Binary (requires thick client) | RESP | HTTP (curl works) |
| **Durability** | ✅ Replicated log | ⚠️ Async replication, no quorum | ✅ Raft consensus (sync-majority) |
| **Operations** | ZooKeeper/KRaft + JVM tuning | Single node or Redis Cluster | Single binary, zero dependencies |
| **Latency (replicated)** | 10-50ms P99 | N/A (async only) | 3.3ms P99 |
| **First message** | ~30 min setup | ~5 min setup | ~60 seconds |

**Kafka** is battle-tested but operationally heavy. JVM tuning, partition rebalancing, and config sprawl add up.

**Redis Streams** is simple and fast, but replication is async-only — no majority quorum, no strong durability guarantees.

**Ayder** sits in the middle: Kafka-grade durability (Raft sync-majority) with Redis-like simplicity (single binary, HTTP API).

---

## What You Get

- **Append-only logs** with per-partition offsets
- **Consumer groups** with committed offsets
- **Durability** via sealed append-only files (AOF) + crash recovery
- **HA replication** with Raft consensus (3 / 5 / 7 node clusters)
- **KV store** with CAS and TTL
- **Streaming queries** with filters, aggregations, and windowed joins

---

## Performance

Benchmarked on DigitalOcean (8 vCPU AMD, 3-node Raft cluster, sync-majority writes):

| Metric | Value |
|--------|-------|
| Throughput | 50,000 msg/s sustained |
| P50 latency | 1.58 ms |
| P99 latency | 3.35 ms |
| P99.9 latency | 8.62 ms |
| Server-side P99.999 | 1.2 ms |

All writes are durable and replicated to 2/3 nodes before acknowledgment.

---

## Quick Start

### Docker (fastest)

```bash
# Clone and run with Docker Compose (includes Prometheus + Grafana)
git clone https://github.com/A1darbek/ayder.git
cd ayder
docker compose up -d --build

# Or build and run standalone
docker build -t ayder .
docker run -p 1109:1109 --shm-size=2g ayder

# That's it. Now produce:
curl -X POST localhost:1109/broker/topics \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"name":"events","partitions":4}'

curl -X POST 'localhost:1109/broker/topics/events/produce' \
  -H 'Authorization: Bearer dev' \
  -d 'hello world'
```

### From Source

```bash
# Dependencies: libuv 1.51+, openssl, zlib, liburing
make clean && make
./ayder --port 1109
```

### Docker Compose Stack

The included `docker-compose.yml` brings up:
- **Ayder** on port `1109`
- **Prometheus** on port `9090` (metrics scraping)
- **Grafana** on port `3000` (dashboards, default password: `admin`)

```yaml
# docker-compose.yml
services:
  ayder:
    build: .
    ports:
      - "1109:1109"
    shm_size: 2g
    environment:
      - RF_BEARER_TOKENS=dev
```

---

## Usage Examples

```bash
# Create a topic
curl -X POST localhost:1109/broker/topics \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"name":"events","partitions":8}'

# Produce a message
curl -X POST 'localhost:1109/broker/topics/events/produce?partition=0' \
  -H 'Authorization: Bearer dev' \
  -d 'hello world'

# Consume messages (binary-safe with base64)
curl 'localhost:1109/broker/consume/events/mygroup/0?limit=10&encoding=b64' \
  -H 'Authorization: Bearer dev'

# Commit offset
curl -X POST localhost:1109/broker/commit \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"topic":"events","group":"mygroup","partition":0,"offset":10}'
```

---

## Core Concepts

### Topics and Partitions

A **topic** contains N **partitions**. Each partition is an independent append-only log with its own offset sequence.

### Consumer Groups

Consumers read from `/broker/consume/{topic}/{group}/{partition}`. Progress is tracked per `(topic, group, partition)` tuple via explicit commits.

If you consume without specifying `?offset=`, Ayder resumes from the last committed offset for that consumer group.

### Write Durability

Ayder acknowledges writes in two modes:

| Mode | `batch_id` | `durable` | Description |
|------|-----------|-----------|-------------|
| **Sealed** | Non-zero | `true` | Appended to AOF, survives crashes |
| **Rocket** | Zero | `false` | In-memory fast path, not persisted |

Use `timeout_ms` to wait for sync confirmation.

---

## API Reference

### Health and Metrics

```bash
GET  /health      → {"ok":true}
GET  /ready       → {"ready":true}
GET  /metrics     → Prometheus format
GET  /metrics_ha  → HA cluster metrics
```

### Topic Management

**Create topic**
```bash
POST /broker/topics
{"name":"events","partitions":8}
```

Response:
```json
{"ok":true,"topic":"events","partitions":8}
```

### Produce

**Single message** (raw bytes in body)
```
POST /broker/topics/{topic}/produce
```

Query parameters:
| Parameter | Description |
|-----------|-------------|
| `partition` | Target partition (optional; auto-assigned if omitted) |
| `key` | Message key, URL-encoded (optional) |
| `idempotency_key` | Deduplication key, URL-encoded (optional) |
| `timeout_ms` | Wait for sync confirmation (optional) |
| `timing` | Set to `1` to include timing breakdown (optional) |

Response:
```json
{
  "ok": true,
  "offset": 123,
  "partition": 0,
  "batch_id": 9991,
  "sealed": true,
  "durable": true,
  "mode": "sealed",
  "synced": true
}
```

Duplicate detection (when `idempotency_key` matches):
```json
{"ok":true,"offset":123,"partition":0,"sealed":true,"synced":null,"duplicate":true}
```

**Batch produce** (NDJSON — one message per line)
```
POST /broker/topics/{topic}/produce-ndjson
```

Response:
```json
{
  "ok": true,
  "first_offset": 1000,
  "count": 250,
  "partition": 0,
  "batch_id": 424242,
  "sealed": true,
  "durable": true,
  "mode": "sealed",
  "synced": false
}
```

### Consume

```
GET /broker/consume/{topic}/{group}/{partition}
```

Query parameters:
| Parameter | Description |
|-----------|-------------|
| `offset` | Start offset, inclusive (resumes from commit if omitted) |
| `limit` | Max messages to return (default: 100, max: 1000) |
| `encoding` | Set to `b64` for binary-safe base64 encoding |

Response:
```json
{
  "messages": [
    {"offset": 0, "partition": 0, "value_b64": "aGVsbG8=", "key_b64": "a2V5"}
  ],
  "count": 1,
  "next_offset": 1,
  "committed_offset": 0,
  "truncated": false
}
```

Use `next_offset` as the `?offset=` parameter for subsequent reads.

### Commit

```
POST /broker/commit
{"topic":"events","group":"g1","partition":0,"offset":124}
```

Response:
```json
{"ok":true}
```

Commits are stored per `(topic, group, partition)`. Backward commits are ignored.

### Retention

**Delete before offset** (hard floor)
```
POST /broker/delete-before
{"topic":"events","partition":0,"before_offset":100000}
```

Response:
```json
{"ok":true,"deleted_count":12345,"freed_bytes":987654}
```

**Set retention policy**
```
POST /broker/retention
```

Examples:
```json
// TTL + size cap for specific partition
{"topic":"events","partition":0,"ttl_ms":60000,"max_bytes":104857600}

// TTL for all topics
{"topic":"*","ttl_ms":300000}
```

---

## KV Store

Ayder includes a key-value store with CAS (compare-and-swap) and TTL support.

**Put**
```
POST /kv/{namespace}/{key}?cas=<u64>&ttl_ms=<u64>
```

Body contains raw value bytes.

Response:
```json
{"ok":true,"cas":2,"sealed":true,"durable":true,"mode":"sealed","synced":true,"batch_id":123}
```

**Get**
```
GET /kv/{namespace}/{key}
```

Response:
```json
{"value":"<base64>","cas":2}
```

**Get metadata**
```
GET /kv/{namespace}/{key}/meta
```

Response:
```json
{"cas":2,"ttl_ms":12345}
```

**Delete**
```
DELETE /kv/{namespace}/{key}?cas=<u64>
```

Response:
```json
{"ok":true,"deleted":true,"sealed":true,"durable":true,"mode":"sealed","synced":false,"batch_id":456}
```

---

## Streaming Queries (Experimental)

### Query

```
POST /broker/query
```

Consume JSON objects from a topic/partition with:
- Row filtering
- `group_by` with aggregations
- Field projection

### Join

```
POST /broker/join
```

Windowed join between two sources:
- Join types: inner / left / right / full
- Window size and allowed lateness
- Optional `dedupe_once`
- Field projection

---

## HA Clustering

Ayder supports **3, 5, or 7 node clusters** with Raft-based replication.

### Write Modes

| Mode | Acknowledgment |
|------|----------------|
| `async` | Leader appends locally, replicates in background |
| `sync-majority` | Waits for majority (e.g., 2/3 nodes) |
| `sync-all` | Waits for all nodes |

### Redirect Behavior

Writes must go to the leader. If you send a write to a follower, it returns an HTTP redirect with the leader's address in the `Location` header.

Options:
1. Follow redirects automatically
2. Discover the leader via `/metrics_ha` and pin writes to it

### Follower Recovery

When a follower rejoins after downtime:

1. Replays local AOF
2. Connects to leader
3. Requests missing offsets
4. Leader streams missing data
5. Follower catches up automatically

Example scenario:

```bash
# 3-node cluster
node1 (7001) = LEADER
node2 (8001) = FOLLOWER
node3 (9001) = FOLLOWER

# Write to leader
curl -X POST 'localhost:7001/broker/topics/test/produce' \
  -H 'Authorization: Bearer dev' -d 'msg-0'
# → offset 0

# Kill node2
kill -9 $(pgrep -f "port 8001")

# Write while node2 is down
curl -X POST 'localhost:7001/broker/topics/test/produce' \
  -H 'Authorization: Bearer dev' -d 'msg-1'
# → offset 1

curl -X POST 'localhost:7001/broker/topics/test/produce' \
  -H 'Authorization: Bearer dev' -d 'msg-2'
# → offset 2

# Restart node2 — automatically catches up to offset 2

# Verify all data is present on recovered node
curl 'localhost:8001/broker/consume/test/g1/0?offset=0&limit=10' \
  -H 'Authorization: Bearer dev'
# → offsets 0, 1, 2 all present
```

---

## Running

### Single Node

```bash
# Default port is 1109
./ayder --port 1109

# Or specify custom port
./ayder --port 7001
```

### HA Cluster (3/5/7 nodes)

Ayder uses Raft for consensus. Here's a complete 3-node setup:

**Environment variables:**

| Variable | Description |
|----------|-------------|
| `RF_HA_ENABLED` | Enable HA mode (`1`) |
| `RF_HA_NODE_ID` | Unique node identifier |
| `RF_HA_NODES` | Cluster topology: `id:host:raft_port:priority,...` |
| `RF_HA_BOOTSTRAP_LEADER` | Set to `1` on initial leader only |
| `RF_HA_WRITE_CONCERN` | Nodes to wait for: `1`=leader only, `2`=majority, `N`=all |
| `RF_HA_DEDICATED_WORKER` | **Set to `0` for best P99 latency** (highly recommended) |
| `RF_HA_TLS` | Enable mTLS for Raft (`1`) |
| `RF_HA_TLS_CA` | Path to CA certificate |
| `RF_HA_TLS_CERT` | Path to node certificate |
| `RF_HA_TLS_KEY` | Path to node private key |
| `RF_BEARER_TOKENS` | HTTP auth tokens (format: `token1@scope:token2:...`) |

**3-Node Example:**

```bash
# Node 1 (bootstrap leader)
export RF_HA_ENABLED=1
export RF_HA_NODE_ID=node1
export RF_HA_BOOTSTRAP_LEADER=1
export RF_HA_NODES='node1:10.0.0.1:7000:100,node2:10.0.0.2:8000:50,node3:10.0.0.3:9000:25'
export RF_HA_WRITE_CONCERN=2  # sync-majority (2/3 nodes)
export RF_HA_DEDICATED_WORKER=0  # critical for low P99
export RF_BEARER_TOKENS='dev@scope:token2:token3'
export RF_HA_TLS=1
export RF_HA_TLS_CA=./certs/ca.crt
export RF_HA_TLS_CERT=./certs/node1.crt
export RF_HA_TLS_KEY=./certs/node1.key
./ayder --port 7001

# Node 2
export RF_HA_ENABLED=1
export RF_HA_NODE_ID=node2
export RF_HA_NODES='node1:10.0.0.1:7000:100,node2:10.0.0.2:8000:50,node3:10.0.0.3:9000:25'
export RF_HA_WRITE_CONCERN=2
export RF_HA_DEDICATED_WORKER=0
export RF_BEARER_TOKENS='dev@scope:token2:token3'
export RF_HA_TLS=1
export RF_HA_TLS_CA=./certs/ca.crt
export RF_HA_TLS_CERT=./certs/node2.crt
export RF_HA_TLS_KEY=./certs/node2.key
./ayder --port 8001

# Node 3 (same pattern, port 9001)
```

**5-Node and 7-Node Clusters:**

```bash
# 5-node topology
export RF_HA_NODES='node1:host1:7000:100,node2:host2:8000:80,node3:host3:9000:60,node4:host4:10000:40,node5:host5:11000:20'
export RF_HA_WRITE_CONCERN=3  # majority of 5

# 7-node topology
export RF_HA_NODES='node1:host1:7000:100,node2:host2:8000:90,node3:host3:9000:80,node4:host4:10000:70,node5:host5:11000:60,node6:host6:12000:50,node7:host7:13000:40'
export RF_HA_WRITE_CONCERN=4  # majority of 7
```

**Generate TLS certificates:**

```bash
# Create CA
openssl req -x509 -newkey rsa:4096 -keyout ca.key -out ca.crt \
  -days 365 -nodes -subj "/CN=ayder-ca"

# Create node certificate (repeat for each node)
openssl req -newkey rsa:2048 -nodes -keyout node1.key -out node1.csr \
  -subj "/CN=node1" -addext "subjectAltName=DNS:node1,IP:10.0.0.1"

openssl x509 -req -in node1.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out node1.crt -days 365 -copy_extensions copy
```

**Write concern tradeoffs:**

| `RF_HA_WRITE_CONCERN` | Durability | Latency | Survives |
|-----------------------|------------|---------|----------|
| `1` | Low | ~1ms | Nothing (leader only) |
| `2` (3-node) | High | ~3ms | 1 node failure |
| `3` (5-node) | High | ~3ms | 2 node failures |
| `N` (all nodes) | Maximum | Higher | N-1 failures, but blocks if any node slow |

---

## Error Responses

Errors follow a consistent format:

```json
{
  "ok": false,
  "error": "missing_topic",
  "message": "Topic name is required",
  "docs": "https://ayder.dev/docs/api/produce"
}
```

---

## What Ayder Is

✅ HTTP-native event log with partitions and offsets  
✅ Fast writes with cursor-based consumption  
✅ Durable with crash recovery  
✅ Horizontally scalable with Raft replication

## What Ayder Is Not (Yet)

❌ Kafka protocol compatible  
❌ A SQL database  
❌ Magic exactly-once without client-side idempotency discipline

---

## License

Apache 2.0 license 



