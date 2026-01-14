# Ayder

**HTTP-native durable event log / message bus — written in C**

Single binary. HTTP API. `curl` works as a client. No JVM, no ZooKeeper, no client libraries to start sending events.

▶️ **1-minute demo: SIGKILL → restart → data still there**
https://www.youtube.com/watch?v=c-n0X5t-A9Y

```bash
# Produce (raw bytes in body)
curl -X POST 'localhost:1109/broker/topics/orders/produce?partition=0' \
  -H 'Authorization: Bearer dev' \
  -d '{"item":"widget"}'

# Consume (base64 for binary-safe payloads)
curl 'localhost:1109/broker/consume/orders/mygroup/0?encoding=b64' \
  -H 'Authorization: Bearer dev'

Looking for 2–3 design partners to try Ayder in real workloads.
30-minute setup, white-glove onboarding. See Discussions or email me.
```

---

## Why Ayder?

### Benchmarks (3-node Raft, sync-majority, real network)
- **wrk2 (rate limited 50K req/s):** 49,871 msg/s, **client p99 3.46ms**
- **server timing (handler only):** **p99.999 1.22ms** (`server_us`, see output below)
- **unclean recovery (SIGKILL):** follower catch-up + healthy cluster in **~40–50s** (~8M offsets)


| | Kafka | Redis Streams | Ayder |
|---|-------|---------------|-------|
| **Protocol** | Binary (requires thick client) | RESP | HTTP (curl works) |
| **Durability** | ✅ Replicated log | ⚠️ Replication depends on Redis topology; quorum semantics differ from a consensus log | ✅ Raft consensus (sync-majority) |
| **Operations** | ZooKeeper/KRaft + JVM tuning | Single node or Redis Cluster | Single binary, zero dependencies |
| **Latency (P99)** | 10-50ms | N/A (async only) | 3.5ms |
| **Recovery time** | Can be long in large clusters (unclean shutdown; reports vary) | Minutes | **40-50 seconds** |
| **First message** | ~30 min setup | ~5 min setup | ~60 seconds |

**Kafka** is battle-tested but operationally heavy. JVM tuning, partition rebalancing, and config sprawl add up.

**Redis Streams** is simple and fast, but replication is async-only — no majority quorum, no strong durability guarantees.

**Ayder** sits in the middle: Kafka-grade durability (Raft sync-majority) with Redis-like simplicity (single binary, HTTP API). Think of it as what Nginx did to Apache — same pattern applied to event streaming.

---

## What You Get

- **Append-only logs** with per-partition offsets
- **Consumer groups** with committed offsets
- **Durability** via sealed append-only files (AOF) + crash recovery
- **HA replication** with Raft consensus (3 / 5 / 7 node clusters)
- **KV store** with CAS and TTL
- **Stream processing** with filters, aggregations, and windowed joins (including cross-format Avro+Protobuf joins)

---

## Performance

All benchmarks below are run over real network (not loopback). Commands and full outputs are included.


### Production Benchmark: 3-Node Cluster (Real Network)

**Setup:**
- 3-node Raft cluster on DigitalOcean (8 vCPU AMD)
- Sync-majority writes (2/3 nodes confirm before ACK)
- 64B payload
- Separate machines, real network

#### wrk2 (Rate-Limited 50K req/s) — Latency Test

| Metric | Client-side | Server-side |
|--------|-------------|-------------|
| **Throughput** | 49,871 msg/s | — |
| **P50** | 1.60ms | — |
| **P99** | 3.46ms | — |
| **P99.9** | 12.94ms | — |
| **P99.999** | 154.49ms | **1.22ms** |

**Server-side breakdown at P99.999:**
```
Handler:     1.22ms
Queue wait:  0.47ms
HTTP parse:  0.41ms
```

The 154ms client-side tail is network/kernel scheduling — the broker itself stays under 2ms even at P99.999. **HTTP is not the bottleneck.**

#### wrk (Max Throughput) — Throughput Test

| Metric | Value |
|--------|-------|
| **Throughput** | 93,807 msg/s |
| **P50** | 3.78ms |
| **P99** | 10.22ms |
| **Max** | 224.51ms |

<details>
<summary>Full wrk output (3-node cluster, max throughput)</summary>

```
Running 1m test @ http://10.114.0.3:8001
  12 threads and 400 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     4.26ms    2.97ms 224.51ms   93.76%
    Req/Sec     7.86k     1.19k   13.70k    67.61%
  Latency Distribution
     50%    3.78ms
     75%    4.93ms
     90%    6.44ms
     99%   10.22ms
  5634332 requests in 1.00m, 2.99GB read
Requests/sec:  93807.95
Transfer/sec:     50.92MB
```
</details>

<details>
<summary>Full wrk2 output (3-node cluster, rate-limited)</summary>

```
Running 1m test @ http://10.114.0.2:9001
  12 threads and 400 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     1.72ms    1.19ms 216.19ms   96.39%
    Req/Sec     4.35k     1.17k    7.89k    79.58%
  Latency Distribution (HdrHistogram - Recorded Latency)
 50.000%    1.60ms
 75.000%    2.03ms
 90.000%    2.52ms
 99.000%    3.46ms
 99.900%   12.94ms
 99.990%   31.76ms
 99.999%  154.49ms
100.000%  216.32ms

  2991950 requests in 1.00m, 1.80GB read
Requests/sec:  49871.12

SERVER  server_us p99.999=1219us (1.219ms)
SERVER  queue_us p99.999=473us (0.473ms)
SERVER  recv_parse_us p99.999=411us (0.411ms)
```
</details>

---

### ARM64 Benchmark: Snapdragon X Elite (WSL2)

Ayder runs natively on ARM64. Here's a benchmark on consumer hardware:

**Setup:**
- Snapdragon X Elite laptop (1.42 kg)
- WSL2 Ubuntu, 16GB RAM
- Running on **battery** (unplugged)
- 3-node Raft cluster (same machine — testing code efficiency)
- wrk: 12 threads, 400 connections, 60 seconds

| Metric | Client-side | Server-side |
|--------|-------------|-------------|
| **Throughput** | 106,645 msg/s | — |
| **P50** | 3.57ms | — |
| **P99** | 7.62ms | — |
| **P99.999** | 250.84ms | **0.65ms** |

**Server-side breakdown at P99.999:**
```
Handler:     0.65ms
Queue wait:  0.29ms
HTTP parse:  0.29ms
```

**Comparison: Snapdragon vs Cloud VMs (Server-side P99.999)**

| Environment | Throughput | Server P99.999 | Hardware |
|-------------|------------|----------------|----------|
| **Snapdragon X Elite** (WSL2, battery) | 106,645/s | **0.65ms** | 1.42kg laptop |
| **DigitalOcean** (8-vCPU AMD, 3 VMs) | 93,807/s | 1.22ms | Cloud infrastructure |

The laptop's server-side latency is **47% faster** while handling **14% more throughput** — on battery, in WSL2.

**What this proves:**
- ARM64 is ready for server workloads
- Efficient C code runs beautifully on Snapdragon
- WSL2 overhead is minimal for async I/O
- You can test full HA clusters on your laptop

<details>
<summary>Full wrk output (Snapdragon X Elite)</summary>

```
Running 1m test @ http://172.31.76.127:7001
  12 threads and 400 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     3.81ms    3.80ms 289.49ms   99.00%
    Req/Sec     8.94k     1.16k   22.81k    80.11%
  Latency Distribution
     50%    3.57ms
     75%    4.01ms
     90%    4.51ms
     99%    7.62ms
  6408525 requests in 1.00m, 3.80GB read
Requests/sec: 106645.65
Transfer/sec:     64.83MB

CLIENT  p99.999=250843us (250.843ms)  max=289485us (289.485ms)
SERVER  server_us p99.999=651us (0.651ms)  max=11964us (11.964ms)
SERVER  queue_us p99.999=285us (0.285ms)  max=3920us (3.920ms)
SERVER  recv_parse_us p99.999=293us (0.293ms)  max=4149us (4.149ms)
```
</details>

---

## Recovery Time

Recovery time is an area where operational complexity shows up quickly in practice.


| Scenario | Kafka | Ayder |
|----------|-------|-------|
| **Cluster restart (unclean)** | 2+ hours (reported in production) | **40-50 seconds** |
| **Broker sync after failure** | 181 minutes for 1TB data | Auto catch-up in seconds |
| **50+ broker rolling restart** | 2+ hours (2 min per broker) | N/A — single binary |

**Tested crash recovery:**

```bash
# 3-node cluster with 8 million offsets
1. SIGKILL a follower mid-write
2. Leader continues, follower misses offsets
3. Restart follower
4. Follower replays local AOF → asks leader for missing offsets
5. Leader streams missing data → follower catches up
6. Cluster fully healthy in 40-50 seconds
7. Zero data loss
```

No manual intervention. No partition reassignment. No ISR drama.

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

curl -X POST 'localhost:1109/broker/topics/events/produce?partition=0' \
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

## Interop / standards (direction)

I’d like Ayder’s consume API to converge toward something that works well with generic tooling.
One reference I’m looking at is Feed API (https://github.com/vippsas/feedapi-spec).
If you’ve implemented similar “log over HTTP” protocols in production, I’d love pointers.


---

## API Reference

### Health and Metrics

```bash
GET  /health      # → {"ok":true}
GET  /ready       # → {"ready":true}
GET  /metrics     # → Prometheus format
GET  /metrics_ha  # → HA cluster metrics
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

## Stream Processing

Built-in stream processing — no separate service required.

### Query

```
POST /broker/query
```

Consume JSON objects from a topic/partition with:
- Row filtering (eq, ne, lt, gt, in, contains)
- `group_by` with aggregations (count, sum, avg, min, max)
- Field projection
- Tumbling windows

### Join

```
POST /broker/join
```

Windowed join between two sources:
- Join types: inner / left / right / full
- Composite keys
- Window size and allowed lateness
- Optional `dedupe_once`
- Cross-format support (Avro + Protobuf in same join)

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
curl -X POST 'localhost:7001/broker/topics/test/produce?partition=0' \
  -H 'Authorization: Bearer dev' -d 'msg-0'
# → offset 0

# Kill node2
kill -9 $(pgrep -f "port 8001")

# Write while node2 is down
curl -X POST 'localhost:7001/broker/topics/test/produce?partition=0' \
  -H 'Authorization: Bearer dev' -d 'msg-1'
# → offset 1

curl -X POST 'localhost:7001/broker/topics/test/produce?partition=0' \
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

## Author

Built by **Aydarbek Romanuly** — solo founder from Kazakhstan 🇰🇿

- GitHub: [@A1darbek](https://github.com/A1darbek)
- Email: aidarbekromanuly@gmail.com

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
✅ Built-in stream processing with cross-format joins  
✅ ARM64-native (tested on Snapdragon X Elite)

## What Ayder Is Not (Yet)

❌ Kafka protocol compatible  
❌ A SQL database  
❌ Magic exactly-once without client-side idempotency discipline

---

## License

MIT





