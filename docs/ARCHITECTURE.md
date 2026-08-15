# Ayder architecture

This document describes the implementation at the Revenue Readiness v1 branch.
It is an engineering map, not a claim that every component is production-ready.

## Process model

The `ayder` parent parses configuration, creates shared-memory storage and
metrics, then forks HTTP workers. Workers use `SO_REUSEPORT`; each owns a libuv
event loop and accepts connections on the same listener. An optional dedicated
HA worker can be removed from HTTP service with `RF_HA_DEDICATED_WORKER`.

```text
HTTP clients
    |
    v
SO_REUSEPORT listener
    |
    +-- worker 0: parse -> auth/rate limit -> route -> broker/persistence
    +-- worker 1: parse -> auth/rate limit -> route -> broker/persistence
    +-- worker N: parse -> auth/rate limit -> route -> broker/persistence
            |                         |
            v                         v
     shared-memory state       sealed append + sync thread
            |                         |
            +----- worker bus --------+
                                      |
                                      v
                               append.aof.sealed
```

Shared storage coordinates items such as offset allocation and idempotency
indexes. Each worker also has local broker structures. The worker bus promotes
records so those local views converge. This design makes cross-worker state and
metric aggregation explicit correctness concerns; a response from one worker
must not infer another worker's local persistence watermark.

Topic publication uses process-shared pending/ready state, and mutation
acknowledgements wait for every local worker to apply the worker-bus record.
Focused regression observed 400/400 load-balanced reads, rejected an injected
worker-apply failure, and repaired every worker through replay after restart.
Longer saturation/fault evidence remains required.

## Request path

`src/http_server.c` owns connection handling, HTTP/1 framing, authentication,
rate limiting, backpressure, and response I/O. `src/router.c` matches a bounded
set of HTTP methods and path parameters. `src/app_routes.c` validates endpoint
inputs and calls the broker, persistence, QUERY, or HA integration layers.

HTTP is the product protocol. Revenue Readiness v1 does not add a private
binary protocol. See [AYDER_WIRE_DECISION.md](AYDER_WIRE_DECISION.md).

## Broker and replay

`src/rf_broker.c` implements topics, partitions, offsets, in-memory rings,
consumer offsets, idempotency indexes, replay, and the records written to the
append-only log. Offsets identify an ordered position within a partition; they
do not by themselves prove that the associated record reached stable storage.

The HTTP QUERY method is a read-only recovery inspection surface. It uses the
same broker state and replay evidence as ordinary consumers and does not infer
the outcome of an arbitrary external side effect.

Idempotency uses process-shared striped serialization, SHA-256 key/payload
digests, and durable `PENDING`/`COMMITTED` state. Same-key/same-payload retries
resolve one result; another payload conflicts. Broker/KV transaction paths
persist and obtain required HA/all-worker outcomes before publishing shared
state. Transport-unknown outcomes still require client reconciliation, and
none of this makes arbitrary external effects exactly once.

## Persistence and acknowledgement states

`src/aof_batch.c` encodes sealed records with a batch ID, timestamp, record
type, payload length, and CRC. A background sync thread writes ready records and
performs the durability barrier. Replay validates record boundaries and CRCs
before applying recognized records.

The response fields have deliberately separate meanings:

| Field | Meaning |
|---|---|
| `ok:true` | The handler accepted the operation. |
| `batch_id` | Local sealed-append identifier; it is not a cluster-global transaction ID. |
| `sealed:true` | The sealed append path was selected. |
| `local_fsync:true` | The local persistence barrier completed. |
| `applied_all_workers:true` | Every worker in this node applied the mutation. |
| `quorum_committed:true` | A voting majority durably stored/committed the Raft entry. |
| `failover_safe:true` | The checked failover property completed. |
| `durability_scope` | `memory`, `local`, or `quorum` for this response. |
| `synced`, `durable` | Legacy non-idempotent broker aliases for local fsync, not quorum proof. |

Clients requiring local durability supply a bounded `timeout_ms` and require
`ok`, `durability_scope:"local"`, `local_fsync`, and
`applied_all_workers`. HA failover-sensitive clients require `quorum` scope and
both quorum/failover fields as well. A deadline/transport failure can be
indeterminate and must be reconciled, not blindly retried.

## HA boundary

The HA layer contains leader election, replication, membership, optional mTLS,
and HTTP leader gating. A checksummed durable store recovers stable term/vote/
commit/applied state and log entries; append durability uses `fdatasync` and
invalid history fails closed. Election/append handling validates stale terms,
candidate recency, previous-log terms and per-peer vote deduplication.

Consequently:

- single-node `synced:true` is evidence of a completed local fsync;
- only the explicit checked quorum/failover receipt is failover evidence;
  local `durable:true` is not;
- correlated restart, power-loss, and durable-Raft safety remain release-blocking
  gaps until the targeted checker and full Jepsen campaign pass on the final
  implementation.

See [PRODUCTION_READINESS.md](PRODUCTION_READINESS.md) and
[REVENUE_READINESS_V1.md](REVENUE_READINESS_V1.md) for the current gate.

## Observability boundary

Prometheus metrics are exposed by the HTTP layer. HTTP counters and operation
counters use the broker's shared metrics mapping so a scrape represents all
workers in one process group. Persistence histograms require the same
cross-worker aggregation discipline; their exact current limitation is listed
in [OBSERVABILITY.md](OBSERVABILITY.md).

Metrics are operational signals, not an acknowledgement ledger. Correctness
tests enumerate durable responses and recovered records independently.

## Main source map

| Area | Primary files |
|---|---|
| Process lifecycle | `src/main.c`, `src/cluster.c` |
| HTTP and routing | `src/http_server.c`, `src/http_framing.c`, `src/router.c` |
| API handlers | `src/app_routes.c` |
| Broker/replay | `src/rf_broker.c` |
| Persistence | `src/persistence_zp.c`, `src/aof_batch.c` |
| HA | `src/ramforge_ha_*.c` |
| Shared state/metrics | `src/shared_storage.c`, `src/metrics_shared.c` |

## Deliberate non-goals for this sprint

- no custom TCP/binary client protocol;
- no Kubernetes operator;
- no claim of universal exactly-once external effects;
- no cloud, compliance, or security certification;
- no SLA invented from development measurements.
