# Architecture

## What Ayder Is

Ayder is a single binary with an HTTP API, append-only durable storage, and optional Raft-based HA replication.

Core subsystems visible in the codebase:

- HTTP server and routing
- broker log with per-partition offsets
- consumer offset storage
- KV store with CAS and TTL
- append-only sealed storage for durability
- Raft HA integration and linearizable leader gates
- Prometheus and HA metrics endpoints

## Request Flow

Write path, simplified:

1. Client sends an HTTP request.
2. Auth is checked unless the path is public.
3. On HA deployments, leader and quorum checks run for linearizable broker operations.
4. The write is appended to durable storage.
5. If HA is enabled, the write is replicated through Raft.
6. The response can optionally wait for local sync with `timeout_ms`.

Read path, simplified:

1. Client calls broker consume or KV get.
2. Broker consume in HA mode goes through a leader-only linearizable read barrier.
3. Data is returned with offsets and metadata.

## Storage Model

Broker:

- Topics contain partitions.
- Each partition is an append-only ordered log.
- Produced messages receive monotonically increasing offsets.
- Consumer groups store committed offsets separately.

KV:

- Keys are namespaced.
- `POST /kv/{namespace}/{key}` supports CAS and `ttl_ms`.
- `DELETE /kv/{namespace}/{key}` writes a tombstone instead of removing history in place.

Durability:

- Ayder uses append-only files under `/data` in the containerized path.
- `entrypoint.sh` links runtime files into `/data` so container restarts preserve state.
- `/ready` flips to `true` only after persistence is ready for writes.
- If persistence is not ready, write requests can return `503` with `sealed_only`.

## HA Model

Ayder supports Raft HA clusters. The project `README.md` documents common topologies as 3, 5, or 7 nodes.

Important behavior in HA mode:

- broker produce and broker consume use leader-only linearizable gates
- non-leaders can return `not_leader` with a `leader_url`
- lack of fresh quorum can reject linearizable operations
- write concern changes latency and durability tradeoffs

Practical write concern guidance from the current README:

- `RF_HA_WRITE_CONCERN=1`: lowest latency, weakest durability
- `RF_HA_WRITE_CONCERN=majority`: recommended default for strong durability
- `RF_HA_WRITE_CONCERN=all`: strongest ack rule, higher tail latency

## Public Paths And Auth

These paths are public by default:

- `/health`
- `/ready`
- `/metrics`
- `/metrics_ha`

Most data-plane endpoints require `Authorization: Bearer <token>`.
Tokens come from `RF_BEARER_TOKENS`.

## What Jepsen Covers

The published Jepsen claim is narrow and useful:

- workload: `broker-log`
- fault modes: `partition-only`, `kill-only`, `mixed`
- profile: `hn-ready`
- latest published full matrix in `README.md`: 45/45 pass on `2026-03-13` UTC

It does not automatically prove:

- all endpoints
- all environment variable combinations
- all client retry behaviors
- exactly-once delivery semantics
