# Ayder vs Kafka vs NATS JetStream

Single-node, resource-constrained durability benchmark for founder, design-partner,
and investor conversations.

This is not a replacement for Jepsen. It answers a narrower question:

> When the systems are forced onto a tiny machine and each write waits for the
> durable acknowledgement path, how much latency and throughput do we see?

## Systems

- Ayder: HTTP produce endpoint, single worker, `--aof always`, 512 MB default limit
- Kafka: single-node KRaft, `acks=all`, one partition, server flush every message,
  1 GB default limit
- NATS JetStream: file-backed stream, synchronous publish acknowledgements,
  512 MB default limit

## Requirements

- Docker with Compose v2
- Enough local disk for container images and benchmark artifacts

In WSL, Docker Desktop must have integration enabled for the distro running this
repository.

## Quick Run

```bash
cd benchmarks/ayder-vs-kafka-nats
./run.sh
```

For the JSONL/combined CSV runner with configurable concurrency:

```bash
MESSAGES=1000 CONCURRENCY=4 ./scripts/run.sh
```

Run one target:

```bash
TARGET=ayder MESSAGES=2000 PAYLOAD_BYTES=128 ./run.sh
TARGET=kafka MESSAGES=2000 PAYLOAD_BYTES=128 ./run.sh
TARGET=nats MESSAGES=2000 PAYLOAD_BYTES=128 ./run.sh
```

Use a tighter Ayder/NATS limit or a larger Kafka limit:

```bash
AYDER_MEM_LIMIT=512m NATS_MEM_LIMIT=512m KAFKA_MEM_LIMIT=1g ./run.sh
```

## Artifacts

The runner writes to `artifacts/<run-id>/`:

- `summary.csv`: one row per target
- `<target>-summary.json`: detailed summary
- `<target>-latencies.csv`: per-message latency samples in microseconds
- `results.jsonl` and `samples.csv` when using `scripts/run.sh`

The CSV summary is intended to be pasted directly into a memo or investor update.
Each runner generates a fresh Ayder bearer token for the process environment and
does not print it or write it to benchmark artifacts.

## Important Reading

Single-node durability benchmarks are useful, but easy to overclaim.

- Kafka is intentionally configured for the expensive path: server-side flush
  every message plus synchronous `acks=all` sends.
- JetStream uses file storage and waits for publish acknowledgements.
- Ayder accepts a sample only when the response explicitly proves local fsync
  and all-worker apply while explicitly denying quorum/failover safety.
- Replication, failover, client batching, compaction, and real production
  workloads need separate evidence. The bounded Jepsen smoke runs are not an HA
  safety proof; this benchmark covers only small-machine local-fsync ergonomics.

## Output Fields

- `ok`: number of successful acknowledged writes
- `errors`: number of failed writes
- `throughput_msg_s`: successful writes divided by wall-clock benchmark time
- `p50_us`, `p95_us`, `p99_us`: client-observed synchronous write latency
- `max_us`: slowest successful write
