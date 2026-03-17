# Ayder

**HTTP-native durable event log and message bus, written in C.**

Ayder is designed to be simple to run and serious about correctness: one binary, HTTP API, Raft-backed durability, and straightforward operational behavior under failure.

You can start with `curl` on a single node and scale to multi-node deployments without switching protocols or client stacks.

## Real Jepsen Result (Strict Claim)

Public correctness claim:
- strictly linearizable under mixed faults: 45/45 pass in the latest full `hn-ready` matrix

Verified run (UTC date):
- 2026-03-13
- matrix dir: `tests/jepsen/results/gold_20260313T103615Z`
- matrix summary: `tests/jepsen/results/gold_20260313T103615Z/cells.csv`
- result: all 9 cells `exit_code=0` (45/45 total runs passed)

Strict run command (mixed first):

```bash
sudo -v
sudo --preserve-env=STRICT_CLAIM,START_CLUSTER,TOKEN,AYDER_JEPSEN_WORKLOAD,AYDER_JEPSEN_PROFILE,AYDER_JEPSEN_MODES,AYDER_JEPSEN_DURATIONS,AYDER_JEPSEN_RUNS_PER_CELL,AYDER_JEPSEN_NEMESIS_STARTUP_SEC,AYDER_JEPSEN_MIXED_NEMESIS_STARTUP_SEC,AYDER_JEPSEN_PRE_READY_TIMEOUT_SEC \
env STRICT_CLAIM=1 START_CLUSTER=1 TOKEN=dev \
AYDER_JEPSEN_WORKLOAD=broker-log \
AYDER_JEPSEN_PROFILE=hn-ready \
AYDER_JEPSEN_MODES="mixed partition-only kill-only" \
AYDER_JEPSEN_DURATIONS="120 300 600" \
AYDER_JEPSEN_RUNS_PER_CELL=5 \
AYDER_JEPSEN_NEMESIS_STARTUP_SEC=10 \
AYDER_JEPSEN_MIXED_NEMESIS_STARTUP_SEC=15 \
AYDER_JEPSEN_PRE_READY_TIMEOUT_SEC=180 \
bash ./tests/demo/ha_broker_jepsen_gold.sh
```

Quick validation:

```bash
RESULT=tests/jepsen/results/gold_YYYYMMDDTHHMMSSZ
cat "$RESULT/cells.csv"
awk -F, 'NR>1 && $4!=0 {bad=1} END {exit bad}' "$RESULT/cells.csv" && echo "ALL CELLS PASS"
```

Proof bundle format:
- `tests/jepsen/artifacts/gold_<run_id>.tar.gz`
- `tests/jepsen/artifacts/gold_<run_id>.tar.gz.sha256`

## See It Live

**1-minute demo: SIGKILL -> restart -> data still there**  
https://www.youtube.com/watch?v=c-n0X5t-A9Y

**Live durability sandbox: SIGKILL -> restart -> committed offsets still correct**  
Per-visitor container + persisted `/data` volume: https://ayder.xyz/invite  
On the sandbox page, click **Run double proof** to produce events, SIGKILL, restart, and get a JSON proof that includes:
- last committed offset before kill
- first consumed offset after restart
- commit persistence across restart

## Why This Project Exists

Most teams want three things at once:
- operational simplicity
- durable replicated writes
- predictable behavior during node failures

Ayder is built for that intersection: Kafka-style durability goals with a much lighter operational footprint.

## Quick Start

### Docker Compose

```bash
git clone https://github.com/A1darbek/ayder.git
cd ayder
docker compose up -d --build
```

### Build From Source

```bash
# Debian/Ubuntu
sudo apt-get update
sudo apt-get install -y build-essential pkg-config \
  libuv1-dev libevent-dev libcurl4-openssl-dev libssl-dev zlib1g-dev liburing-dev

make clean && make
./ayder --port 1109
```

## 60-Second API Walkthrough

```bash
# 1) Create topic
curl -X POST localhost:1109/broker/topics \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"name":"events","partitions":1}'

# 2) Produce
curl -X POST 'localhost:1109/broker/topics/events/produce?partition=0' \
  -H 'Authorization: Bearer dev' \
  -d 'hello world'

# 3) Consume
curl 'localhost:1109/broker/consume/events/mygroup/0?offset=0&limit=10&encoding=b64' \
  -H 'Authorization: Bearer dev'

# 4) Commit offset
curl -X POST 'localhost:1109/broker/commit/events/mygroup/0' \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"offset":0}'
```

## Why Ayder?

### Practical Positioning

- Kafka: powerful and battle-tested, but operationally heavy for small/medium teams.
- Redis Streams: very easy to start, but durability/consensus semantics differ from a replicated consensus log.
- Ayder: single-binary ergonomics with Raft-based replicated durability.

### At-a-Glance Comparison

| | Kafka | Redis Streams | Ayder |
|---|---|---|---|
| Protocol | Kafka binary protocol | RESP | HTTP |
| Operational model | JVM + cluster tuning | simple, topology-dependent | single binary + HA mode |
| Replication semantics | replicated log | topology-dependent | Raft consensus |
| First message time | typically higher setup | very fast | very fast |

## Performance Snapshot

Measured on real-network HA runs (3-node, sync-majority):
- wrk2 rate-limited throughput: 49,871 msg/s
- client p99 latency: 3.46 ms
- server p99.999 handler: 1.219 ms
- max-throughput wrk run: 93,807 msg/s

Recovery observation from documented runbooks:
- follower SIGKILL and catch-up to healthy cluster in about 40-50 seconds (around 8M offsets in tested setup)

## HA Model and Write Semantics

Ayder supports 3/5/7 node Raft clusters.

Write concern guidance:
- `RF_HA_WRITE_CONCERN=1`: leader-local ack, lowest latency, weakest durability
- `RF_HA_WRITE_CONCERN=majority`: recommended for strong durability
- `RF_HA_WRITE_CONCERN=all`: strongest ack rule, higher tail latency

Common strict path settings used in Jepsen campaigns:
- `RF_HA_SYNC_MODE=1`
- majority write concern for voter count
- mixed fault modes (`partition-only`, `kill-only`, `mixed`)

## Features

- Append-only broker topics with per-partition offsets
- Consumer groups with committed offsets
- Durable storage with crash recovery
- Raft HA replication with dynamic membership flows
- KV with CAS and TTL
- Built-in stream processing (filters, aggregates, joins)

## Project Documentation Map

Use these docs by task:
- `tests/README.md` - test docs entrypoint
- `tests/demo/README.md` - practical local runbooks and strict command flow
- `tests/jepsen/README.md` - Jepsen workload/checker internals and artifacts
- `scripts/README.md` - chaos and benchmark helper scripts
- `cluster/README.md` - local cluster ports/layout/start flow

## Limits and Non-Goals (Current State)

- not Kafka protocol compatible
- not a SQL database
- exactly-once still requires client idempotency discipline

## Author

Built by Aydarbek Romanuly.

- GitHub: [@A1darbek](https://github.com/A1darbek)
- Email: aidarbekromanuly@gmail.com

## License

MIT