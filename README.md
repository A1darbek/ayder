# Ayder

## Recovery-first event infrastructure for systems where failures are normal.

Most event systems optimize for throughput on the happy path.

Ayder is built for something different:

- power loss
- process crashes
- unstable networks
- partial recovery
- replay ambiguity
- duplicated side effects
- operational uncertainty after failure

Ayder helps teams recover with confidence instead of spending weeks reconstructing what happened.

---

# The Problem

Modern event systems expose failures through:

- lag
- retries
- metrics
- logs
- backpressure

But they rarely guarantee that the resulting business state is actually correct after recovery.

In practice, teams often deal with:

- duplicate processing
- partial writes
- replay uncertainty
- offset drift
- silently inconsistent downstream state
- long debugging cycles after incidents

Infrastructure may look healthy while reality diverges underneath.

This becomes especially painful in:

- edge / IIoT systems
- unreliable networks
- constrained hardware
- multi-service event pipelines
- operational environments where crashes are expected

---

# What Ayder Does

Ayder is a durability-first event log focused on recovery correctness.

Core properties:

- disk-backed durability before acknowledgment
- crash recovery via replay
- Raft-based replication
- HTTP-native interface
- lightweight deployment model
- explicit recovery behavior under failure

The goal is not just storing events.

The goal is ensuring systems recover predictably after failures.

---

# Verified Failure Behavior

## Jepsen verified

Ayder passed:

- 45/45 mixed-fault Jepsen tests

Faults included:

- SIGKILL crashes
- network partitions
- node restarts
- mixed-failure scenarios
- disk replay validation

Artifacts:
- `tests/jepsen/results/gold_20260313T103615Z`
- `tests/jepsen/artifacts/gold_20260313T103615Z.tar.gz`

---

# Exactly-Once Reference Harness

Ayder includes a reproducible end-to-end recovery harness demonstrating:

Ayder → consumer → Postgres

with:

- transactional dedupe
- idempotent business effects
- monotonic offset progression
- replay-safe recovery semantics

Stress-tested under:

- consumer crashes
- network partitions
- broker SIGKILL faults
- retries
- replay scenarios

Latest heavy verified run:

- TOTAL_EVENTS=500
- duplicate effects: 0
- missing effects: 0
- monotonic violations: 0

This is intentionally scoped:

Ayder does not claim magical global exactly-once semantics across arbitrary systems.

Instead, Ayder demonstrates a reproducible recovery discipline that preserves business correctness under real failures.

Artifacts:
- `artifacts/e2e_exactly_once/e2e_eos_20260507T164110Z`

---

# Why This Matters

Most systems recover operationally.

Far fewer recover correctly.

In production, teams often spend days or weeks answering questions like:

- Did we lose data?
- Was something processed twice?
- Which offsets are trustworthy?
- Did downstream systems diverge?
- Can we replay safely?
- Is recovery complete or only partial?

Ayder is designed to reduce that uncertainty.

---

# Design Philosophy

Ayder prioritizes:

- recovery confidence over benchmark theater
- operational simplicity over infrastructure sprawl
- explicit guarantees over implicit assumptions
- predictable failure behavior over happy-path throughput

---

# Current Capabilities

- durable append-only event log
- consumer groups + committed offsets
- Raft-based HA replication
- replay-based recovery
- Jepsen-tested correctness
- exactly-once reference harness

---

# Current Non-Goals

Ayder is intentionally not trying to be:

- a Kafka clone
- a stream processing framework
- a database
- globally magical exactly-once infrastructure

Exactly-once semantics still require disciplined downstream handling.

Ayder focuses on making recovery behavior explicit, testable, and reproducible.

---

# Quick Start

```bash
git clone https://github.com/A1darbek/ayder.git
cd ayder
docker compose up -d --build
```

**Create topic:**
```bash
curl -X POST localhost:1109/broker/topics \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"name":"events","partitions":1}'
```

**Produce:**
```bash
curl -X POST 'localhost:1109/broker/topics/events/produce?partition=0' \
  -H 'Authorization: Bearer dev' \
  -d 'hello world'
```

**Consume:**
```bash
curl 'localhost:1109/broker/consume/events/mygroup/0?offset=0&limit=10&encoding=b64' \
  -H 'Authorization: Bearer dev'
```

---

# Looking For Design Partners

Currently exploring use cases in:

- edge / IIoT
- industrial systems
- fleet infrastructure
- unreliable network environments
- operationally sensitive event pipelines

Especially interested in teams dealing with:

- replay ambiguity
- crash recovery pain
- duplicate processing
- operational debugging after failures

If this sounds familiar, I'd value connecting.

---

# Author

**Aidarbek Romanuly**

- GitHub: [https://github.com/A1darbek](https://github.com/A1darbek)
- Email: [aidarbekromanuly@gmail.com](mailto:aidarbekromanuly@gmail.com)

---

# License

MIT
