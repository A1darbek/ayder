# Ayder

**A durability-first event log that survives real failures.**

Most systems optimize for throughput.

Ayder is built for something else:
**correctness under crashes, partitions, and disk failures.**

- Verified with Jepsen: **45/45 mixed-fault tests passed**
- SIGKILL → restart → data still correct
- Single binary, HTTP-native, Raft-backed durability

👉 Try the live crash demo: https://ayder.xyz/invite

---

## What this solves

In many real systems, failure handling is unclear:

- what happens if the process crashes mid-write?
- is acknowledged data actually durable?
- how is recovery verified?

In edge / IIoT environments, these are not edge cases — they are normal conditions.

Ayder is designed to make these guarantees explicit and testable.

---

## What Ayder is (simple)

Ayder is a log/message system where:

- you send events via HTTP  
- data is written to disk before acknowledgment  
- if the process crashes, data is recovered via replay  
- in HA mode, data is replicated using Raft  

The goal:
**predictable behavior under failure, not just performance under load.**

---

## Verified correctness under real failures (Jepsen)

Public correctness claim:

- strictly linearizable under mixed faults  
- **45/45 tests passed (latest full matrix)**

Test conditions include:

- process crashes (SIGKILL)  
- network partitions  
- mixed fault scenarios  
- disk recovery validation  

This focuses on **behavior under failure**, not just benchmarks.

Full results and artifacts:
- tests/jepsen/results/gold_20260313T103615Z
- tests/jepsen/artifacts/gold_<run_id>.tar.gz

---

## See it live

**Crash demo (1 min):**  
https://www.youtube.com/watch?v=c-n0X5t-A9Y

**Live durability sandbox:**  
https://ayder.xyz/invite

Run:

1. produce events  
2. SIGKILL the process  
3. restart  
4. verify offsets and data consistency  

Each visitor gets an isolated container with persistent `/data`.

---

## Why not existing systems?

- Kafka: strong durability, but operationally heavy  
- Redis Streams: simple, but different durability/consensus model  
- MQTT pipelines: lightweight, but often rely on best-effort buffering  

Ayder explores a different point:

**lightweight + explicit durability under failure**

---

## Performance snapshot

Measured on HA (3-node, sync-majority):

- ~49k msg/s (wrk2 rate-limited)  
- p99 latency: ~3.46 ms  
- p99.999 handler: ~1.2 ms  

Recovery observation:

- follower SIGKILL → catch-up ~40–50s (≈8M offsets)

---

## Quick start

```bash
git clone https://github.com/A1darbek/ayder.git
cd ayder
docker compose up -d --build

Produce / consume:

# create topic
curl -X POST localhost:1109/broker/topics \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"name":"events","partitions":1}'

# produce
curl -X POST 'localhost:1109/broker/topics/events/produce?partition=0' \
  -H 'Authorization: Bearer dev' \
  -d 'hello world'

# consume
curl 'localhost:1109/broker/consume/events/mygroup/0?offset=0&limit=10&encoding=b64' \
  -H 'Authorization: Bearer dev'

Current scope

Ayder is focused on:

durable append-only log
consumer groups + offsets
crash recovery correctness
Raft-based HA

Not goals (yet):

Kafka protocol compatibility
full database semantics
exactly-once without client discipline
Looking for real-world feedback

I’m currently trying to understand where this matters most.

If you're working on edge / IIoT / distributed systems:

have you seen data loss after crashes?
how do you recover today?

Would value your perspective.

Author

Aidarbek Romanuly
GitHub: https://github.com/A1darbek

Email: aidarbekromanuly@gmail.com

License

MIT
