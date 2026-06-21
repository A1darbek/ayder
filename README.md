# Ayder

A small, durable event log for integration paths where a duplicate, retry, or replay can corrupt business state.

Ayder is for payment and webhook workflows, transactional outbox and saga recovery, and other correctness-sensitive paths where the hard question after an incident is not "is the system up?" but "what actually happened to my data?" It runs as a single binary, speaks plain HTTP, and curl is the client. No JVM, no ZooKeeper, no thick client libraries.

It passes a mixed-fault Jepsen suite (45/45) and ships with a reproducible end-to-end recovery harness that drives Ayder through a consumer into Postgres with zero duplicate and zero missing effects under crash, partition, and replay faults. Evidence and artifact paths are below.

---

## The problem

Most event systems are tuned for throughput on the happy path. They tell you about lag, retries, and backpressure, but they do not tell you whether your business state is correct after a crash or failover.

So when something breaks, you reconstruct the truth by hand. Did we lose data? Was a charge processed twice? Which offsets can we trust? Did the downstream database diverge? Is recovery complete, or only partial? Teams routinely lose days to those questions after a single incident.

This hurts most in:

- payment and billing flows, where a duplicate effect is a real charge
- webhook ingestion with retries and out-of-order delivery
- transactional outbox and saga workflows
- multi-service integration pipelines
- any path where processing something twice has consequences you cannot undo

Ayder is built so the event path itself preserves enough evidence to answer those questions, instead of leaving you to piece it together from scattered logs.

---

## What it does

Ayder is a durability-first event log. Writes hit disk before they are acknowledged, and recovery happens by replaying that disk state, so a hard kill does not lose committed data or offsets. Replication is Raft-based for HA, and the entire interface is HTTP, so any client that can send a request can produce, consume, and commit.

Current capabilities:

- durable append-only log with sealed batch writes
- idempotent produce, so duplicate writes are deduplicated
- consumer groups with committed offsets
- Raft-based HA replication
- linearizable consume in HA mode
- crash recovery by disk replay

---

## Evidence

Here is what Ayder can demonstrate, with artifacts you can inspect yourself.

### Jepsen, 45/45 mixed-fault

Ayder passes 45 of 45 mixed-fault Jepsen tests, covering SIGKILL crashes, network partitions, node restarts, mixed-failure scenarios, and disk-replay validation.

Artifacts:
- `tests/jepsen/results/gold_20260313T103615Z`
- `tests/jepsen/artifacts/gold_20260313T103615Z.tar.gz`

### End-to-end recovery harness

A reproducible harness runs the full path, Ayder to a consumer to Postgres, with transactional dedupe, idempotent effects, monotonic offset progression, and replay-safe recovery. It is stress-tested under consumer crashes, network partitions, broker SIGKILL, retries, and replays.

Latest heavy run (`TOTAL_EVENTS=500`):
- duplicate effects: 0
- missing effects: 0
- monotonic violations: 0

Artifact:
- `artifacts/e2e_exactly_once/e2e_eos_20260507T164110Z`

This is scoped on purpose. Ayder does not claim global exactly-once across arbitrary systems, because that is not a real thing you can buy off the shelf. Exactly-once still needs disciplined handling on the consumer side. What Ayder gives you is a recovery discipline you can reproduce and verify, so the correctness is demonstrable rather than assumed.

---

## Quick start

curl is the client.

```bash
git clone https://github.com/A1darbek/ayder.git
cd ayder
docker compose up -d --build
```

Create a topic:

```bash
curl -X POST localhost:1109/broker/topics \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"name":"events","partitions":1}'
```

Produce:

```bash
curl -X POST 'localhost:1109/broker/topics/events/produce?partition=0' \
  -H 'Authorization: Bearer dev' \
  -d 'hello world'
```

Consume:

```bash
curl 'localhost:1109/broker/consume/events/mygroup/0?offset=0&limit=10&encoding=b64' \
  -H 'Authorization: Bearer dev'
```

---

## What Ayder is not

- not a Kafka clone or a drop-in Kafka replacement
- not a stream processing framework
- not a database
- not "magic exactly-once" for arbitrary systems

It is deliberately small. The point is a correctness-sensitive integration path you can run and reason about without operating a large broker stack.

---

## Looking for design partners

I am looking for a few teams to work with directly, the kind who have been burned by recovery ambiguity and want it gone.

That usually means you deal with:

- duplicate or out-of-order webhooks
- payment provider ambiguity (did the charge actually go through?)
- consumer offset recovery after a crash
- transactional outbox and saga recovery
- DLQ and redrive workflows
- replay-safe integration paths

If you have ever spent days after an incident reconstructing what was processed, committed, and deduplicated, that is exactly the pain I want to remove. Pick one integration path you are nervous about, and I will model its fault scenario in Ayder and show you the recovery evidence it produces. You keep your current stack.

Reach me at aidarbekromanuly@gmail.com.

---

## Author

**Aidarbek Romanuly**
GitHub: https://github.com/A1darbek
Email: aidarbekromanuly@gmail.com

## License

MIT
