# Workload Notes

This harness measures durable produce/write acknowledgements on one node under small memory limits.

## Common Shape

- One topic/stream per service per run.
- One partition/subject.
- Fixed-size payloads, default `256` bytes.
- Configurable synchronous producers, default `CONCURRENCY=1`.
- One recorded latency per acknowledged write.
- Outputs are CSV plus JSON lines under `artifacts/<run-id>/`.

The default workload is intentionally conservative because forced fsync makes high message counts slow on laptops. Increase `MESSAGES` after a smoke run.

## Durability Settings

Ayder:

- Built from the repository `Dockerfile`.
- Started as `./ayder --workers 1 --aof always`.
- Produces use `POST /broker/topics/{topic}/produce?partition=0&durable=1&timeout_ms=5000&timing=1`.
- The client treats the operation as successful only when the JSON response has `ok: true` and `synced: true`.

Kafka:

- Single-node KRaft broker.
- Topic replication factor is `1`, as required for one node.
- Broker settings include `log.flush.interval.messages=1`, `log.flush.interval.ms=1`, and `log.flush.scheduler.interval.ms=1`.
- Client settings include `acks=all`, `linger.ms=0`, `batch.num.messages=1`, and one delivery acknowledgement per measured write.

NATS JetStream:

- JetStream enabled with file storage under `/data/jetstream`.
- The client creates a file-backed stream and awaits the publish ack for every message.

## Useful Runs

Smoke:

```bash
MESSAGES=100 CONCURRENCY=1 ./scripts/run.sh
```

Synchronous durability comparison:

```bash
MESSAGES=1000 CONCURRENCY=1 PAYLOAD_BYTES=256 ./scripts/run.sh
```

Small parallel pressure:

```bash
MESSAGES=5000 CONCURRENCY=4 PAYLOAD_BYTES=256 ./scripts/run.sh
```

Run one service while tuning:

```bash
SERVICES=ayder MESSAGES=1000 ./scripts/run.sh
```
