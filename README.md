# Ayder

**HTTP-native durable event log and message bus, written in C.**

Ayder provides durable produce, deterministic offset replay, consumer groups,
committed offsets, idempotency keys, and Raft-based high availability through a
plain HTTP API.

The repository contains two distinct layers:

1. **Ayder itself**: the broker, persistence, replay, commit, and HA code.
2. **Examples on top of Ayder's broker/replay path**: payment, webhook, and data
   pipeline applications that generate operator-facing recovery receipts.

The examples are not built-in payment or data products. They demonstrate how
application evidence can be derived from Ayder's durable event and replay
semantics.

## Quick Start

```bash
git clone https://github.com/A1darbek/ayder.git
cd ayder
docker compose up -d --build
curl -fsS http://127.0.0.1:1109/health
```

Build from source on Debian/Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y build-essential pkg-config \
  libuv1-dev libevent-dev libcurl4-openssl-dev libssl-dev zlib1g-dev liburing-dev

make clean && make
./ayder --port 1109
```

## HTTP Walkthrough

```bash
AUTH='Authorization: Bearer dev'

curl -X POST http://127.0.0.1:1109/broker/topics \
  -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"name":"events","partitions":1}'

curl -X POST \
  'http://127.0.0.1:1109/broker/topics/events/produce?partition=0&timeout_ms=5000&idempotency_key=event-001' \
  -H "$AUTH" --data-binary 'hello'

curl \
  'http://127.0.0.1:1109/broker/consume/events/my-group/0?offset=0&limit=10&encoding=b64' \
  -H "$AUTH"

curl -X POST http://127.0.0.1:1109/broker/commit \
  -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"topic":"events","group":"my-group","partition":0,"offset":0}'
```

A successful durable produce response includes evidence such as:

```json
{
  "ok": true,
  "offset": 0,
  "partition": 0,
  "batch_id": 1,
  "sealed": true,
  "durable": true,
  "synced": true
}
```

## Design-Review Examples

These examples run application workflows on top of Ayder's broker/replay path.
Each example starts its required database services, uses the real Ayder HTTP
API, and emits machine-readable and operator-readable receipts.

| Example | What it demonstrates | Run |
|---|---|---|
| [Payment ambiguity / PayPal verification](examples/payment-ambiguity-paypal/README.md) | Provider timeout is not failure; verify external commitment; DB wins over Redis | `./examples/payment-ambiguity-paypal/run_all.sh` |
| [Duplicate/out-of-order payment webhooks](examples/payment-webhook-ordering/README.md) | Terminal-state monotonicity, provider-event dedupe, one business effect | `./examples/payment-webhook-ordering/run_demo.sh` |
| [Databricks/Airflow/dbt pipeline recovery](examples/data-pipeline-recovery/README.md) | PASS/WARN/FAIL based on confirmed, missing, retried, critical, and audited data | `./examples/data-pipeline-recovery/run_all.sh` |

See [examples/README.md](examples/README.md) for the design-review sequence.

## Additional Recovery Demos

The `demos/` directory contains smaller scenario-specific receipts:

- provider timeout and financial indeterminism
- ledger clearance and bank reconciliation
- duplicate/out-of-order webhook suppression
- DLQ redrive safety
- data pipeline audit receipts

See [demos/README.md](demos/README.md).

## Correctness Evidence

### Broker-Level Jepsen Evidence

Published scoped claim:

- workload: `broker-log`
- fault modes: mixed, partition-only, kill-only
- verified matrix date: March 13, 2026
- result: 45/45 runs passed in the recorded `hn-ready` matrix

This is workload- and fault-model-specific evidence, not a universal proof for
every endpoint or configuration.

Run the published campaign:

```bash
bash ./tests/demo/ha_broker_jepsen_gold.sh
```

### End-to-End Exactly-Once Reference Pipeline

The reference `Ayder -> consumer -> Postgres` harness demonstrates exactly-once
business effects for its documented transactional consumer pattern:

```bash
bash ./tests/e2e/exactly_once/run_campaign.sh
```

It checks:

- no missing expected effects
- no duplicate business effects
- monotonic consumer offset state
- replay safety across injected crashes, partitions, and broker restarts

This is a reference-pipeline claim. Arbitrary external side effects still
require application idempotency and transactional discipline.

See [EVIDENCE.md](EVIDENCE.md) and
[tests/e2e/exactly_once/README.md](tests/e2e/exactly_once/README.md).

## Repository Map

```text
src/                 Ayder C source
deps/                vendored dependencies
examples/            design-review applications on Ayder
demos/               additional recovery-receipt scenarios
tests/e2e/           integration and exactly-once harnesses
tests/jepsen/        Jepsen workload and campaign tooling
tests/smoke/         HTTP API smoke coverage
scripts/             operations, benchmark, and partner helpers
cluster/             local HA cluster configuration
```

For partner evaluation, start with [DESIGN_PARTNERS.md](DESIGN_PARTNERS.md).

## Scope And Non-Goals

- Ayder is not Kafka protocol compatible.
- Ayder is not a SQL database.
- Exactly-once business effects are not automatic across arbitrary systems.
- Application examples demonstrate patterns; they are not production payment,
  banking, or data-platform implementations.

## License

MIT
