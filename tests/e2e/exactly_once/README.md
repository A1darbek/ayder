# End-to-End Exactly-Once Reference Harness (Ayder -> Consumer -> Postgres)

This harness validates a scoped claim:

- exactly-once business effects in Postgres for this consumer pattern
- under retries, timeouts, process crashes, network partitions, and broker restarts

It is intentionally explicit about scope. It is not a blanket global guarantee for arbitrary downstream systems.

## What It Implements

- Producer writes events to Ayder broker with `idempotency_key=event_id`
- Consumer reads from Ayder, then runs one Postgres transaction that does:
  - dedupe insert (`processed_events.event_id` unique)
  - business effect apply (`account_balances`)
  - monotonic offset state upsert (`consumer_state`)
  - offset history append (`consumer_state_history`)
- Consumer commits Ayder offset after DB transaction (best-effort with retry)
- Fault injector can trigger:
  - consumer crash between consume/process/commit stages
  - network partition (client -> Ayder port via iptables)
  - Ayder SIGKILL (when harness starts Ayder)

## Invariants Checked

1. No duplicate business effects (`processed_events` unique event ids).
2. No missing business effects for produced events.
3. Monotonic offset progression (`new_offset >= prev_offset` in history).
4. Replay safety after crash (balances equal expected total delta).
5. Consumer state offset equals max processed offset and expected last offset.

## Prerequisites

- `docker` + `docker compose`
- `bash`, `curl`, `jq`, `base64`
- Ayder binary at `./ayder` (or set `AYDER_BIN=/path/to/ayder`)
- For partition injection: passwordless `sudo -n` for iptables commands

## Quick Run

```bash
# from repo root
bash ./tests/e2e/exactly_once/run_campaign.sh
```

Artifacts are written to:

- `artifacts/e2e_exactly_once/<run_id>/`

Key files:

- `producer.log`
- `consumer.log`
- `faults.log`
- `invariants.log`
- `summary.txt`

## Tunables

```bash
TOTAL_EVENTS=300 \
DUP_EVERY=8 \
START_AYDER=1 \
ENABLE_PARTITION=1 \
ENABLE_AYDER_KILL=1 \
FAULT_CRASH_AFTER_CONSUME_PCT=10 \
FAULT_CRASH_BEFORE_DB_COMMIT_PCT=10 \
FAULT_CRASH_AFTER_DB_COMMIT_PCT=10 \
bash ./tests/e2e/exactly_once/run_campaign.sh
```

If you already run Ayder externally:

```bash
START_AYDER=0 \
AYDER_BASE=http://127.0.0.1:1109 \
TOKEN=dev \
bash ./tests/e2e/exactly_once/run_campaign.sh
```

## Cleanup

```bash
bash ./tests/e2e/exactly_once/stop.sh
```
