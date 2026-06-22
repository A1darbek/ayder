# Exactly-Once Integration Patterns (Reproducible)

This folder contains reproducible, bounded exactly-once integration patterns.

## Pattern 1: Postgres Transactional Consumer

Path:

- `tests/e2e/exactly_once/`

Bounded guarantee:

- exactly-once business effects for `Ayder -> consumer -> Postgres` under the harness fault model

Run:

```bash
DOCKER_BIN=docker.exe bash ./tests/e2e/exactly_once/run_campaign.sh
```

## Pattern 2: Webhook + Transactional Outbox

Path:

- `tests/e2e/patterns/webhook_outbox/`

Bounded guarantee:

- exactly-once business effects for `Ayder -> consumer -> outbox -> idempotent webhook sink`
- under the harness fault model (consumer/dispatcher crash, logical partition, broker SIGKILL, sink transient failures)

Run:

```bash
bash ./tests/e2e/patterns/webhook_outbox/run_campaign.sh
```

## Pattern 3: Idempotent REST Sink

Path:

- `tests/e2e/patterns/idempotent_rest_sink/`

Bounded guarantee:

- exactly-once sink effects for `Ayder -> consumer -> idempotent REST sink`
- under the harness fault model (consumer crash, logical partition, broker SIGKILL, sink transient failures)

Run:

```bash
bash ./tests/e2e/patterns/idempotent_rest_sink/run_campaign.sh
```

## Run All Three

```bash
bash ./tests/e2e/patterns/run_all_patterns.sh
```
