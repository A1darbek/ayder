# Pattern: Webhook + Transactional Outbox

Bounded guarantee:

- exactly-once business effects for `Ayder -> consumer -> transactional outbox -> idempotent webhook sink`
- under the harness fault model (consumer/dispatcher crash injection, logical partitions, broker SIGKILL, sink transient failures)

Run:

```bash
bash ./tests/e2e/patterns/webhook_outbox/run_campaign.sh
```

WSL + Docker Desktop friendly:

```bash
DOCKER_BIN=docker.exe bash ./tests/e2e/patterns/webhook_outbox/run_campaign.sh
```

Artifacts:

- `artifacts/e2e_patterns/webhook_outbox_<timestamp>/summary.json`
- `artifacts/e2e_patterns/webhook_outbox_<timestamp>/invariants.json`
- `artifacts/e2e_patterns/webhook_outbox_<timestamp>/ayder.log`
