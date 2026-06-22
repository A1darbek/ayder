# Pattern: Idempotent REST Sink

Bounded guarantee:

- exactly-once sink effects for `Ayder -> consumer -> idempotent REST sink`
- under the harness fault model (consumer crash injection, logical partitions, broker SIGKILL, sink transient failures)

Run:

```bash
bash ./tests/e2e/patterns/idempotent_rest_sink/run_campaign.sh
```

Artifacts:

- `artifacts/e2e_patterns/idempotent_rest_<timestamp>/summary.json`
- `artifacts/e2e_patterns/idempotent_rest_<timestamp>/invariants.json`
- `artifacts/e2e_patterns/idempotent_rest_<timestamp>/ayder.log`
