# FAQ

## What does the Jepsen claim actually mean?

Current public claim from `README.md`:

- latest full `hn-ready` matrix passed `45/45`
- verified run date: `2026-03-13` UTC
- artifact directory: `tests/jepsen/results/gold_20260313T103615Z`

That claim is specifically about the Jepsen `broker-log` workload under the published strict run settings. It is strong evidence for that path. It is not a proof that every API and every deployment choice is linearizable.

## Is Ayder a Kafka replacement?

Not protocol-compatible.

Ayder targets a different operational shape:

- HTTP API instead of Kafka protocol
- single binary
- lighter local and small-cluster setup

If you need Kafka clients or Kafka ecosystem tooling, Ayder does not provide that.

## Does Ayder guarantee exactly-once delivery?

No.

Ayder supports idempotency keys on broker produce paths, but end-to-end exactly-once semantics still depend on client behavior, retries, and consumer design.

## Why do I get `401 invalid_token`?

Most data-plane endpoints require `Authorization: Bearer <token>`.
Tokens are loaded from `RF_BEARER_TOKENS`.

Public endpoints by default:

- `/health`
- `/ready`
- `/metrics`
- `/metrics_ha`

## Why do writes fail with `sealed_only`?

The process is alive, but persistence is not ready yet. Wait for:

```bash
curl -fsS http://127.0.0.1:1109/ready
```

When it returns `{"ready":true}`, writes should be allowed again unless another error exists.

## Why does HA consume or produce fail on a healthy follower?

In HA mode, broker consume and broker produce go through leader-only linearizable paths. A healthy follower is not enough. The request must hit the leader, and the cluster must have fresh quorum.

## Are `/health` and `/ready` the same thing?

No.

- `/health` means the HTTP service answered
- `/ready` means persistence is ready for writes

For HA operations, also inspect `/metrics_ha`.

## What are the practical limitations today?

- no Kafka protocol compatibility
- not a SQL database
- Jepsen evidence is workload-scoped, not universal
- exactly-once requires client discipline
- operational behavior depends on HA settings such as write concern and sync mode
