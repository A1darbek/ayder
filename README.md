# Ayder

Ayder is a single-binary, HTTP-native durable event log and message broker
written in C. It is built for recovery-critical AI and API workflows where an
operator needs durable evidence before deciding whether to retry, replay, or
reconcile an uncertain external side effect.

Ayder does not claim universal exactly-once effects across arbitrary external
systems. A provider timeout can leave an outcome unknown; Ayder preserves and
queries the broker evidence, while the application still owns provider
reconciliation and idempotent effect handling.

## Five-minute quick start

Docker publishes the broker on loopback only and requires an explicit bearer
credential. Generate a local evaluation token; use a secret manager for any
shared environment.

```bash
git clone https://github.com/A1darbek/ayder.git
cd ayder
export AYDER_TOKEN="$(openssl rand -hex 24)"
export RF_BEARER_TOKENS="${AYDER_TOKEN}@5000:5000:1000000"
docker compose up -d --build

curl -fsS http://127.0.0.1:1109/health
curl -fsS http://127.0.0.1:1109/ready
```

Create a topic, produce one locally durable record, consume it, and commit the
processed offset:

```bash
BASE=http://127.0.0.1:1109
AUTH="Authorization: Bearer ${AYDER_TOKEN}"

curl -fsS -X POST "$BASE/broker/topics" \
  -H "$AUTH" -H 'Content-Type: application/json' \
  --data-binary '{"name":"events","partitions":1}'

curl -fsS -X POST \
  "$BASE/broker/topics/events/produce?partition=0&timeout_ms=5000&idempotency_key=event-001" \
  -H "$AUTH" --data-binary 'hello'

curl -fsS \
  "$BASE/broker/consume/events/my-group/0?offset=0&limit=10&encoding=b64" \
  -H "$AUTH"

curl -fsS -X POST "$BASE/broker/commit" \
  -H "$AUTH" -H 'Content-Type: application/json' \
  --data-binary '{"topic":"events","group":"my-group","partition":0,"offset":0}'
```

For a source build on Debian/Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y build-essential pkg-config libuv1-dev libevent-dev \
  libcurl4-openssl-dev libssl-dev zlib1g-dev liburing-dev
make clean && make -j"$(nproc)"
RF_BEARER_TOKENS="${AYDER_TOKEN}@5000:5000:1000000" \
  ./ayder --port 1109 --workers 0
```

See the [full quick start](docs/quickstart.md) and
[API reference](docs/api-reference.md).

## Architecture

HTTP/1 requests pass through bounded framing, bearer authentication, resource
controls, routing, broker state, and the sealed append path. A parent process
can fork `SO_REUSEPORT` workers that coordinate selected state and metrics in
shared memory. Optional experimental HA adds leader gating, replication, and
peer mTLS; the current implementation does not yet support a crash-safe Raft
claim.

The implementation map and trust boundaries are in
[Architecture](docs/ARCHITECTURE.md) and the
[Threat model](docs/THREAT_MODEL.md).

## Durability contract

A single-node write is locally fsync-confirmed and visible to every local worker
only when all of these are true:

```json
{
  "ok": true,
  "durability_scope": "local",
  "local_fsync": true,
  "quorum_committed": false,
  "failover_safe": false,
  "applied_all_workers": true
}
```

Successful writes on the sealed path complete the local persistence and
all-worker barriers even when `timeout_ms` is omitted; an explicit positive
value bounds coordination/replication waits rather than selecting an async
mode. HTTP 2xx or `ok:true` alone is still insufficient—check every receipt
field. A timeout or persistence error is an ambiguous outcome that must be
reconciled before retrying. `batch_id` and the sealed sync watermark are
worker-local, not cluster-global transaction identifiers.

In HA mode, local `durable:true` does not prove failover durability. Require
`durability_scope:"quorum"`, `quorum_committed:true`,
`failover_safe:true`, and the local/all-worker fields above. Durable Raft state,
protocol validation and atomic payload-bound idempotency are implemented with
focused regressions; the full post-change fault campaign is still a release
blocker. See
[Production readiness](docs/PRODUCTION_READINESS.md).

## Evidence

- The repository contains fixed-rate wrk2 tooling, machine-readable summaries,
  redacted environment capture, and crash/restart reconciliation under
  [`bench/`](bench/).
- The fixed ARM64 WSL2 matrix, requiring local fsync and all-worker application,
  achieved roughly 308–382 responses/s with 19–20 second p99 and timeouts.
  Every returned success carried a valid explicit local-only receipt. No 50k or
  production-performance claim is supported. See
  [Benchmarks](docs/BENCHMARKS.md).
- A separate standard-wrk closed-loop check at one connection had 5.522 ms
  median p99 at 347.16 responses/s across three clean trials. One trial reached
  15.755 ms p99, and higher concurrency produced queueing; this does not replace
  the fixed-rate result or change the RED performance gate.
- A historical scoped `broker-log` Jepsen matrix recorded 45/45 passes on March
  13, 2026. Persistence and acknowledgement behavior changed in this hardening
  branch, so that historical result is context—not final-branch validation.
  Current-tree short kill, partition and mixed runs pass with exact post-run
  convergence; the strengthened full campaign remains required before merge.
- HTTP framing has deterministic adversarial tests plus an ASan/UBSan mutation
  smoke harness. Exact commands and bounded claims are indexed in the
  [Buyer evidence pack](docs/BUYER_EVIDENCE_PACK.md).

## Recovery and QUERY

Ayder supports deterministic offset replay and the HTTP `QUERY` method for
read-only recovery inspection. The examples demonstrate how applications can
record an external-effect attempt, inspect durable evidence after an ambiguous
response, reconcile provider state, and then make a deliberate retry or manual
decision.

- [Payment ambiguity / provider verification](examples/payment-ambiguity-paypal/README.md)
- [Duplicate and out-of-order webhooks](examples/payment-webhook-ordering/README.md)
- [Data-pipeline recovery](examples/data-pipeline-recovery/README.md)
- [Additional recovery receipts](demos/README.md)

These are reference applications, not built-in payment, banking, or data
products.

## Observability and operations

Ayder exports Prometheus metrics, example alert rules, and a Grafana dashboard.
`/metrics` and `/metrics_ha` require bearer authentication by default; setting
`RF_METRICS_PUBLIC=1` is an explicit deployment choice. Liveness and readiness
remain unauthenticated, and `/ready` returns HTTP 503 when persistence is not
ready.

- [Observability](docs/OBSERVABILITY.md)
- [Operations runbook](docs/RUNBOOK.md)
- [Prometheus configuration and alerts](deploy/prometheus/)
- [Grafana dashboard](deploy/grafana/ayder-dashboard.json)

## Security

The hardening branch rejects ambiguous request framing (including TE+CL and
invalid duplicate Content-Length), bounds decoded chunks and request lifetime,
protects operational metrics by default, uses strict bearer syntax and bounded
constant-time token comparison, and validates configured HA peer identity.
These are internally tested controls, not an independent penetration test or
certification.

Read [SECURITY.md](SECURITY.md) before deployment. Report vulnerabilities using
its private disclosure process rather than a public issue.

## Current production limitations

This branch is prepared for technical evaluation, but it is not release-ready.
Material open items include:

- final-branch full Jepsen 45/45 and stronger acknowledged-durability workloads
  have not completed;
- durable Raft state/protocol checks and explicit quorum receipts have focused
  evidence, but the full post-change 45-run and power/storage-fault campaigns
  have not completed;
- cross-worker and atomic payload-bound idempotency defects have focused passing
  regressions, but longer HA/concurrency faults remain required;
- online sealed-log GC is disabled pending safe cross-process inode
  coordination;
- client TLS requires a trusted reverse proxy; authorization is bearer-wide
  rather than fine-grained; encryption at rest and audit logging are external
  deployment responsibilities;
- cold backup/restore, distinct-binary rollback and one narrow crash RPO/RTO
  drill pass, but online backup, rolling upgrade and target-scale DR remain
  unsupported; and
- final native target-storage ARM64/amd64 performance remains pending; hosted
  native ARM64/amd64 build, security, scanner and provenance evidence passes
  for the documented clean candidate.

The complete gate and gap matrix are in
[Revenue Readiness v1](docs/REVENUE_READINESS_V1.md) and
[Enterprise readiness](docs/ENTERPRISE_READINESS.md).

## Contributing

Start with [CONTRIBUTING.md](CONTRIBUTING.md). Security-sensitive changes must
include adversarial regression coverage and must not weaken acknowledgement or
persistence semantics to improve a benchmark.

## Repository map

```text
src/                 broker, HTTP, persistence, HA, and shared state
bench/               fixed-rate, endpoint, system, and crash/restart harnesses
tests/security/      parser and live adversarial HTTP regressions
tests/regression/    durability and HA contracts
tests/jepsen/        distributed fault workloads and campaign tooling
deploy/              Prometheus and Grafana examples
examples/, demos/    recovery-oriented reference workflows
cluster/             local HA configuration and certificate generator
```

Ayder is pre-revenue and has no paying or production customers. Design-partner
and maintainer evaluation is technical feedback, not customer adoption.

License: [MIT](LICENSE).
