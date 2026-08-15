# Ayder Documentation

Start here:

- [Quick Start](quickstart.md): a copy-paste HTTP walkthrough covering every
  Ayder endpoint family.
- [API Reference](api-reference.md): all registered HTTP routes, request
  shapes, query parameters, response fields, and operational notes.
- [Architecture](ARCHITECTURE.md): process, request, broker, persistence, HA,
  and observability boundaries.
- [Revenue Readiness v1](REVENUE_READINESS_V1.md): exact test/benchmark results,
  RED/YELLOW/GREEN gates, residual risks, and merge decision.
- [Buyer evidence pack](BUYER_EVIDENCE_PACK.md): direct technical evidence index.
- [Benchmarks](BENCHMARKS.md): fixed-rate methodology, client-side results,
  profile, crash scope, and reproducibility commands.
- [Threat model](THREAT_MODEL.md) and [security policy](../SECURITY.md).
- [Observability](OBSERVABILITY.md), [runbook](RUNBOOK.md), and
  [production](PRODUCTION_READINESS.md)/[enterprise](ENTERPRISE_READINESS.md)
  gap matrices.
- [Ayder Wire decision](AYDER_WIRE_DECISION.md): why profiling does not justify
  a custom binary protocol now.

For design partners, the important boundary is:

Ayder is the durable broker/replay/commit path. Application examples and demos
write business-effect audit evidence on top of that path, then generate recovery
receipts for operators.

For HN and technical reviewers, the useful verification path is:

```bash
START_SERVER=1 SERVER_BIN="$PWD/ayder" PORT=1109 tests/smoke/api_smoke.sh
```

That smoke test exercises every registered HTTP route and checks route coverage
against `src/app_routes.c`.
