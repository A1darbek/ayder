# Ayder Docs

Ayder is an HTTP-native durable event log and message bus written in C.

Use this docs set by task:

- `quickstart.md`: start a single node or a local HA cluster
- `api-reference.md`: broker, KV, health, and metrics endpoints
- `architecture.md`: storage, HA, and request flow
- `operations.md`: readiness, durability checks, Jepsen, and incident basics
- `faq.md`: claim scope, limits, and common questions

Public correctness claim, matching the project `README.md`:

- Ayder reports a real Jepsen result for the `broker-log` workload: 45/45 pass in the latest full `hn-ready` matrix.
- Verified run date: `2026-03-13` UTC.
- Result bundle path in this repo: `tests/jepsen/results/gold_20260313T103615Z`.

Important scope:

- This is a workload-specific claim, not a blanket proof for every endpoint or deployment shape.
- The public claim is about strict runs over broker APIs with published artifacts and fault injection.
- If you want the same confidence level, run the published Jepsen flow against your own build and config.

Fastest next step:

```bash
# single node
docker compose up -d --build
curl -fsS http://127.0.0.1:1109/health

# HA demo
docker compose -f docker-compose.ha5.yml up -d --build
curl -fsS -H 'Authorization: Bearer dev' http://127.0.0.1:7001/metrics_ha
```


Additional docs:

- comms/twitter_launch_kit.md: X/Twitter launch copy and schedule
- comms/social_post_bank.md: reusable short/long post and reply bank
- ../wiki/: GitHub Wiki-ready pages
