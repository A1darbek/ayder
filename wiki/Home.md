# Ayder Wiki

Ayder is an HTTP-native durable event log and message bus with Raft-backed durability.

Use these pages as the short path into the core docs:

- [[Quickstart]]
- [[Architecture]]
- [[Operations]]
- [[Jepsen-Evidence]]
- [[FAQ]]

## What to expect

- One binary, HTTP API, and Raft-based HA mode
- Local-first workflows with `curl`
- Evidence-backed claims only, with artifact paths listed on the relevant pages

## Where the evidence lives

- Jepsen results: `tests/jepsen/results/`
- Published bundles: `tests/jepsen/artifacts/`
- Chaos reports and captures: `artifacts/chaos_*`

## Caution

Treat durability and linearizability statements as exact-run claims, not general promises. If you need to cite a result, use the run date, matrix directory, and bundle hash from [[Jepsen-Evidence]].