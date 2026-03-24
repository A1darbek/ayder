# FAQ

## Is Ayder Kafka-compatible?

No. The repo docs say it is not Kafka protocol compatible. It is an HTTP service with broker-style APIs.

## What is the main durability model?

Raft-backed replicated durability in HA mode. For stronger durability semantics, the docs point to majority write concern and sync mode.

## What does the 6-node demo use?

The demo preset uses nodes on `7001` through `12001`, with matching Raft ports on `7000` through `12000`.

## Where do I find proof artifacts?

- `tests/jepsen/results/` for raw runs
- `tests/jepsen/artifacts/` for bundles and hashes
- `artifacts/chaos_*` for chaos reports, metrics, and captures

## What should I say about the correctness claim?

Use the exact wording from [[Jepsen-Evidence]]. Keep the date, matrix path, and bundle path attached to the claim.

## What if a run differs from the published result?

Call that out directly. A different commit, different matrix, or different fault model is a different claim.

## Where should I start reading?

- [[Quickstart]] for the shortest path to a running node
- [[Architecture]] for the shape of the system
- [[Operations]] for the runbook
- [[Jepsen-Evidence]] for correctness claims