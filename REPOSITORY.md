# Repository Guide

## Core

- `src/`: Ayder broker, HTTP API, persistence, replay, offsets, and HA
- `deps/`: vendored dependencies
- `cluster/`: local HA cluster configuration
- `scripts/`: operational and benchmark helpers

## Public Examples

- `examples/`: polished design-review applications on Ayder
- `demos/`: additional focused recovery receipts

## Evidence

- `tests/e2e/exactly_once/`: reference exactly-once business-effect pipeline
- `tests/e2e/partial_batch_receipt/`: effect-level partial batch receipt
- `tests/jepsen/`: broker correctness workloads and campaign tooling
- `tests/smoke/`: API route smoke checks

## Generated Files

Runtime artifacts, local binaries, AOF files, test outputs, IDE state, and GTM
workspaces are excluded through `.gitignore`.

The public repository should contain source, runnable examples, harnesses, and
small documentation entrypoints. Generated evidence bundles should be attached
to releases or published intentionally, not committed as routine workspace
output.
