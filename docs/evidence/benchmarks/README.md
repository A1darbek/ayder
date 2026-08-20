# Revenue Readiness v1 benchmark evidence

The files in this directory are machine-readable summaries, not production
SLAs. Every WSL2 result is labeled development/regression evidence.

- [`rrv1-pre-fix-wsl2-128b.json`](rrv1-pre-fix-wsl2-128b.json) is the original
  performance baseline. It predates sealed-watermark and complete-write fixes
  and is not publishable durability evidence.
- [`rrv1-post-fix-wsl2.json`](rrv1-post-fix-wsl2.json) is the condensed
  pre-visibility-fix summary retained for historical comparison only; its own
  status field marks it superseded.
- [`rrv1-post-visibility-commitfix-wsl2.json`](rrv1-post-visibility-commitfix-wsl2.json)
  is the strict fixed-code matrix. Every counted success required explicit local
  fsync and all-worker application and explicitly denied quorum/failover safety.
- [`rrv1-wrk-durable-c1-ca9e210.json`](rrv1-wrk-durable-c1-ca9e210.json)
  is a standard-wrk, one-connection closed-loop check from an exact clean source
  archive. Its 5.522 ms median p99 is low-concurrency latency evidence only and
  is not comparable to the fixed-rate, coordinated-omission-corrected matrix.

The raw per-trial output, environment capture, system samples, and perf data are
under `bench/results/`. The measured working tree was dirty, so the summaries
record the baseline Git SHA, tracked patch hash, untracked manifest hash, server
binary hash, and wrk2 hash. They must not be presented as an immutable release
benchmark.

Invalid setup attempts were removed from the result set and moved to the
recoverable local quarantine `/tmp/ayder-rrv1-invalid-results-20260815/`.
