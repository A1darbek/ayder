# HTTP API Smoke Tests

`api_smoke.sh` exercises every registered HTTP route from a single-node local
Ayder process and verifies route coverage against `src/app_routes.c`.

It covers:

- liveness/readiness/metrics
- admin compaction and sealed status/gc/release routes
- HA membership routes, expecting non-HA errors on a normal single node
- broker topic, produce, batch produce, consume, commit, retention, delete-before
- broker query and join
- KV put, get, meta, CAS delete
- route coverage check (`covered N/N registered routes`)

Run against an already-running server:

```bash
BASE=http://127.0.0.1:1109 TOKEN=dev tests/smoke/api_smoke.sh
```

Start a temporary local server from the repo binary:

```bash
START_SERVER=1 SERVER_BIN=./ayder PORT=1109 tests/smoke/api_smoke.sh
```

When `START_SERVER=1`, the script starts Ayder in a temporary directory so it
does not mutate repository-local AOF files.

Disable route coverage check (not recommended):

```bash
ROUTE_COVERAGE=0 START_SERVER=1 SERVER_BIN=./ayder PORT=1109 tests/smoke/api_smoke.sh
```
