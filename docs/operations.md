# Operations

## Start And Stop

Single node:

```bash
docker compose up -d --build
docker compose logs -f ayder
docker compose down
```

HA cluster:

```bash
docker compose -f docker-compose.ha5.yml up -d --build
docker compose -f docker-compose.ha5.yml ps
docker compose -f docker-compose.ha5.yml down
```

## Persistent Data

Container runs persist state in `/data`.

Current startup behavior from `entrypoint.sh`:

- `append.aof`
- `append.aof.sealed`
- `append.aof.sealed.lock`

Those files are linked into `/app` from `/data`.
If `RF_RESET_DATA=1` is set, startup clears them.
Use that only for intentional resets.

## Liveness, Readiness, And HA Checks

Basic checks:

```bash
curl -fsS http://127.0.0.1:1109/health
curl -fsS http://127.0.0.1:1109/ready
curl -fsS http://127.0.0.1:1109/metrics | head
```

HA checks:

```bash
AUTH='Authorization: Bearer dev'
for port in 7001 8001 9001 10001 11001; do
  echo "== :$port =="
  curl -fsS -H "$AUTH" "http://127.0.0.1:$port/health"
  curl -fsS -H "$AUTH" "http://127.0.0.1:$port/metrics_ha" | head
  echo
done
```

What to watch:

- `/health` only says the HTTP process is alive
- `/ready` says persistence is ready for writes
- HA clients should care about leader and fresh-quorum metrics, not just liveness

## Write Availability Failure Modes

### `sealed_only`

If the node is up but persistence is not ready, writes can return:

```json
{"ok":false,"error":"sealed_only","message":"Writes disabled until persistence is ready"}
```

Action:

- wait for `/ready` to become `true`
- check startup logs and storage path health

### `not_leader`

In HA mode, linearizable broker produce and consume are leader-only.
A follower can reply with `not_leader` and include a `leader_url`.

Action:

- retry against the reported leader
- if there is no stable leader, inspect `/metrics_ha` across nodes

## Compact And Observe

Trigger background compaction:

```bash
curl -fsS -X POST http://127.0.0.1:1109/admin/compact
```

Response:

```json
{"result":"compaction_started","async":true}
```

Prometheus scrape points:

- `/metrics`
- `/metrics_ha`

The code exports HTTP counters, active connections, AOF queue depth, fsync histograms, and HA metrics.

## Jepsen And Fault Testing

Local Jepsen prereq check:

```bash
./tests/jepsen/bin/check-prereqs.sh
```

No-sudo local toolchain bootstrap:

```bash
./tests/jepsen/bin/bootstrap-toolchain.sh
```

Recommended matrix run:

```bash
AYDER_JEPSEN_WORKLOAD=broker-log ./tests/jepsen/bin/campaign-matrix.sh
```

Strict public-claim wrapper:

```bash
bash ./tests/demo/ha_broker_jepsen_gold.sh
```

Evidence to keep:

- exact Ayder commit SHA
- command line used
- `cells.csv` or `summary.csv`
- raw result directories
- bundle hash from `tests/jepsen/artifacts/*.sha256`

## Honest Limits

- `45/45` in the published matrix is meaningful, but it is still bounded by the tested workload and fault model.
- `/health` is not a durability guarantee.
- exactly-once delivery is not automatic; clients still need idempotency discipline.
- changing HA env vars changes the system you are operating, so re-test the configuration you actually plan to run.
