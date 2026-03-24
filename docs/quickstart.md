# Quickstart

## Single Node With Docker

```bash
docker compose up -d --build
curl -fsS http://127.0.0.1:1109/health
curl -fsS http://127.0.0.1:1109/ready
```

Expected responses:

- `/health` -> `{"ok":1}`
- `/ready` -> `{"ready":true}` once persistence is ready for writes

## Single Node From Source

Install build deps on Debian or Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y build-essential pkg-config \
  libuv1-dev libevent-dev libcurl4-openssl-dev libssl-dev zlib1g-dev liburing-dev
```

Build and run:

```bash
make clean && make
RF_BEARER_TOKENS='dev@55555555555555:11111111111111111:111111111111111111111' \
./ayder --port 1109
```

## 60-Second Broker Walkthrough

Set a base URL and auth header:

```bash
BASE=http://127.0.0.1:1109
AUTH='Authorization: Bearer dev'
```

Create a topic:

```bash
curl -fsS -X POST "$BASE/broker/topics" \
  -H "$AUTH" \
  -H 'Content-Type: application/json' \
  -d '{"name":"events","partitions":1}'
```

Produce one message:

```bash
curl -fsS -X POST "$BASE/broker/topics/events/produce?partition=0&timeout_ms=5000" \
  -H "$AUTH" \
  -H 'Content-Type: text/plain' \
  --data-binary 'hello world'
```

Consume from offset `0`:

```bash
curl -fsS "$BASE/broker/consume/events/mygroup/0?offset=0&limit=10&encoding=b64" \
  -H "$AUTH"
```

Commit offset `0`:

```bash
curl -fsS -X POST "$BASE/broker/commit" \
  -H "$AUTH" \
  -H 'Content-Type: application/json' \
  -d '{"topic":"events","group":"mygroup","partition":0,"offset":0}'
```

## Local HA Cluster

The repo includes a 5-node Docker Compose file:

```bash
docker compose -f docker-compose.ha5.yml up -d --build
```

HTTP ports:

- `node1` -> `7001`
- `node2` -> `8001`
- `node3` -> `9001`
- `node4` -> `10001`
- `node5` -> `11001`

Quick checks:

```bash
curl -fsS http://127.0.0.1:7001/health
curl -fsS http://127.0.0.1:7001/ready
curl -fsS -H 'Authorization: Bearer dev' http://127.0.0.1:7001/metrics_ha
```

Recommended durability settings for serious HA testing:

- `RF_HA_SYNC_MODE=1`
- `RF_HA_WRITE_CONCERN=majority` or an equivalent majority integer

## Jepsen Reproduction

Strict claim command from the root `README.md`:

```bash
sudo -v
sudo --preserve-env=STRICT_CLAIM,START_CLUSTER,TOKEN,AYDER_JEPSEN_WORKLOAD,AYDER_JEPSEN_PROFILE,AYDER_JEPSEN_MODES,AYDER_JEPSEN_DURATIONS,AYDER_JEPSEN_RUNS_PER_CELL,AYDER_JEPSEN_NEMESIS_STARTUP_SEC,AYDER_JEPSEN_MIXED_NEMESIS_STARTUP_SEC,AYDER_JEPSEN_PRE_READY_TIMEOUT_SEC \
env STRICT_CLAIM=1 START_CLUSTER=1 TOKEN=dev \
AYDER_JEPSEN_WORKLOAD=broker-log \
AYDER_JEPSEN_PROFILE=hn-ready \
AYDER_JEPSEN_MODES="mixed partition-only kill-only" \
AYDER_JEPSEN_DURATIONS="120 300 600" \
AYDER_JEPSEN_RUNS_PER_CELL=5 \
AYDER_JEPSEN_NEMESIS_STARTUP_SEC=10 \
AYDER_JEPSEN_MIXED_NEMESIS_STARTUP_SEC=15 \
AYDER_JEPSEN_PRE_READY_TIMEOUT_SEC=180 \
bash ./tests/demo/ha_broker_jepsen_gold.sh
```

Use that result as evidence only for the tested workload and configuration.
