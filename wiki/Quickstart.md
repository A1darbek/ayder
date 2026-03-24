# Quickstart

Start here if you want a local run without reading the whole repo.

## Build

```bash
git clone https://github.com/A1darbek/ayder.git
cd ayder
make clean && make
```

On Debian/Ubuntu, install the usual build deps first:

```bash
sudo apt-get update
sudo apt-get install -y build-essential pkg-config \
  libuv1-dev libevent-dev libcurl4-openssl-dev libssl-dev zlib1g-dev liburing-dev
```

## Run

```bash
./ayder --port 1109
```

## Minimal API flow

```bash
# Create topic
curl -X POST localhost:1109/broker/topics \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  -d '{"name":"events","partitions":1}'

# Produce
curl -X POST 'localhost:1109/broker/topics/events/produce?partition=0' \
  -H 'Authorization: Bearer dev' \
  -d 'hello world'

# Consume
curl 'localhost:1109/broker/consume/events/mygroup/0?offset=0&limit=10&encoding=b64' \
  -H 'Authorization: Bearer dev'
```

## Docker path

```bash
docker compose up -d --build
```

## Next

- [[Architecture]] for the system shape and HA model
- [[Operations]] for ports, readiness, and recovery
- [[Jepsen-Evidence]] for claim wording and artifact locations