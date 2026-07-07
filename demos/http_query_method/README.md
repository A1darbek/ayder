# HTTP QUERY Method Demo

Ayder is HTTP-native, so RFC 10008 gives it a clean protocol surface for
read-only broker queries with request bodies.

The point of this demo:

- `POST` still creates topics and writes events.
- `QUERY` reads broker data through a request body.
- The query is explicitly safe and idempotent at the HTTP method level.
- The response advertises `Accept-Query: application/json`.
- Operators avoid long or sensitive query details in the URI.

This is not a replacement for broker produce, consume, or commit. It is a better
HTTP shape for read-only query/reporting requests over Ayder's durable event
log.

## Run

From a fresh clone:

```bash
docker compose up -d --build
curl -fsS http://127.0.0.1:1109/health
./demos/http_query_method/run_demo.sh
```

## What It Shows

The demo writes three JSON events to an Ayder topic, then runs:

```bash
curl -X QUERY http://127.0.0.1:1109/broker/query \
  -H 'Authorization: Bearer dev' \
  -H 'Content-Type: application/json' \
  --data-binary @query.json
```

Expected result:

- HTTP `200`
- `Accept-Query: application/json`
- two matching rows
- one aggregate group
- an operator-readable receipt under `artifacts/http_query_method/`
