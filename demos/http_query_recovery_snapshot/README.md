# HTTP QUERY Recovery Snapshot

This demo shows Ayder using RFC 10008 `QUERY` as an HTTP-native, read-only
recovery inspection surface over durable broker history.

It proves:

- `POST` writes payment recovery events.
- `QUERY` finds provider-ambiguous payments that are not safe to close.
- `QUERY` does not mutate high watermarks, committed offsets, message counts, or
  idempotency evidence.
- Ayder returns `Accept-Query`, `ETag`, and `Content-Location`.
- `If-None-Match` returns `304 Not Modified`.
- Equivalent query JSON with different object key order gets the same
  fingerprint.
- A bounded `from_offset..to_offset` query remains stable after new events are
  appended.
- `explain` reports a bounded partition scan with `broker_state_mutated=false`.

## Run

```bash
docker compose up -d --build
curl -fsS http://127.0.0.1:1109/health
./demos/http_query_recovery_snapshot/run_demo.sh
```

Artifacts are written under `artifacts/http_query_recovery_snapshot/`.
