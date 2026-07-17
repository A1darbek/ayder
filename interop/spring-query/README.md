# RFC 10008 QUERY Spring Interop Package

This package gives Volodymyr a reproducible Ayder-side matrix for HTTP
`QUERY` request bodies, then defines the Spring-side receipt contract to match.

The goal is side-by-side evidence:

```text
Ayder direct
  -> Ayder through Envoy
  -> Spring implementation / Spring client
  -> comparable receipts
```

The proof answers four questions:

- Does native `QUERY` with a JSON body reach the application?
- Do transport, security, firewall, and CORS layers recognize `QUERY`?
- Are `ETag`, `Content-Location`, `Accept-Query`, and `304` preserved?
- Does `QUERY` remain read-only and avoid cache-identity collisions?

## One Command

From the repository root:

```bash
./interop/spring-query/scripts/run-all.sh
```

The script starts an isolated Docker Compose stack:

- Ayder origin: `http://127.0.0.1:1119`
- Envoy forwarding boundary: `http://127.0.0.1:18080`

It writes:

```text
interop/spring-query/receipts/ayder-direct/receipt.json
interop/spring-query/receipts/ayder-direct/receipt.txt
interop/spring-query/receipts/ayder-envoy/receipt.json
interop/spring-query/receipts/ayder-envoy/receipt.txt
```

The host port for direct Ayder is `1119` to avoid colliding with the normal
repository demo service on `1109`. Inside Docker, Ayder still listens on `1109`.

## Run Only One Target

Against an existing Ayder process:

```bash
BASE=http://127.0.0.1:1109 ./interop/spring-query/scripts/run-direct.sh
```

Against an existing Envoy forwarding boundary:

```bash
BASE=http://127.0.0.1:18080 ./interop/spring-query/scripts/run-envoy.sh
```

## Fixtures

The fixtures in `fixtures/` use Ayder's current `QUERY /broker/query` shape:

- `bounded.json`: sealed range `0..3`
- `bounded-equivalent.json`: same semantic query with reordered object keys
- `expanded.json`: sealed range `0..5`
- `unsupported.txt`: non-JSON body for the `415` row

The scripts patch only `source.topic` and `include.commit_status_for_group` at
runtime so each run can be isolated without changing the query semantics.

## Cache Boundary

This package does not claim body-aware cache compatibility merely because Envoy
forwards `QUERY`.

It separates:

- `QUERY` forwarding through Envoy
- `QUERY` conditional revalidation through Envoy
- true body-aware caching with hit/miss and cache-key evidence

Until a cache layer reports origin request counts, hit/miss evidence, and no
cross-body collision, the cache proof rows stay `UNVERIFIED`.

## Spring Handoff

Volodymyr should implement the Spring-side checks in `SPRING_TARGET.md` and emit
the same row IDs in the same receipt schema. Then the receipts can be compared
line by line.
