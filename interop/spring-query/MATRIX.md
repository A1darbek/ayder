# QUERY Interop Matrix

All receipts use:

```json
{
  "schema_version": "0.1",
  "target": "ayder-direct",
  "generated_at": "2026-07-16T12:00:00Z",
  "implementation_commit": "COMMIT_SHA",
  "rows": [],
  "summary": {
    "pass": 0,
    "fail": 0,
    "unverified": 0
  }
}
```

Allowed row statuses:

- `PASS`
- `FAIL`
- `NOT_SUPPORTED`
- `NOT_APPLICABLE`
- `UNVERIFIED`
- `OBSERVED`

## Core Method Handling

| Row ID | Question | Expected |
| --- | --- | --- |
| `core.native_query` | Does native `QUERY` with JSON body reach Ayder? | `200` |
| `core.accept_query_header` | Does Ayder advertise accepted query media types? | `Accept-Query: application/json` |
| `core.unsupported_content_type` | Is unsupported request content rejected? | `415` |
| `core.missing_content_type` | What happens without `Content-Type`? | Record actual behavior |
| `core.query_body_reaches_handler` | Did the JSON body reach the query handler? | Two matching rows and a fingerprint |
| `core.post_compatibility_fallback` | Does legacy `POST /broker/query` still work? | Record separately |
| `cors.allow_methods_mentions_query` | Does the response CORS method list mention `QUERY`? | `PASS` if present |
| `ayder.rate_limit_headers_sane` | Did the enabled limiter emit sane `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset` values? | Positive limit/reset and `0 <= remaining <= limit` |

## Representation Identity

| Row ID | Question | Expected |
| --- | --- | --- |
| `representation.first_bounded_query` | First bounded `0..3` query | `200`, two matching rows |
| `representation.repeat_identical_query` | Repeat identical body | Same fingerprint and `ETag` |
| `representation.equivalent_json` | Reordered object keys and whitespace | Same Ayder fingerprint |
| `representation.revalidation` | Does exact `If-None-Match` produce `304` while the selected representation has not changed? | `304` |
| `representation.content_location` | Is a retrievable authenticated result resource advertised? | `NOT_SUPPORTED` until that resource exists |
| `representation.expanded_query` | Expanded range `0..5` | Different fingerprint/result |

## Safety Invariants

| Row ID | Question | Expected |
| --- | --- | --- |
| `safety.broker_state` | Did `QUERY` mutate high watermark, message count, or idempotency records? | No |
| `safety.committed_offset` | Did `QUERY` advance committed offset? | No |
| `safety.bounded_after_append` | Does bounded `0..3` stay stable after appends? | Same result |
| `safety.expanded_after_append` | Does expanded `0..5` see appended rows? | Yes |

## Envoy Boundary

| Row ID | Question | Expected |
| --- | --- | --- |
| `transport.direct_envoy_equivalence` | Do direct and Envoy produce the same fingerprint and `ETag`? | `PASS` |

The Envoy receipt also reuses every core, representation, and safety row above.

## Cache Boundary

| Row ID | Question | Expected |
| --- | --- | --- |
| `cache.forwarded_not_cache_proof` | Did we avoid over-claiming cache compatibility? | `UNVERIFIED` until real cache evidence exists |
| `cache.collision_test` | Do body A, body B, body A avoid response collision? | Correct results observed; cache proof remains `UNVERIFIED` without hit/miss evidence |

Required future cache proof:

```text
QUERY body A -> result A
QUERY body B -> result B
QUERY body A -> result A
```

Failure condition:

```text
Body B receives cached result A.
```

To upgrade the cache rows from `UNVERIFIED`, capture:

- origin request count
- cache hit/miss evidence
- cache key or derived identity
- two different request bodies on the same URI
- proof that there is no cross-body response collision
