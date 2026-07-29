# Spring-Side QUERY Target

Volodymyr should reproduce the same row IDs and receipt schema from Spring.

The Spring target should test both:

```text
Spring server receives QUERY
Spring client calls Ayder direct and through Envoy
```

## Profile A: Routing And Model Binding

Prove:

- native `QUERY` reaches the controller
- JSON body binds to the request DTO
- no method rewrite to `POST` is required
- equivalent JSON requests are accepted
- unsupported content type is rejected

Suggested receipt rows:

- `core.native_query`
- `core.json_body_binding`
- `core.unsupported_content_type`
- `semantic.equivalent_json`

## Profile B: Security Chain

This covers the failure mode where Spring Security or the HTTP firewall rejects
unknown methods before the controller sees the request.

Prove:

- Spring Security firewall does not return an accidental early `400`
- `QUERY` is explicitly allowed where required
- authentication executes
- authorization executes
- request reaches the intended controller
- disallowed requests fail intentionally

Receipt evidence:

```json
{
  "id": "security.firewall",
  "result": "PASS",
  "security": {
    "firewall_passed": true,
    "authentication_executed": true,
    "controller_reached": true,
    "status": 200
  }
}
```

Suggested receipt rows:

- `security.firewall`
- `security.authorization`

## Profile C: CORS

Run a preflight:

```http
OPTIONS /query-endpoint
Origin: https://example.test
Access-Control-Request-Method: QUERY
Access-Control-Request-Headers: content-type,authorization
```

Record:

- `OPTIONS` status
- `Access-Control-Allow-Methods`
- whether `QUERY` appears
- `Access-Control-Allow-Headers`
- whether the actual `QUERY` succeeds afterward

Suggested receipt row:

- `cors.preflight`

## Profile D: Ayder Client Interoperability

Spring client should call:

```text
Spring -> Ayder direct
Spring -> Ayder through Envoy
```

Verify:

- native method sent
- JSON body retained
- `Accept-Query` readable
- `ETag` captured
- exact `ETag` reused through `If-None-Match`
- `304` handled without deserializing a normal body
- rate-limit headers captured and forwarded unchanged
- `415` exposed correctly
- equivalent JSON receives the same Ayder fingerprint

Ayder does not currently advertise `Content-Location`, because there is no
retrievable authenticated result resource. Spring should record:

```json
{
  "id": "spring.forwarding.content_location_preserved",
  "result": "NOT_APPLICABLE",
  "reason": "Ayder did not provide an upstream Content-Location header to preserve"
}
```

Suggested receipt rows:

- `representation.etag`
- `representation.revalidation`
- `spring.forwarding.content_location_preserved`
- `semantic.equivalent_json`
- `transport.direct_envoy_equivalence`

## Profile E: Receipt Parity

Spring should output the same row IDs where applicable:

- `core.native_query`
- `core.json_body_binding`
- `core.unsupported_content_type`
- `security.firewall`
- `security.authorization`
- `cors.preflight`
- `representation.etag`
- `representation.revalidation`
- `spring.forwarding.content_location_preserved`
- `semantic.equivalent_json`

Use the same statuses:

- `PASS`
- `FAIL`
- `NOT_SUPPORTED`
- `NOT_APPLICABLE`
- `UNVERIFIED`
- `OBSERVED`

This lets the Ayder direct, Ayder-through-Envoy, and Spring receipts be compared
line by line.
