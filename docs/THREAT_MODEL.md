# Threat model

Status: internal security hardening model for Revenue Readiness v1. This is not
a third-party penetration test, certification, or guarantee.

## Assets

- event payloads, keys, topics, and consumer offsets;
- idempotency records and recovery evidence;
- sealed append files and replay integrity;
- bearer credentials, TLS keys, and CA material;
- HA membership, leader identity, and replication traffic;
- availability, capacity, and operational telemetry.

## Trust boundaries

```text
untrusted HTTP client
        |
        v
HTTP framing -> authentication -> rate/resource policy -> route validation
        |                                           |
        v                                           v
 broker/shared memory                         admin/metrics APIs
        |
        v
sealed persistence ---- HA replication ---- peer TLS identity
        |
        v
operator-controlled filesystem, backups, and restore process
```

HTTP request bytes, bearer headers, paths, query strings, bodies, persistence
files during recovery, and HA network frames are treated as untrusted at their
parsing boundary. The host, process supervisor, explicitly provisioned secrets,
and storage permissions are operator trust anchors.

## Adversaries and failure actors

- unauthenticated internet or adjacent-network clients;
- authenticated tenants using malformed or resource-intensive requests;
- a client credential compromised without an authorization boundary;
- a peer with no valid CA identity or with a certificate for the wrong node;
- truncated, corrupt, or adversarial persistence input after disk failure;
- process crash, kill, partition, delayed I/O, short/partial writes, and restart;
- accidental operator exposure of metrics, debug controls, demo credentials, or
  test certificates.

Root compromise, malicious kernel/hypervisor behavior, physical attacks, and a
compromised configured CA are outside the guarantees of the broker process.
They still require deployment controls and incident response.

## Security objectives

1. Reject ambiguous HTTP framing before routing or authentication state can
   desynchronize.
2. Enforce bounded headers, bodies, decoded chunks, connections, and request
   lifetime.
3. Fail authentication and intended-HA startup closed.
4. Never label an append durable until the documented durability barrier is
   observed.
5. Reject HA peers that cannot prove the expected node identity under the
   configured CA.
6. Detect malformed persistence records without applying untrusted lengths or
   corrupt payloads.
7. Keep secrets out of source, logs, benchmark artifacts, and telemetry.
8. Expose administrative and operational detail only on an intentionally
   controlled network/authentication path.

## Principal threats and controls

| Threat | Current/added control | Residual gap |
|---|---|---|
| HTTP request smuggling | Conflicting TE/CL, invalid or conflicting CL, unsupported TE, malformed chunks, overflows, and leftover bytes are rejected by a bounded framing layer and regression/fuzz harnesses. | HTTP/1 policy changes must retain the 237 unit and 27 raw-socket regressions. |
| Parser memory corruption | Explicit limits, checked integer conversion, sanitizer builds, pure-parser fuzz targets. | Campaign duration in this sprint is short and does not constitute exhaustive fuzzing. |
| Credential guessing/theft | Bearer auth fails closed; benchmark capture redacts sensitive environment names; production guidance requires explicit secrets and TLS termination. | Tokens currently lack fine-grained data/admin/metrics authorization and automated rotation. |
| Cross-origin abuse | CORS is off by default; one exact configured origin is emitted, and it does not bypass authentication. | Browser deployments still require a reviewed origin, TLS, token storage, and CSRF/application policy. |
| Metrics intelligence leak | Metrics require bearer authentication by default and deployment guidance requires a private TLS scrape path. | Metrics use the broad bearer boundary rather than a dedicated scoped identity/listener. |
| Slowloris/resource exhaustion | Header/body/idle/absolute deadlines, body/header limits, active-connection caps, backpressure, and rate controls. | Distributed/resource-exhaustion behavior still needs target-topology load and fail-open policy review. |
| False durable acknowledgement | Sync watermark follows complete write plus fsync; receipts separate `local_fsync`, `applied_all_workers`, `quorum_committed`, `failover_safe`, and scope; malformed/timeouts/failures are typed and fail closed. | Local fsync is not HA failover proof; complete power/fault-ledger evidence remains pending. |
| HA downgrade on startup | Intended HA initialization failure is fatal. | Operators can still intentionally disable HA; configuration review is required. |
| Peer impersonation | mTLS CA validation, expected node-ID check, and per-connection binding prevent a verified peer from changing its claimed node. | Demo keys are public fixtures and must never be production trust material. Certificate rotation/revocation automation is absent. |
| Corrupt replay | CRC/length validation, forward progress, partial-tail truncation, file/directory sync, fail-closed readiness, checked partial/EINTR/zero-progress rewrite writes, and durable Raft state/log records. | Physical power/controller/filesystem campaigns and long decoder fuzzing remain incomplete. |
| Debug/test control abuse | Test/overload hooks are disabled by default and the live regression proves the default route is inactive. | Bearer authorization is not scoped by administrative action. |
| Cross-worker state divergence | Pending/ready topic publication and worker-bus mutation application are acknowledged only after all workers apply; failure/replay regressions pass. | Longer multiworker saturation/restart campaigns remain required. |
| Replayed/forged consensus messages | Peer mTLS identity is connection-bound; header bounds/CRC, stale-term/log-recency/previous-term validation and per-peer vote deduplication are tested. | Full post-change Jepsen and power/storage-fault evidence are pending; no broad Raft safety claim yet. |
| Idempotency-key collision or reuse | SHA-256 payload-bound, process-shared serialized durable `PENDING`/`COMMITTED` state rejects another payload and resolves uncertain durable records. | Longer HA/concurrent-fault coverage and external-effect reconciliation remain application responsibilities. |
| Supply-chain compromise | CI build/security workflows, pinned inputs where feasible, SBOM and scanner jobs. | A generated SBOM or scan is evidence only for its exact commit; signed provenance and organizational patch process remain gaps. |

## Deployment assumptions

- Bind client traffic to a private interface or place it behind a TLS reverse
  proxy with controlled ingress.
- Supply unique, high-entropy credentials through a secret manager; do not use
  examples or repository fixtures.
- Use least-privilege service/container users and filesystem permissions.
- Use HA mTLS with a private CA and one unique node identity per certificate.
- Restrict metrics and administration to an operations network.
- Back up persistent files using a tested, quiesced procedure and validate
  restore independently.
- Monitor readiness, 5xx, backpressure, fsync/storage errors, leader state, and
  restarts.

## Verification evidence

The executable evidence lives under `tests/security/`, `fuzz/`, build targets in
the `Makefile`, and the CI workflows. Exact commands, campaign durations, tool
availability, and findings belong in [REVENUE_READINESS_V1.md](REVENUE_READINESS_V1.md).
Unrun tools or pending campaigns are never described as passing.

## Explicitly unresolved release risks

At this sprint's current gate, the identified cross-worker, Raft protocol/state,
receipt, idempotency and rewrite-write defects have focused passing evidence.
The mandatory full fault campaign, power/storage testing, target hardware,
authorization scopes, native client TLS, dedicated metrics identity, key
lifecycle, clean signed release evidence and long fuzzing remain incomplete.
Any issue capable of
acknowledged-data loss, memory corruption, authentication bypass, or request
smuggling keeps `READY TO MERGE: NO` until fixed or explicitly deferred by the
founder after review.
