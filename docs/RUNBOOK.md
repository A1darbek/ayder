# Ayder operator runbook

This runbook is an evaluation baseline, not an SLA or a claim that Ayder is
production-ready. Validate every command against the exact commit, architecture,
filesystem, and durability contract you intend to operate.

## 1. Ownership and escalation

Before deployment, assign named owners for application, host/network, persistent
storage, security, and incident command. Record where alerts page, where logs
are retained, and who can authorize data restore or HA membership changes. The
project currently has no contractual support organization or response-time SLA.

## 2. Install and stage

Install the dependencies and build as described in
[`CONTRIBUTING.md`](../CONTRIBUTING.md). For a deployment artifact:

1. pin an exact reviewed commit;
2. build on the target architecture or a controlled compatible builder;
3. record compiler, flags, dependency versions, and the binary checksum;
4. run the smoke and required security/correctness workflows;
5. copy the binary to a read-only application directory; and
6. create a dedicated unprivileged account and a private writable data directory.

The repository defines a repeatable checksummed ARM64/amd64 build and GitHub
OIDC/Sigstore attestation workflow, but no final candidate bundle has yet been
issued and verified. Do not substitute an unverified binary merely because its
filename matches.
When Syft is installed, the local evidence helper can record a source SPDX SBOM,
artifact checksum, tool version, and dirty-tree state without committing its
outputs:

```bash
bash scripts/supply-chain/generate_local_evidence.sh ./ayder \
  /tmp/ayder-evidence-$(git rev-parse --short HEAD)
```

This is source/build evidence only, not a signature, provenance attestation,
runtime-library inventory, or clean-tree claim.

## 3. Configuration inventory

Store configuration in a root-readable environment file or secret manager, not
in shell history or a public repository. Relevant settings include:

| Area | Setting | Operator note |
| --- | --- | --- |
| HTTP | `--port`, `RF_HTTP_PORT`, `PORT`, `RF_HTTP_ADDR` | The host portion of `RF_HTTP_ADDR`/`--http` is not used for binding; the server currently listens on `0.0.0.0` |
| Workers | `--workers N`, `RAMFORGE_WORKERS` | `--workers 0` is the in-process single-worker mode; absent configuration defaults to online CPU count |
| Tokens | `RF_BEARER_TOKENS` | Comma-separated `token@rps:burst:daily`; use generated secrets and restart all workers to rotate |
| Metrics | `RF_METRICS_PUBLIC` | Leave unset; metrics require bearer auth by default. Public mode is an explicit unsafe opt-in unless an external private/authenticated boundary exists |
| CORS | `RF_CORS_ALLOWED_ORIGIN` | Leave unset unless browser access is required; accepts one exact non-wildcard origin and fails startup for invalid values |
| HTTP deadlines | `RF_HTTP_HEADER_TIMEOUT_MS`, `RF_HTTP_BODY_TIMEOUT_MS`, `RF_HTTP_IDLE_TIMEOUT_MS`, `RF_HTTP_REQUEST_TIMEOUT_MS` | Positive millisecond values; validate against slow legitimate clients and overload tests |
| Test hooks | `RF_ENABLE_TEST_HOOKS` | Leave unset outside isolated tests |
| Rate limit | `RL_DEFAULT_RPS`, `RL_DEFAULT_BURST`, `RL_DEFAULT_DAILY` | Defaults only; load-test the selected values |
| Backpressure | `BP_MAX_SEALED_Q`, `BP_MAX_ACTIVE_CONNS`, `BP_RETRY_AFTER_SEC` | Tune from capacity evidence, not the example alerts |
| Shared broker memory | `RF_BUS_HEAP_MB`, `RF_BUS_DESC` | Size against workload; confirm host and `/dev/shm` capacity |
| Metrics shared memory | `AYDER_METRICS_SHM_NAME` | Required unique value for collocated instances; absent HA node identity, the default is the global `/ayder_metrics`, and exclusive creation means stale/colliding segments can block startup |
| Logging | `LOG_LEVEL`, `LOG_TIMESTAMP` | Capture stdout and stderr externally |
| HA | `RF_HA_ENABLED`, `RF_HA_NODE_ID`, `RF_HA_NODES`, `RF_HA_WRITE_CONCERN`, `RF_HA_SYNC_MODE` | Treat membership and acknowledgement settings as correctness-sensitive |
| HA TLS | `RF_HA_TLS`, `RF_HA_TLS_CA`, `RF_HA_TLS_CERT`, `RF_HA_TLS_KEY`, `RF_HA_TLS_STRICT_ID`, `RF_HA_TLS_TIMEOUT_MS` | Require unique private certificates and strict peer identity |

Do not use `RF_METRICS_PUBLIC=1`, `RF_HTTP_DISABLE_RL=1`, broad
`RF_AUTH_SKIP_PREFIXES`, `RF_ENABLE_TEST_HOOKS=1`, or
`RF_HA_TLS_STRICT_ID=0` in a production-like environment without an explicit,
reviewed compensating control. The HTTP rate limiter currently fails open if
shared rate-limit storage is unavailable (`src/http_server.c:893-946`).
Authentication still runs before rate limiting on protected paths.

## 4. Generic Linux VM/systemd example

This unit is a starting point. It deliberately uses one in-process worker for a
simple evaluation topology and stable process-local signals. Focused
multiworker visibility/failure/replay regressions pass, but that does not close
HA, capacity, or longer durability campaigns. Adjust limits from measured
evidence.

```ini
[Unit]
Description=Ayder broker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ayder
Group=ayder
WorkingDirectory=/var/lib/ayder
EnvironmentFile=/etc/ayder/ayder.env
ExecStart=/opt/ayder/ayder --port 1109 --workers 0
Restart=on-failure
RestartSec=5s
TimeoutStopSec=120s
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/ayder
CapabilityBoundingSet=
AmbientCapabilities=
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

Set `/etc/ayder/ayder.env` to mode `0600`; set `/opt/ayder` root-owned and
non-writable by the service; set `/var/lib/ayder` owned by the service account.
Test stop behavior and sufficient shutdown time under load before relying on
this example. Place an authenticating TLS reverse proxy/private load balancer in
front of port 1109 and restrict direct ingress to that proxy and Prometheus.

After the HTTP shutdown-path correction, local working-tree `--workers 0` and
two-worker instances both exited cleanly after `SIGTERM`, left no worker
process, and removed their metrics shared-memory segment on 2026-08-15. This is
useful regression evidence, not a final loaded/systemd result or proof that
every durability queue flushed.

For AWS EC2 or Azure VM evaluation, apply the same pattern with a private subnet,
dedicated persistent block/data disk, deny-by-default security group/NSG,
instance/managed identity instead of static cloud credentials, encrypted volume,
central logs, monitoring, and health probes. No AWS AMI or Azure VM image is
currently built or certified by this repository.

## 5. Start and preflight

From the intended data directory:

```bash
umask 077
RF_BEARER_TOKENS="$(cat /run/secrets/ayder_tokens)" \
  /opt/ayder/ayder --port 1109 --workers 0
```

Prefer service-manager secret injection to command substitution in real
automation; the example illustrates that a non-default token is mandatory.
After start:

```bash
curl --fail --silent http://127.0.0.1:1109/health |
  jq --exit-status '.ok == 1'
curl --fail --silent http://127.0.0.1:1109/ready |
  jq --exit-status '.ready == true'
curl --config /etc/ayder/operator.curl.conf --fail --silent \
  http://127.0.0.1:1109/metrics |
  grep -E '^(ayder_persistence_ready|ayder_storage_writes_total)'
```

Store an `Authorization: Bearer ...` header in the mode-`0600`
`/etc/ayder/operator.curl.conf`; provision it from the secret manager and never
commit it. Prometheus should use its own raw token file as shown in
[`deploy/prometheus/prometheus.yml`](../deploy/prometheus/prometheus.yml).
The curl config contains a line of the form
`header = "Authorization: Bearer <raw-token>"`; replace the placeholder during
secret provisioning rather than storing a real token in documentation.
Ayder tokens are not route-scoped, so the Prometheus credential can reach other
protected APIs; restrict its file/network access and rotate it as a privileged
credential even when its rate/daily policy is scrape-specific.

Also verify that an invalid token is rejected, a valid token can perform a
non-destructive read, the data volume has expected ownership/capacity, logs have
no secret values, Prometheus is scraping, and alert routing works. For HA,
verify every node has `ramforge_ha_enabled 1`, exactly one expected leader is
reported by role, `ramforge_ha_leader_available 1`, and the role-0 leader reports
fresh quorum before accepting writes. Followers normally export fresh quorum as
zero.

The `--aof always` path maps a zero flush interval to the strongest mode. The
reviewed persistence initializer now treats that zero as enabled rather than as
an off sentinel. `/ready` also drops when the sealed path is unhealthy or a
shared persistence failure is recorded. Focused zero-flush, replay, and
readiness regressions pass on the 2026-08-15 dirty working tree; repeat them on
the immutable candidate and never infer the contract from the option name or
`/ready` alone.

## 6. Stop and maintenance

1. stop new client traffic and wait for in-flight clients to drain;
2. record `/ready`, queue gauges, storage failure counters, HA state, and logs;
3. request stop through the service manager;
4. wait for every Ayder process to exit; do not copy live files merely because
   the parent process disappeared;
5. preserve shutdown logs and verify the data directory remains owned and
   readable by the service account; and
6. if shutdown exceeds the tested timeout, capture process state before forcing
   termination and treat the next start as crash recovery.

There is no externally documented flush-complete endpoint. A zero queue is
useful evidence but is not by itself a durable-shutdown receipt.

Online sealed-log GC is intentionally unavailable in this sprint:
`POST /admin/sealed/gc` returns HTTP 503 with
`{"ok":false,"error":"sealed_gc_unavailable"}` because safe cross-process
drain, lock, rename, and descriptor-reopen coordination is not implemented.
Do not automate the endpoint or rewrite live sealed files manually.

## 7. Backup and restore

Ayder does not provide an online snapshot or point-in-time recovery command.
The evaluation-only cold tools create and verify an atomic directory-level
archive, manifest and checksums, but do not establish a general compatibility
window or RPO.

```bash
bash scripts/operations/cold_backup.sh DATA_DIR BACKUP_DIR /path/to/ayder
bash scripts/operations/cold_restore.sh BACKUP_DIR NEW_DATA_DIR
```

The backup command refuses a live data directory, detects changes during
capture, and fsyncs its outputs. Restore verifies manifest, archive and per-file
checksums, rejects unsafe archive entries, stages the result, preserves modes,
and fsyncs the restored files and parent. Keep the manual checks below: a tool
success is not an application-level recovery proof.

For evaluation-only cold backup:

1. drain clients and stop every process as above;
2. snapshot or copy the entire data directory as one unit, including all AOF,
   sealed, rotated, snapshot, and metadata files present;
3. record exact commit/binary SHA-256, environment with secrets redacted,
   filesystem, ownership/mode, wall-clock time, and a checksum manifest;
4. store the backup encrypted and access-controlled; and
5. restart and recheck readiness.

Restore drills must use an isolated host/network first:

1. install the recorded binary and dependencies;
2. restore the entire directory with original ownership/modes;
3. start with no external write traffic;
4. capture replay/recovery logs and confirm readiness;
5. verify application-level record/offset invariants and expected record counts;
6. run read-only recovery/QUERY inspection; and
7. only promote after the owner signs off the evidence.

Do not truncate, concatenate, hand-edit, or selectively copy AOF/sealed files.
Escalate CRC/corruption errors and preserve the original media. A restore that
starts is not proof that every acknowledged record survived.

## 8. Crash recovery

On startup the persistence path loads the AOF and replays sealed records before
setting readiness (`src/persistence_zp.c:41-66`). After an unexpected exit:

- isolate the node from writes;
- snapshot/copy the data volume before remediation where feasible;
- retain prior and recovery logs;
- start the exact binary once, watching recovery attempt/success counters;
- require `/ready` and application-level invariant checks; and
- compare acknowledged-write evidence with recovered records.

Repeated restart attempts can destroy useful evidence or compound an operator
mistake. Disable restart-on-failure during corruption investigation.

## 9. Upgrade and rollback

There is no published storage-format compatibility matrix, schema migration
tool, rolling-upgrade protocol, or general rollback guarantee. The repository
contains `scripts/operations/upgrade_rollback_drill.sh`; the 2026-08-16 ARM64
WSL2 drill with distinct old/fixed binary hashes passed a cold-backup upgrade
and restored rollback. That one pair is evidence for the exercise, not a broad
compatibility promise.

For each upgrade, take and validate a cold backup, reproduce the workload and
recovery tests in staging, compare metrics, and rehearse failure. Use canary
traffic only if the data/HA topology keeps the test isolated. Do not mix binary
versions in an HA cluster unless that exact combination has passed compatibility
and fault tests.

Rollback means restoring both the prior binary and a backup known compatible
with it. Never run an older binary against files already modified by a newer
binary without explicit compatibility evidence.

## 10. Capacity planning and scaling

No universal CPU, RAM, disk, IOPS, throughput, latency, or retention sizing
number is published. Benchmark the exact payload distribution, durability mode,
fsync semantics, worker count, auth/rate limits, client topology, storage class,
and HA write concern on the target architecture.

Track at minimum CPU, RSS, file descriptors, active connections, request and
operation rate, response classes, backpressure, queue depth, storage failures,
fsync/sealed latency, disk capacity/latency from the host, and recovery time.
Leave headroom for replay, compaction/rotation, traffic bursts, and a failed HA
node. Current metrics do not expose disk free space; monitor the volume with a
host/cloud agent.

Scale workers only after measuring correctness and telemetry behavior. Scale
out with HA only after quorum, failover, leader routing, mTLS identity, recovery,
and membership-change tests pass. Do not put independent single-node instances
behind a load balancer and call that a replicated cluster.

Revenue Readiness v1 initially found a 30–40% load-balanced visibility failure.
The fixed broker now publishes topics/records through process-shared state and
waits for every local worker. Focused regressions observed 400/400 responses,
rejected an injected apply failure, and repaired all workers after restart.
This closes the identified defect but does not supply a production sizing
envelope: the strict durable benchmark achieved only 308–382 RPS with timeouts.

## 11. HA setup boundary

The scripts under `cluster/` demonstrate local topology and are not production
automation. Private keys previously committed there remain in Git history, and
any regenerated keys or example bearer tokens are unsafe outside a disposable
local environment.

A production-like HA evaluation needs unique node IDs and addresses, an odd
voter set, explicit write concern/sync mode, private peer networking, a private
CA, unique node certificates, strict peer identity, controlled membership
changes, clock monitoring, and client behavior for leader redirects. The HA TLS
implementation sets TLS 1.2 as the minimum and requires peer certificates
(`src/ramforge_ha_tls.c:107-169`), but deployment-specific identity and failure
tests are still required.

The current HA state machine durably recovers term, voted-for, log,
commit/applied state and validates stale terms, candidate recency, previous-log
terms and per-peer vote deduplication. Short kill/partition/mixed Jepsen runs
passed exact post-run convergence. A crash-safe/linearizable Raft claim is still
withheld until the mandatory complete post-change campaign and stronger
power/storage-fault ledger tests pass.

Never infer failover safety from `durable:true` or `synced:true`. Require the
explicit response fields `durability_scope:"quorum"`, `local_fsync:true`,
`quorum_committed:true`, `failover_safe:true`, and
`applied_all_workers:true`.

## 12. Incident response

| Signal/symptom | Immediate action | Evidence and likely next checks |
| --- | --- | --- |
| `AyderUnavailable` | Stop routing; check host/process/network | service status, exit code, kernel/OOM, listener, recent deploy |
| `AyderPersistenceUnavailable` | Remove from write traffic | recovery logs, storage mount, permissions, storage failure counter |
| Storage write failure | Stop writes and preserve the volume | errno/log context, filesystem/kernel/cloud-volume events, free space |
| Sustained 5xx | Reduce/stop traffic if impact grows | status mix, broker errors, readiness, deployment diff |
| Backpressure/queue growth | Shed offered load; do not disable durability | queue trends, fsync latency, active connections, disk latency |
| High server-side HTTP p99 | Protect the latency/error budget; compare recent deploy/load changes | HTTP duration p50/p95/p99, status mix, operation rate, queues, host saturation, client-side latency |
| High persistence p99 | Protect latency/error budget | host I/O latency, queue depth, noisy neighbors; remember it is not HTTP p99 |
| No HA leader/fresh quorum | Freeze writes and prevent blind retries | node reachability, terms/roles, peer TLS, clocks, membership, partition |
| Restart loop | Disable automatic restart after evidence capture | core/ASan output, OOM, signals, storage corruption, config |
| Recovery mismatch | Isolate and escalate | acknowledged-write receipts, checksums, replay logs, exact binary/config |
| Suspected compromise | Isolate network and rotate credentials from a clean control plane | preserve disks/logs/memory where policy permits; follow `SECURITY.md` |

Do not “fix” an incident by selecting `--aof never`, weakening write concern,
disabling HA TLS, clearing the data directory, or silently dropping bad records.

## 13. Disaster recovery and objectives

Ayder has no defensible default RPO, RTO, SLA, or cross-region DR
contract. Those objectives depend on an independently tested durability mode,
backup cadence/atomicity, restore validation, HA topology, storage failure
domain, dataset size, and operator response.

A narrow 2026-08-16 process-crash exercise set RPO=0 acknowledged records and
RTO=5 seconds; it recovered 100/100 IDs with zero duplicates and reached ready
in 74 ms on WSL2 ext4. Treat that only as proof that the drill mechanism and
specific candidate met that specific objective.

A pilot must set its own target RPO/RTO, then demonstrate them with timed exercises:
process crash, host loss, volume loss, leader loss, network partition, corrupt
copy, and full restore. Record acknowledged writes before the fault and compare
them with recovered data. An untested objective is not an achieved objective.

## 14. Log collection checklist

- Capture stdout and stderr from every worker and the parent/service manager.
- Add UTC receive time, host, environment, commit/binary hash, node ID, and
  worker/process identifiers in the collector.
- Collect systemd/journal, kernel/OOM, filesystem, cloud-volume, load balancer,
  reverse-proxy, and Prometheus/Alertmanager events on the same timeline.
- Redact bearer tokens, certificate keys, Authorization headers, and payloads.
- Restrict access and define retention/legal hold before a pilot.
- Preserve raw incident logs before filtering or restarting.

Current logs are human-oriented and are not a complete, tamper-evident audit
trail. See [`OBSERVABILITY.md`](OBSERVABILITY.md) for metric-specific caveats.
