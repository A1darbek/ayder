# Observability

This document describes only telemetry present in the current source tree. It
does not imply that every production signal requested by an SRE is available.

## Endpoints and probes

| Endpoint | Purpose | Current response | Ayder-layer authentication |
| --- | --- | --- | --- |
| `GET /health` | Process liveness | HTTP 200, `{"ok":1}` | None |
| `GET /ready` | Persistence write readiness | HTTP 200 with `{"ready":true}` or HTTP 503 with `{"ready":false}` | None |
| `GET /metrics` | Broker, HTTP, and persistence metrics | Prometheus text | Bearer token required by default; `RF_METRICS_PUBLIC=1` is an explicit public opt-in |
| `GET /metrics_ha` | HA snapshot metrics, or a comment when HA is disabled/unavailable | Prometheus-compatible text | Bearer token required by default; same opt-in applies |

Evidence: public-path handling is in `src/http_server.c:602-609`; readiness is
in `src/app_routes.c:2358-2368`; metric route registration is in
`src/app_routes.c:2836-2859`.

Use both status and body for readiness:

```bash
curl --fail --silent --show-error http://127.0.0.1:1109/health |
  jq --exit-status '.ok == 1'

curl --fail --silent --show-error http://127.0.0.1:1109/ready |
  jq --exit-status '.ready == true'
```

`/health` only proves that the HTTP process can answer. `/ready` requires local
persistence initialization, a healthy sealed path, and no shared persistence
failure (`src/persistence_zp.c:20-22`). It still does not prove available disk
space, HA quorum, leader availability, or that an acknowledged-write recovery
campaign passed. A load balancer for writes should use `/ready` and, for HA, an
independently verified leader-routing strategy.

## Network security

Ayder binds its HTTP listener to `0.0.0.0` (`src/http_server.c:1921`). Health and
readiness remain public; metrics are protected by the same bearer-token layer
as application routes unless `RF_METRICS_PUBLIC=1` is set. The server does not
terminate client HTTP TLS. In a production-like deployment:

1. place Ayder and Prometheus on a private subnet or loopback-only proxy path;
2. allow the scrape port only from the Prometheus identity/security group;
3. configure Prometheus with a dedicated bearer token and scrape-appropriate
   rate/daily policy, then terminate TLS at a reverse proxy or private load
   balancer. Current tokens are bearer-wide rather than endpoint-scoped;
4. leave `RF_METRICS_PUBLIC` unset and never publish `/metrics_ha` directly to
   the Internet; and
5. treat metric labels and values as operationally sensitive.

Bearer authentication prevents anonymous scraping by default but does not
encrypt the token or response, and a valid metrics token can reach other
protected routes. Keep the port private, isolate the Prometheus credential, and
use a trusted TLS boundary. If `RF_AUTH_SKIP_PREFIXES` includes either metric
path, that runtime override also bypasses the default and must be treated as a
security exception.

## Metric inventory

The `/metrics` exporter is implemented in
`src/ramforge_rotation_metrics.c:638-778`.

| Metric | Type | Meaning | Important limitation |
| --- | --- | --- | --- |
| `ayder_aof_files` | gauge | Current number of AOF segments known to rotation metrics | Not a disk-usage or disk-error signal |
| `ayder_aof_generation` | gauge | Current AOF rotation generation | A change is not by itself an error |
| `ayder_process_start_time_seconds` | gauge | Unix timestamp when the broker process group started | Supports restart detection only while Prometheus retains the prior sample |
| `ayder_persistence_ready` | gauge | `1` after local initialization while the sealed path is healthy and no shared persistence failure is recorded | Does not prove free disk space, HA quorum, or recovery correctness |
| `ayder_http_requests_total` | counter | HTTP requests processed by all workers in the process group | Shared across workers |
| `ayder_http_responses_total{code="2xx"}` | counter | Responses in the 2xx class | Shared across workers |
| `ayder_http_responses_total{code="3xx"}` | counter | Responses in the 3xx class | Shared across workers |
| `ayder_http_responses_total{code="4xx"}` | counter | Responses in the 4xx class | Includes 401 and 429; do not add the exact-code series to this class total |
| `ayder_http_responses_total{code="401"}` | counter | Unauthorized responses | Subset of `4xx` |
| `ayder_http_responses_total{code="429"}` | counter | Rate-limited responses | Subset of `4xx` |
| `ayder_http_responses_total{code="5xx"}` | counter | Responses in the 5xx class | Includes 503; shared across workers |
| `ayder_http_responses_total{code="503"}` | counter | Service-unavailable responses | Subset of `5xx`; covers backpressure and other 503 paths |
| `ayder_http_active_connections` | gauge | Connections active across workers | Shared across workers |
| `ayder_http_request_duration_seconds` | histogram | Server time from first request byte through handler completion | Shared across workers; excludes proxy, network, and client time and has no route/method/status labels |
| `ayder_broker_operations_total{operation}` | counter | Broker handler operations for `produce`, `consume`, `commit`, and `query` | Operation count, not record or byte count; shared across workers |
| `ayder_broker_errors_total` | counter | Broker handler errors | Not labeled by route or error class; shared across workers |
| `ayder_backpressure_events_total` | counter | Requests rejected by the broker backpressure gate | Distinct from other 503 responses; shared across workers |
| `ayder_storage_writes_total{outcome}` | counter | Storage writes labeled `success` or `failure` | Does not identify errno, disk, or path; shared across workers |
| `ayder_recovery_attempts_total` | counter | Persistence recovery attempts | Shared across workers |
| `ayder_recovery_successes_total` | counter | Successful persistence recoveries | Compare with attempts and logs; no explicit failure-reason label |
| `ayder_aof_pending_writes` | gauge | Pending writes in AOF ring/io_uring paths | Process-local exporter view; a value of zero does not prove durability |
| `ayder_sealed_queue_depth` | gauge | Sealed batches ready for fsync | Process-local exporter view |
| `ayder_fsync_duration_seconds` | histogram | Observed fsync-group duration | Process-local; not end-to-end HTTP latency |
| `ayder_sealed_durable_latency_seconds` | histogram | Time from sealed append to durable fsync | Process-local; not end-to-end HTTP latency |

The `/metrics_ha` exporter is implemented in
`src/ramforge_ha_integration.c:963-1015`. When HA is disabled it still exports
the enabled and leader-available gauges as zero; the remaining series require a
shared HA snapshot.

| Metric | Type in practice | Meaning |
| --- | --- | --- |
| `ramforge_ha_enabled` | gauge | `1` when HA is configured on the node, otherwise `0` |
| `ramforge_ha_role` | gauge | `0` leader, `1` follower, `2` candidate, `3` learner |
| `ramforge_ha_state` | gauge | `0` initializing, `1` ready, `2` syncing, `3` degraded, `4` failed |
| `ramforge_ha_term` | gauge | Current observed HA term |
| `ramforge_ha_epoch` | gauge | Membership configuration epoch |
| `ramforge_ha_joint_active` | gauge | `1` while a joint membership configuration is active, otherwise `0` |
| `ramforge_ha_fresh_quorum` | gauge | `1` when a leader reports a fresh quorum; followers export `0`, so always join/filter on role 0 |
| `ramforge_ha_leader_available` | gauge | `1` when the node knows a current leader, otherwise `0` |
| `ramforge_ha_log_last_index` | gauge | Last index in the node's durable Raft log |
| `ramforge_ha_log_last_term` | gauge | Term of that last durable log entry |
| `ramforge_ha_commit_index` | gauge | Highest durably committed Raft index |
| `ramforge_ha_applied_index` | gauge | Highest Raft index durably applied to the state machine |

The role and state numeric mappings are defined in
`src/ramforge_ha_config.h:9-22`. The HA endpoint emits HELP/TYPE metadata but
does not expose leader identity, per-peer replication lag, election totals, or
heartbeat ages. The last/commit/applied gauges support convergence checks but
do not replace an acknowledged-write ledger.

### Multi-worker aggregation warning

Ayder workers accept connections with `SO_REUSEPORT`. Process start, HTTP
counters and request duration, operation, backpressure, storage, and recovery
series use shared memory across the worker group. HA data also comes from a
shared snapshot. Those series can be scraped through any worker without summing
per-worker copies.

The AOF queue gauges and persistence-duration histograms still read
process-local state from the worker that answers the scrape. Prometheus can
therefore reach a different worker on successive scrapes and see an incomplete
view. Run a single worker (`--workers 0`) when queue-depth and
persistence-duration alerts must be trustworthy, or wait for explicit worker
aggregation/stable per-worker labels.

### Histogram interpretation

The shared HTTP request-duration histogram has finite buckets from 0.5 ms
through 1 second (`src/http_server.c:169-180`) and includes observations above
1 second in `le="+Inf"`. It measures from the first request byte through handler
completion (`src/http_server.c:1273-1280`), not load-balancer, TLS-proxy,
network, or client time. Its supplied 250 ms p99 alert threshold is an example;
derive a real threshold from the service SLO and measured baseline.

The process-local fsync and sealed-durability histograms have finite buckets
through 0.5 seconds. All three histograms emit the total observation count for
`le="+Inf"`, including observations above their largest finite bucket
(`src/ramforge_rotation_metrics.c:682-702,755-778`). This preserves the
Prometheus `+Inf == _count` invariant. Use a stable/single-worker scrape before
relying on persistence percentiles. The supplied 100 ms persistence thresholds
are examples and measure storage paths, not HTTP requests.

### Exposition lint status

The working-tree exporter names the AOF segment gauge `ayder_aof_files`, emits
HELP/TYPE metadata for it and `ayder_aof_generation`, and preserves histogram
structure. A local exact-current GCC build and authenticated live scrape passed
`promtool check metrics` with Prometheus 2.53.5 on 2026-08-15 with no lint
findings. Repeat that check
against the immutable final release binary; a working-tree result is not
release evidence.

## Example Prometheus and Grafana deployment

The example files are:

- [`deploy/prometheus/prometheus.yml`](../deploy/prometheus/prometheus.yml)
- [`deploy/prometheus/ayder-alerts.yml`](../deploy/prometheus/ayder-alerts.yml)
- [`deploy/grafana/ayder-dashboard.json`](../deploy/grafana/ayder-dashboard.json)

Copy both Prometheus files into the Prometheus container/host, preserving the
rule path `/etc/prometheus/ayder-alerts.yml`, or adjust `rule_files`. Put only
the raw bearer token in `/etc/prometheus/ayder.token`, restrict it to the
Prometheus service account, and configure the corresponding
`token@rps:burst:daily` entry in Ayder's `RF_BEARER_TOKENS`. Replace `ayder:1109`
and the `environment` label. For an HA deployment, list every node under both
scrape jobs and replace `ha_cluster: disabled` with the same stable cluster name
on each node. Leaving it `disabled` suppresses HA-specific alerts for a normal
single-node deployment.

Validate before reload when `promtool` is installed:

```bash
promtool check config --syntax-only deploy/prometheus/prometheus.yml
promtool check rules deploy/prometheus/ayder-alerts.yml
```

Run the full `promtool check config` in the deployment environment after the
referenced rule and credential files are installed. A syntax-only check cannot
prove that either file exists or is readable by Prometheus.

As a working-tree integration check on 2026-08-09, Prometheus 2.53.5 loaded the
rules plus a temporary credential file and reported `up == 1` for both the
authenticated `/metrics` and `/metrics_ha` jobs. Repeat this with the release
binary, deployed secret permissions, service discovery, TLS path, and final
Prometheus version.

Import the Grafana JSON and select a Prometheus datasource when prompted. Panel
thresholds and alert thresholds are examples, not measured capacity limits.

## Supplied alerts

| Alert | Trigger | Operator intent |
| --- | --- | --- |
| `AyderUnavailable` | Prometheus `up == 0` for two minutes | Check process, listener, network policy, and host health |
| `AyderPersistenceUnavailable` | Persistence-ready remains zero for one minute | Remove from write traffic and inspect recovery/storage logs |
| `AyderSustained5xx` | More than 5% 5xx for ten minutes while traffic is over 1 request/second | Inspect logs, readiness, disk, and recent changes |
| `AyderSustainedBackpressure` | More than 0.1 explicit backpressure events/second for ten minutes | Inspect queues, connections, storage, and offered load |
| `AyderHighHTTPP99` | Server-side HTTP p99 over example 250 ms for ten minutes | Inspect handler/storage/load behavior; correlate with client-side telemetry and tune the threshold |
| `AyderRestartLoop` | Process start timestamp changes more than twice in 15 minutes | Disable automatic churn, preserve logs/data, and find the exit cause |
| `AyderAOFQueueGrowth` | More than 1,000 pending AOF writes for ten minutes | Investigate storage latency and offered load; tune threshold |
| `AyderSealedQueueGrowth` | More than 1,000 sealed batches for ten minutes | Investigate fsync/storage health and offered load; tune threshold |
| `AyderStorageWriteFailures` | At least one recorded failed storage write in five minutes | Preserve evidence and inspect the persistent volume before restart |
| `AyderHighFsyncP99` | Fsync p99 over example 100 ms for ten minutes | Investigate storage latency; valid only with a stable/single-worker scrape |
| `AyderHighSealedDurabilityP99` | Sealed-durability p99 over example 100 ms for ten minutes | Investigate durable path; this is not HTTP request latency |
| `AyderHANoLeader` | HA enabled but no node reports a leader available | Freeze or redirect writes and investigate election/quorum state |
| `AyderHAFreshQuorumLost` | The role-0 leader reports no fresh quorum | Investigate peer reachability, clocks, and HA logs; followers are excluded |
| `AyderHAStateFailed` | An HA node reports state 4 | Treat the node as failed and collect evidence before restart |

The no-leader expression cannot create a cluster label when every HA endpoint
is absent; `AyderUnavailable` must cover that case. A leader's fresh-quorum
signal is a coarse replication-health proxy, not a lag measurement. A follower's
zero value is normal and must not be alerted on.

## Useful queries

Shared request, response, and broker-operation rates:

```promql
rate(ayder_http_requests_total{job="ayder"}[5m])
rate(ayder_http_responses_total{job="ayder",code="5xx"}[5m])
rate(ayder_http_responses_total{job="ayder",code="429"}[5m])
rate(ayder_broker_operations_total{job="ayder"}[5m])
rate(ayder_backpressure_events_total{job="ayder"}[5m])
histogram_quantile(0.50, sum by (instance, le) (rate(ayder_http_request_duration_seconds_bucket{job="ayder"}[5m])))
histogram_quantile(0.95, sum by (instance, le) (rate(ayder_http_request_duration_seconds_bucket{job="ayder"}[5m])))
histogram_quantile(0.99, sum by (instance, le) (rate(ayder_http_request_duration_seconds_bucket{job="ayder"}[5m])))
```

Pressure signals:

```promql
ayder_aof_pending_writes{job="ayder"}
ayder_sealed_queue_depth{job="ayder"}
ayder_http_active_connections{job="ayder"}
```

HA inventory:

```promql
ramforge_ha_role{job="ayder-ha",ha_cluster!="disabled"}
ramforge_ha_state{job="ayder-ha",ha_cluster!="disabled"}
ramforge_ha_fresh_quorum{job="ayder-ha",ha_cluster!="disabled"}
ramforge_ha_leader_available{job="ayder-ha",ha_cluster!="disabled"}
```

## Missing signals and required follow-up

The following cannot be derived honestly from current metrics and therefore
have no supplied alert:

- per-route/method/status request rate and latency breakdowns;
- client-observed end-to-end latency including proxy and network time;
- per-operation record/byte rates (operation invocation rates are available);
- disk bytes/free space, I/O errors, and last successful persistence time;
- broker errors labeled by route, subsystem, or error reason;
- storage failure errno/reason/path labels;
- leader identity (leader availability is available as a boolean);
- commit/applied index and per-peer replication lag/health; and
- recovery/replay phase, duration, and completion state.

Prioritize disk capacity/error signals, HTTP histogram validation/SLO
baselining, per-worker persistence queue aggregation, and HA leader/lag detail
before treating this as production-grade observability.

## Logs

Ayder writes human-oriented logs to stdout/stderr. `LOG_LEVEL` accepts names
such as `ERROR`, `WARN`, `INFO`, and `DEBUG`; `LOG_TIMESTAMP=1` enables local
timestamps (`src/log.h:29-61`). Capture both streams, add host/process metadata
in the collector, redact tokens and customer payloads, restrict access, and set
retention. There are no structured JSON logs, correlation IDs guaranteed across
all paths, audit-log integrity controls, or documented log schema today.
