# Enterprise readiness gap matrix

Assessment date: 2026-08-16. Scope: repository-level technical readiness for a
bounded pilot evaluation. This is not AWS or Microsoft approval, a third-party
audit, penetration test, SOC 2/ISO/NIST certification, legal advice, or a claim
of production use.

Status vocabulary:

- **IMPLEMENTED** — present in the working tree; still requires commit-specific
  validation where noted.
- **PARTIAL** — useful control exists but does not meet the full expectation.
- **PENDING EVIDENCE** — executable artifact exists, but no passing result for
  the final commit is asserted.
- **GAP** — not implemented or not supportable from current evidence.
- **EXTERNAL** — organization/legal/commercial control that code cannot complete.

“Blocks pilot?” means a real-data or paid enterprise pilot under ordinary
infrastructure review. A tightly isolated, synthetic-data technical evaluation
may accept more gaps with written risk acceptance.

Current technical gate: **RED; READY TO MERGE: NO**. The identified cross-worker,
durable-state/protocol, receipt and idempotency defects have focused passing
regressions, but the mandatory full post-change Jepsen campaign, native target
storage evidence and final clean-commit CI/provenance remain blockers.

## Security

| CONTROL / EXPECTATION | CURRENT STATUS | EVIDENCE | GAP | ACTION | BLOCKS PILOT? YES/NO |
| --- | --- | --- | --- | --- | --- |
| Secure defaults | PARTIAL | Main and musl containers require `RF_BEARER_TOKENS`, run UID 10001, compose binds loopback, metrics require auth, and CORS is off | Native listener is `0.0.0.0`; bearer authorization is broad | Add bind/scoped-admin controls and verify all examples | YES |
| Authentication | PARTIAL | Protected routes enforce strict bearer syntax; missing/malformed/duplicate-header live cases pass | Bearer-only; no expiry, identity metadata, online revocation, or scoped service identity | Document/integrate identity and lifecycle boundary | YES |
| Authorization | GAP | Public-vs-protected route split only | No tenant/topic/admin scopes or roles | Define authorization model and enforce least privilege | YES |
| Token lifecycle | GAP | `RF_BEARER_TOKENS` loads bounded token configs at startup | No online issue/revoke/rotate API, expiry, identity metadata, or audit | Integrate secret manager; define dual-token rotation/restart procedure; add revocation | YES |
| TLS for client traffic | GAP | External TLS termination is documented | Ayder HTTP listener has no native TLS and cannot bind a requested host interface | Require private proxy/LB TLS and network policy; test forwarded behavior | YES |
| HA mTLS | PARTIAL | TLS 1.2 minimum, CA/client-cert validation, node-ID rejection, and per-connection peer binding; negative identity test passes | PKI issuance/rotation/revocation and expiry/partition campaigns absent | Use private CA/unique identities; run complete PKI/fault tests | YES for HA |
| Encryption in transit | PARTIAL | HA mTLS plus documented external client TLS boundary | Metrics/admin/client traffic can be plaintext on direct port | Enforce TLS/private networking at every ingress and scrape boundary | YES |
| Encryption at rest | GAP | Filesystem permissions are used | AOF/sealed data is not application-encrypted | Require encrypted cloud volume/host disk; document key owner/rotation | YES when customer policy requires |
| Least privilege | PARTIAL | Main, musl and benchmark images use numeric non-root UIDs; compose uses `no-new-privileges`; HA compose drops capabilities | Filesystem/runtime policy and read-only-rootfs evidence incomplete | Test read-only rootfs and minimal writable paths | YES |
| Network exposure | PARTIAL | Compose maps loopback; metrics require auth; runbook specifies private TLS ingress | Binary still binds `0.0.0.0`; metrics share broad bearer authorization | Add bind-address/dedicated operations identity; deny-by-default SG/NSG | YES |
| CORS | IMPLEMENTED | Default emits no origin; exact configured origin and suffix rejection pass live tests | Browser clients still need reviewed TLS/token/CSRF policy | Retain exact-origin regressions and document browser deployment | NO if browser access is disabled |
| Secrets handling | PARTIAL | Container requires injected token; benchmark capture redacts sensitive names; tracked fixtures are removed; Gitleaks reports zero unsuppressed findings after exact historical fingerprints | Removed development keys remain public/compromised in history; no dedicated security mailbox | Never reuse fixtures; provision pilot PKI; retain fingerprint-specific triage and assign monitored security owner | YES |
| Audit logging | GAP | Human logs can be externally collected | No complete structured, immutable security/admin audit trail | Define events/schema/redaction/retention and protected sink | Depends on pilot, usually YES |
| Request validation | PARTIAL | 237 pure framing checks, 27 raw cases, 31 live smoke checks on normal/hardened/sanitized binaries, route validation, and fuzz-smoke pass | Long fuzzing plus AOF/HA decoder coverage and immutable final-run evidence absent | Run long/decoder campaigns and final CI; triage every finding | YES |
| DoS/resource controls | PARTIAL | Header/body/idle/absolute deadlines, active connection cap, limits, rate control, and backpressure pass focused live tests | Distributed saturation/fail-open policy and target-topology capacity evidence absent | Define overload policy and run topology-specific exhaustion tests | YES |
| Dependency security | PARTIAL | SBOM/CVE workflow and checksummed libuv download in main Dockerfile | System dependencies and musl fallback/mutable downloads are not fully locked/scanned | Pin all sources/checksums; scan built images and runtime packages | YES |
| Vulnerability disclosure | PARTIAL | [`SECURITY.md`](../SECURITY.md) defines a private GitHub path and disclosure expectations | No dedicated monitored security mailbox or response SLA | Enable/test private reporting; assign owners/on-call | YES for paid pilot |
| Patch process | GAP | CI workflows can validate candidate fixes | No supported release branches, severity SLA, advisory/release process, or customer notification | Define ownership, severity targets, backport/release/advisory procedure | YES |

## Software supply chain

| CONTROL / EXPECTATION | CURRENT STATUS | EVIDENCE | GAP | ACTION | BLOCKS PILOT? YES/NO |
| --- | --- | --- | --- | --- | --- |
| Dependency inventory | PARTIAL | Makefile/Dockerfiles name direct system libraries; source SBOM workflow exists | No reviewed transitive/runtime inventory for a final artifact | Archive source and image SBOMs per release | YES |
| SPDX/CycloneDX SBOM | PARTIAL | Syft 1.51.0 generated/validated 143-package source and one-package binary dirty-tree SPDX catalogs; workflow emits clean-commit artifacts | Local output is unsigned and not a final image/runtime inventory | Run, review, sign, and retain clean source/image SBOMs | YES |
| CVE scanning | PARTIAL | Trivy 0.74.0 final local scan reports zero vulnerabilities and zero HIGH/CRITICAL misconfigurations after remediation | Dirty-tree filesystem scan has limited compiled-system-library visibility; no final image scan | Run clean candidate and built-image/package scans under triage policy | YES |
| Secret scanning | PARTIAL | Gitleaks 8.30.1 scanned all 131 commits with zero unsuppressed findings; eight removed public fixture keys have exact fingerprint exceptions | Final clean candidate CI and staffed rotation/response process pending | Enforce scan and never broaden the historical exceptions | YES |
| CodeQL C/C++ | PENDING EVIDENCE | `codeql.yml` initializes `c-cpp` and performs a manual build | No passing result or finding triage for final commit | Run on PR/default branch and retain results | YES |
| Static analysis | PARTIAL | GCC `-fanalyzer` passes framing, tests, HTTP, storage and AOF; warning-as-error build passes; CodeQL defined | Clang-tidy unavailable and no final CodeQL triage | Run CodeQL/other analyzers on candidate | YES for high-impact findings |
| ASan/UBSan | PARTIAL | GCC ASan+UBSan parser, fuzz-smoke, route, and live security paths pass without diagnostics | Forked live leak detection disabled; TSan and immutable candidate run absent | Run final candidate; add focused TSan where feasible | YES |
| Reproducible/repeatable release | PARTIAL | Portable warning-clean ARM64/amd64 build and deterministic packaging/provenance workflow defined | Mutable OS packages and no completed final workflow run | Pin/record build image and retain final manifest/artifacts | YES |
| Source-to-binary traceability | GAP | Operators can record commit and SHA manually | Binary does not provide embedded version/build metadata; no automated manifest | Embed/version safely and publish commit/toolchain/hash manifest | YES |
| SHA-256 checksums | PARTIAL | Dockerfile verifies libuv; release workflow creates artifact checksums | No published final-candidate checksum bundle | Complete workflow and retain signed bundle | YES |
| Signed release artifacts | PENDING EVIDENCE | GitHub OIDC/Sigstore attestation bundles are defined | No final workflow-issued bundle exists | Run protected candidate workflow and publish verification instructions | Usually YES |
| Provenance | PENDING EVIDENCE | ARM64/amd64 workflow requests provenance and SBOM attestations | Definition is not an issued attestation | Complete and verify controlled final run | Usually YES |
| Immutable CI dependencies | IMPLEMENTED/PARTIAL | Third-party Actions are pinned to commit SHAs and actionlint passes | Review/update ownership and final run pending | Automate reviewed updates | YES |
| Patch/vulnerability cadence | EXTERNAL/GAP | Scheduled CodeQL/scanner jobs are declared | No staffed triage cadence, SLAs, or release channel | Assign security owner and documented severity deadlines | YES |

Workflow presence does not make the supply chain secure. A reviewer needs the
exact run, tool version, findings, exceptions, generated SBOM, binary digest,
and provenance for the candidate artifact.

## Fuzzing and adversarial testing

| CONTROL / EXPECTATION | CURRENT STATUS | EVIDENCE | GAP | ACTION | BLOCKS PILOT? YES/NO |
| --- | --- | --- | --- | --- | --- |
| HTTP framing harness | IMPLEMENTED | `fuzz/http_framing_fuzz.c`, corpus/dictionary; ASan+UBSan standalone smoke ran 855 executions | No long coverage-guided campaign; result is a dirty-tree snapshot | Run/retain a long final campaign with exact duration/findings | YES until final clean campaign |
| TE/CL, duplicate/malformed CL, chunk overflow/malformed chunks | IMPLEMENTED | 237 pure checks and 27 hardened raw-socket cases pass | Future parser changes require the same adversarial suite | Enforce in CI and retain final logs | NO for isolated evaluation |
| Header/body/count/fragmentation/auth/CORS/metrics/slow-client tests | IMPLEMENTED/PARTIAL | 31-check smoke passes normal, hardened and sanitized binaries, including shutdown; focused raw security has 27/27 | Target-topology distributed exhaustion and long campaigns remain | Retain final run and add topology load tests | YES for production pilot |
| Persistence/AOF decode fuzzing | GAP | Defensive checks exist in persistence code | No isolated committed harness/corpus evidence identified in this review | Extract pure decoder boundary and fuzz under sanitizers | YES |
| HA network frame fuzzing | GAP | HA bounds/TLS checks exist | No committed HA frame fuzz harness/corpus evidence identified | Fuzz pre-auth and post-auth frame decoder boundaries | YES for HA |
| HA TLS identity rejection | PARTIAL | Node-ID/connection-binding source contract and negative identity test pass | Wrong-CA/expiry/rotation/fault campaign absent | Execute complete certificate lifecycle/fault matrix | YES for HA |
| CI fuzz smoke and overnight commands | PARTIAL | Smoke/coverage targets and long command documented; 855-execution smoke passed locally | No final CI artifact or overnight result | Record exact final duration and findings | YES for final smoke; long campaign risk-based |

## Operability and SRE

| CONTROL / EXPECTATION | CURRENT STATUS | EVIDENCE | GAP | ACTION | BLOCKS PILOT? YES/NO |
| --- | --- | --- | --- | --- | --- |
| Liveness/readiness | IMPLEMENTED | `/health`, `/ready`, and `ayder_persistence_ready`; false readiness maps to 503 | Readiness does not include disk capacity or HA leader/quorum | Define separate traffic checks for write/HA topology | YES if not integrated |
| Uptime/restarts | PARTIAL | `ayder_process_start_time_seconds`; restart-loop example alert | No explicit restart counter/reason; requires retained Prometheus history | Correlate supervisor exits and add reason/restart metric | NO for narrow pilot with supervisor telemetry |
| Request/status rate | IMPLEMENTED | Shared `ayder_http_requests_total` and bounded response class/exact-code counters | No per-route/method labels | Add only bounded low-cardinality route groups if justified | NO |
| Produce/consume/commit/QUERY rate | PARTIAL | Shared `ayder_broker_operations_total{operation}` | Counts operations, not records/bytes/outcomes | Add bounded record/byte/outcome counters | NO for pilot if API logs/evidence suffice |
| HTTP p50/p95/p99 | PARTIAL | Shared bounded histogram measures first request byte through handler completion | No route/method/status labels, client/network time, SLO baseline, or final-run evidence | Validate/tune server-side percentiles and benchmark client-side too | YES for SLO-backed pilot |
| Connections | IMPLEMENTED | Shared `ayder_http_active_connections` | No connection acceptance/rejection/timeout totals | Add if needed for exhaustion diagnosis | NO |
| Append/fsync | PARTIAL | Write outcomes and fsync/sealed duration histograms | No errno/reason; histograms/queue view remain process-local | Aggregate across workers; label bounded failure reasons | YES |
| Queue/backpressure | PARTIAL | Queue gauges and shared `ayder_backpressure_events_total` | Queue gauges process-local | Aggregate or expose stable worker identity | YES for multi-worker SLOs |
| Storage errors/disk | PARTIAL | `ayder_storage_writes_total{outcome="failure"}` | No disk free/inodes, errno, mount, or last-success metric | Deploy host agent; add bounded broker details | YES |
| Broker errors | PARTIAL | Shared `ayder_broker_errors_total` | No route/reason/subsystem labels | Add bounded labels and log correlation | NO for narrow pilot, YES for SLO maturity |
| HA role/leader/quorum | IMPLEMENTED | HA enabled/role/state/leader/fresh-quorum gauges with HELP/TYPE | No leader identity | Add identity carefully or external service discovery | YES for HA |
| Replication lag/health | PARTIAL | Fresh quorum/state plus durable last/commit/applied indices support node convergence checks | No per-peer lag/heartbeat age or leader identity | Export bounded per-peer or worst-case lag/age | YES for HA |
| Recovery state | PARTIAL | Recovery attempts/successes | No in-progress/phase/duration/last completion/failure reason | Add recovery state and duration metrics | YES |
| Prometheus rules | IMPLEMENTED | `deploy/prometheus/ayder-alerts.yml` uses exported series only and 14 rules pass `promtool check rules` | Example thresholds require baselining | Validate and tune on final topology | YES |
| Prometheus exposition quality | PARTIAL | Local working-tree live scrape passed `promtool check metrics` with lint-compatible AOF names/metadata and valid histograms | Final release-binary lint evidence is pending | Re-run live `promtool check metrics` on the immutable release | NO for evaluation; YES before stable metric contract |
| Grafana dashboard | IMPLEMENTED | `deploy/grafana/ayder-dashboard.json` | Import/live query test pending | Import against a live scrape and capture review | NO if equivalent dashboards exist |
| Logging | PARTIAL | stdout/stderr levels and timestamp option; runbook collection guidance | Not structured, complete, or tamper-evident | Define schema, redaction, central retention, audit events | Usually YES |

## Operations and cloud deployment

| CONTROL / EXPECTATION | CURRENT STATUS | EVIDENCE | GAP | ACTION | BLOCKS PILOT? YES/NO |
| --- | --- | --- | --- | --- | --- |
| Install/config/start/stop | PARTIAL | Quickstart/runbook; cluster and single-process SIGTERM live tests now pass and close active connections/listeners | Target systemd/load/VM stop behavior and packaging remain untested | Exercise idle/loaded target VM lifecycle | YES |
| Backup/restore | PARTIAL | Cold backup/restore scripts use manifests, archive/per-file checksums, change detection, safe staging and fsync; regression and real drill pass | No online/point-in-time backup or declared long-term compatibility | Pilot-size restore and define support window | YES |
| Crash recovery | PARTIAL | AOF/sealed replay/readiness plus 100/100 one-worker fsync-acknowledged SIGKILL recovery | No power-loss, multiworker, or HA ledger campaign | Run exact-mode crash/power/failover reconciliation | YES |
| Upgrade/rollback | PARTIAL | Distinct old/fixed binaries passed cold-backup upgrade and restored rollback | No general format matrix, rolling HA upgrade or in-place downgrade | Test every supported pair/topology and publish matrix | YES |
| Capacity/resource planning | GAP/PARTIAL | Benchmark harness and required signals documented | No validated target-hardware results/headroom model | Run fixed methodology on ARM64/amd64 target-like storage | YES |
| Scaling | PARTIAL/RED | Cross-worker publication/apply barrier passes 400/400 and failure/replay regressions | Strict durable benchmark is only 308–382 RPS; metrics partly process-local; no safe sizing envelope | Redesign completion path without weakening correctness and retest | YES |
| Disaster recovery | PARTIAL | Exercise-specific 0-record/5-second SIGKILL target passed at 74 ms; restore timing recorded | No volume-loss, cross-zone/region design or default RPO/RTO | Set pilot objectives and perform target-topology drills | YES |
| Generic Linux VM | PARTIAL | systemd/least-privilege example in runbook | No packaged install, automated validation, or support matrix | Test supported distro/kernel/filesystem matrix | YES |
| AWS EC2 | GAP/PARTIAL | Generic private-VM guidance applies | No AMI/CloudFormation/image pipeline or AWS validation | Build hardened AMI path and validate official checklist | YES for AWS offer |
| Azure VM | GAP/PARTIAL | Generic private-VM guidance applies | No managed image/VHD/ARM template/Azure Agent/certification | Build Azure image path and validate Partner Center requirements | YES for Azure offer |
| linux/arm64 | PARTIAL | Normal/hardened/portable/sanitized builds and normal/sanitize/portable route smoke pass locally; public-preview hosted workflow exists | WSL2 is not native target-cloud evidence; no clean release artifact | Retain green native/cloud runs and release artifact | YES for ARM offer |
| linux/amd64 | PENDING EVIDENCE | Ubuntu x86-64 CI workflow defined | No passing final run is asserted | Run and retain build/smoke/security evidence | YES |
| Repository quality | PARTIAL | README/quickstart/API plus architecture, security, threat, observability, runbook, readiness, and contribution docs | Cross-linking, issue templates, release notes/changelog, ownership metadata remain | Final technical edit and buyer walkthrough | NO for evaluation; YES for mature support |

## Official AWS Marketplace research

Research was rechecked against public AWS documentation on 2026-08-16. AWS can
change policies; re-check immediately before submission.

Primary sources:

- [Registering as a seller](https://docs.aws.amazon.com/marketplace/latest/userguide/seller-account-registering.html)
- [Seller eligibility requirements](https://docs.aws.amazon.com/marketplace/latest/userguide/seller-eligibility.html)
- [AMI-based product requirements](https://docs.aws.amazon.com/marketplace/latest/userguide/product-and-ami-policies.html)
- [AMI product checklist](https://docs.aws.amazon.com/marketplace/latest/userguide/aws-marketplace-listing-checklist.html)

Relevant official requirements include passing security checks without known
high/critical unpatched vulnerabilities or malware; supported/current OS and
software; no hardcoded secrets/private keys/credentials; no password-based SSH
or embedded authorized keys; least-privilege IAM roles rather than requested AWS
credentials; customer administrative access; HVM, EBS-backed x86-64 or ARM64,
region-agnostic image constraints; self-service deployment; and disclosure of
external dependencies. The checklist also calls for production-ready software,
working usage/listing material, and successful AWS validation.

### AWS category A — implemented repository-level prerequisites

These are helpful inputs, not proof of AMI compliance:

- explicit health/readiness and Prometheus interfaces;
- main container non-root execution and required runtime bearer secret;
- private-network/TLS/least-privilege VM guidance;
- native x86-64/public-preview ARM64 build workflow and architecture-aware
  benchmark methodology; and
- security, sanitizer, CodeQL, SBOM, CVE, and secret workflows awaiting runs.

### AWS category B — implementable technical gaps

- produce a supported, patched, HVM/EBS-backed, region-agnostic AMI in `us-east-1`
  for each offered x86-64/ARM64 architecture;
- remove all embedded/demo secrets and private keys; disable password SSH and
  embedded authorized keys; provide customer `sudo` administration;
- pass image malware/vulnerability and AWS Test Add Version checks;
- use instance roles with minimum IAM permissions and no requested static AWS
  credentials;
- make deployment self-service and document every downloaded/external dependency;
- respect the source-AMI restrictions on encrypted snapshots/filesystems, then
  document customer-controlled encrypted data volumes after deployment, private
  subnets, controlled ingress, TLS, monitoring, backup/restore, upgrade, and
  support; and
- prepare accurate metadata, architecture/pricing/version inputs and working URLs.

### AWS category C — organization/legal/commercial requirements

AWS documents an account in good standing, valid organization contact, seller
terms, and—already for free products—a production-ready full-feature product,
defined support process/organization, and regular vulnerability-free updates.
Paid/BYOL eligibility adds eligible residence/business jurisdiction, tax
information, eligible bank account, bank verification, and KYC in applicable
cases. Code cannot complete or attest to these items.

Current status: no AWS seller-account, legal, tax, bank, KYC, pricing, support
organization, or listing evidence was provided to this repository review.

## Official Microsoft Marketplace research

Research was rechecked against public Microsoft Learn documentation on 2026-08-16.
Re-check before submission.

Primary sources:

- [Create a Microsoft Marketplace account in Partner Center](https://learn.microsoft.com/en-us/partner-center/account-settings/create-account)
- [Offer and service listing requirements](https://learn.microsoft.com/en-us/partner-center/marketplace-offers/marketplace-criteria-content-validation)
- [Azure VM plan technical configuration](https://learn.microsoft.com/en-us/partner-center/marketplace-offers/azure-vm-plan-technical-configuration)
- [Review and publish an offer](https://learn.microsoft.com/en-us/partner-center/marketplace-offers/review-publish-offer)
- [Getting paid in Partner Center](https://learn.microsoft.com/en-us/partner-center/marketplace-offers/marketplace-get-paid)

Microsoft documents a Partner Center Marketplace account/enrollment, work
account tied to the organization, legal business details, authority to accept
agreements, and the Microsoft Publisher Agreement. Listing guidance requires
accurate title/description/assets, pricing, working public learn-more links,
support contact, and public legal terms/policies. Azure VM configuration includes
OS/vendor, recommended sizes, explicit public ports, supported image properties,
and declarations for cloud-init, extensions, SSH/locked images, and custom ARM
templates. If extensions are claimed, the documented Linux validation includes
Azure Linux Agent greater than 2.2.41. Publication passes automated validation,
certification (including business/content/technical checks), preview, publisher
sign-off, and publish. Paid offers require accepted agreements plus payout,
bank, and tax setup. The current VM plan page permits x64 Gen 1/Gen 2 and ARM64
Gen 2 image types; that option is not evidence that Ayder has a conforming image.

### Microsoft category A — implemented repository-level prerequisites

These inputs can support, but do not satisfy, an Azure VM submission:

- Linux build/container path and generic VM/systemd guidance;
- liveness/readiness, Prometheus, alerting, and runbook interfaces;
- main non-root container and explicit runtime secret requirement; and
- security/build/SBOM/scanner workflow definitions awaiting final results.

### Microsoft category B — implementable technical gaps

- build and version an Azure-compatible managed image/VHD for each architecture;
- select supported OS/vendor/generation and document tested recommended sizes;
- declare only required ports and default to private/deny-by-default ingress;
- decide and validate cloud-init, Azure Linux Agent/VM extensions, Entra identity,
  NVMe/accelerated networking, backup property, SSH access, and any ARM template;
- remove demo keys/secrets, pass malware/package/functionality/certification tests,
  and provide certification instructions;
- exercise private preview and publisher sign-off with install/upgrade/recovery;
  and
- prepare accurate listing assets, pricing, public learn-more/support/legal URLs.

### Microsoft category C — organization/legal/commercial requirements

Code cannot create the company work account, legal publisher profile, authorized
signatory, Partner Center Marketplace enrollment/verification, Publisher
Agreement acceptance, pricing authority, support organization/contact, public
legal policies, payout account, bank verification, or tax forms.

Current status: none of those organization/commercial artifacts was provided to
this repository review.

## Other enterprise organization/compliance gaps

SOC 2, ISO 27001, external penetration testing, cyber insurance, data-processing
agreements, privacy/legal review, contractual SLA/support staffing, and customer
security questionnaires are not established here. The official sources above
do not make every one a universal marketplace prerequisite, but enterprise
buyers or contracts may require them. Treat them as external gaps, never as
completed controls or certifications.

## Minimal prioritized plan

1. **Complete correctness evidence:** run the full strict 45-run Jepsen campaign,
   longer acknowledged-ledger/idempotency/commit faults, and power/storage-error
   recovery against the fixed candidate.
2. **Make evidence immutable:** run build/CodeQL/SBOM/CVE/secret jobs on a clean
   commit; retain scanner triage and publish the issued checksum/SBOM/provenance
   bundle.
3. **Prove target operations:** repeat backup/restore, distinct-binary rollback,
   shutdown and pilot-specific RPO/RTO drills at target data scale/topology.
4. **Close telemetry gaps:** validate/baseline HTTP latency; add disk/error
   detail, recovery state, per-worker persistence queue/histogram aggregation,
   and HA replication-lag/commit health; validate/tune rules with `promtool` and
   live Grafana.
5. **Validate target platforms:** retain passing ARM64 and amd64 CI evidence;
   run the identical benchmark/crash suite on native target-like storage; create
   hardened AWS/Azure images and run official validation tools.
6. **Complete organization work:** seller accounts/agreements, legal/tax/bank/KYC,
   support/security ownership, public policies, pricing, and buyer-required
   independent assurance.
