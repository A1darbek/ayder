# Security policy

Ayder is pre-1.0 infrastructure software. This repository does not currently
claim a third-party security audit, penetration test, compliance certification,
or production security SLA.

## Supported versions

| Version | Security fixes |
| --- | --- |
| Current default branch | Best-effort fixes while the project is pre-1.0 |
| Tags and older commits | Not supported unless a release explicitly says otherwise |

There is no long-term-support release line yet. Pin an exact commit for an
evaluation and review every upgrade.

## Report a vulnerability privately

Do not open a public issue containing exploit details, credentials, private
keys, customer data, or an unpatched proof of concept.

Use GitHub's private vulnerability report form for this repository:

<https://github.com/A1darbek/ayder/security/advisories/new>

If that form is unavailable, contact the repository owner through the GitHub
profile and request a private channel without including sensitive details in
the first message. The project does not currently publish a dedicated security
mailbox; creating and monitoring one is an open operational requirement.

Include, when safe:

- the affected commit and build configuration;
- platform, architecture, compiler, and dependency versions;
- impact and prerequisites;
- minimal reproduction steps or a private reproducer;
- whether the issue is already public or being exploited; and
- a safe way to contact you.

Receipt, triage, fix, and disclosure times are not guaranteed. Reporters are
asked to allow a reasonable remediation window and coordinate public disclosure.
Maintainers should acknowledge reports privately, preserve evidence, avoid
sharing reporter information, and publish a security advisory when a fix is
available.

## Deployment security expectations

Before any production-like evaluation:

- set `RF_BEARER_TOKENS` to generated, high-entropy tokens delivered through a
  secret manager. The current container entrypoint refuses to start without an
  explicit value; do not reintroduce or copy a `dev` example token;
- rotate tokens by updating the secret and restarting all workers; there is no
  online token-reload or revocation API;
- terminate client-facing TLS at a trusted reverse proxy or private load
  balancer; Ayder does not currently terminate TLS for client HTTP traffic;
- keep ports private and explicitly restrict ingress. The server binds to
  `0.0.0.0`. `/health` and `/ready` are intentionally unauthenticated;
  `/metrics` and `/metrics_ha` require a valid bearer token by default. Do not
  set `RF_METRICS_PUBLIC=1` unless an independently authenticated private
  scrape boundary makes that exposure explicit. Tokens are not endpoint-scoped,
  so a Prometheus token can also reach other protected routes and must be
  guarded like an application/admin credential;
- leave CORS disabled unless a browser client is required. If required, set
  `RF_CORS_ALLOWED_ORIGIN` to one exact trusted origin; wildcard, comma-separated,
  whitespace/control-containing, and overlong values are rejected at startup;
- keep `RF_ENABLE_TEST_HOOKS` disabled, do not broaden
  `RF_AUTH_SKIP_PREFIXES`, and do not disable rate limiting without a reviewed
  compensating control;
- enable HA mTLS with `RF_HA_TLS=1`, a private CA, unique per-node certificates,
  and strict peer identity validation. Do not set `RF_HA_TLS_STRICT_ID=0` in a
  production-like environment;
- generate new certificates. Private keys previously committed under
  `cluster/certs/` remain in public Git history, and regenerated local fixtures
  are disposable; treat all such keys as compromised and exclude them from
  release artifacts;
- use OS-level encryption for persistent volumes if encryption at rest is
  required; Ayder does not encrypt AOF data itself;
- run the process as a dedicated unprivileged account with a private writable
  data directory and a read-only application directory;
- place metrics behind a private network and TLS proxy; bearer authentication
  alone does not encrypt the token or the metric response; and
- retain logs in a protected external system. Current logs are not a complete,
  tamper-evident audit trail.

See [Production readiness](docs/PRODUCTION_READINESS.md), [Enterprise
readiness](docs/ENTERPRISE_READINESS.md), and [Observability](docs/OBSERVABILITY.md)
for additional known gaps.

## Security maintenance

The repository defines CodeQL, ASan/UBSan, secret scanning, source-SBOM, and
filesystem scanning workflows, with third-party actions pinned to full commit
digests. A provenance workflow also builds checksummed amd64/ARM64 archives and
requests GitHub/Sigstore provenance and SBOM attestations. Definitions alone are
not evidence that a specific commit passed or was attested: use the immutable
GitHub Actions run and verify its bundles for the exact artifact digest.

Clean candidate `2e334d1` passed native ARM64/amd64 build/security, CodeQL,
Trivy, Gitleaks and SPDX gates. Both checksummed package archives have verified
SLSA provenance and SPDX SBOM Sigstore bundles. Exact immutable runs, archive
digests and offline verification commands are in
[`CLEAN_COMMIT_2e334d1.md`](docs/evidence/supply-chain/CLEAN_COMMIT_2e334d1.md).
This is exact-candidate evidence, not a third-party audit or release policy.

For local evidence generation, [`scripts/supply-chain/generate_local_evidence.sh`](scripts/supply-chain/generate_local_evidence.sh)
uses an installed Syft CLI to create an SPDX JSON source-tree SBOM and records
SHA-256 checksums plus dirty-tree/tool metadata. That output is unsigned and is
not a runtime dependency inventory, attestation, or proof of a clean release.

Security fixes should include a regression test where disclosure risk permits,
run the normal and sanitizer suites, document residual risk, and avoid weakening
durability or HA semantics to remove a symptom.

On the 2026-08-15 working tree, the live HTTP security suite passed 25/25 cases
against both normal and GCC ASan/UBSan builds, covering metrics authentication,
strict bearer handling, CORS defaults/allowlisting, disabled test hooks,
connection/time limits, and cooperative SIGTERM. Leak detection was disabled
for that forked live sanitizer suite, so it is not leak-test evidence. Repeat
all security gates on the immutable release candidate and retain the run.
