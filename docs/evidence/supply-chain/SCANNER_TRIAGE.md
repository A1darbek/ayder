# Scanner triage — 2026-08-16

This is an internal dirty-tree scan, not a clean-release attestation. Scanner
release archives were downloaded from their official GitHub releases and
matched against the published SHA-256 checksum files before use.

## Tools and scope

| Tool | Version | Scope |
|---|---:|---|
| Trivy | 0.74.0, `linux/arm64` | Filesystem vulnerabilities and Dockerfile misconfiguration |
| Gitleaks | 8.30.1, `linux/arm64` | All 131 Git commits |
| Syft | 1.51.0, `linux/arm64` | Source tree and measured ARM64 binary SPDX JSON |

The measured binary SHA-256 is
`de14a5e1f46785785434d059dde9c4fc3b1b152eeafe36dd101151103c298008`.
The source catalog contains 143 packages and the binary catalog contains one
artifact package. Both SPDX documents passed structural validation; local
checksums were generated and rechecked.

## Gitleaks

The initial complete-history scan found eight `private-key` findings, all in
the development HA certificate fixtures introduced by commit `38dcd556`:
`cluster/certs/ca.key` and `node1.key` through `node7.key`. No other rule or
path was reported.

Those keys are public and must be treated as compromised. The current change
removes them from Git, ignores generated certificate material, documents that
pilots must use their own PKI, and retains only a local certificate generator.
Rewriting public Git history is explicitly outside this sprint. Therefore
`.gitleaksignore` contains eight commit/path/rule/line fingerprints, not a path
or rule allowlist. A newly committed key at any of those paths will still fail.
The complete-history scan after this triage reports zero unsuppressed leaks.

## Trivy

The initial scan reported 34 fixed vulnerabilities, all against the benchmark
client's stale `aiohttp==3.9.5` dependency. Two were HIGH; none were CRITICAL.
It also reported three HIGH Dockerfile findings because the musl runtime and
two benchmark-client images ran as root.

The benchmark dependency is now `aiohttp==3.14.3`, and all three images use a
numeric unprivileged user. A repeated scan reports:

- zero known fixed vulnerabilities at any severity;
- zero HIGH or CRITICAL misconfigurations;
- one MEDIUM style finding for `RUN cd` in the main multi-stage Docker build;
- two LOW missing-healthcheck findings for short-lived benchmark client
  images, where a service healthcheck is not meaningful.

The main glibc and musl server images have readiness healthchecks. The two
remaining LOW benchmark-client findings are accepted as non-runtime tooling
image behavior; they do not justify a broad scanner ignore.

## Evidence and release boundary

Raw redacted JSON and checksums are retained locally under
`bench/results/rrv1-supply-chain-20260816/`. Because HEAD does not describe the
dirty working tree, these results do not satisfy the clean-candidate release
gate. The pinned CI workflows must rerun Gitleaks, Trivy, CodeQL, SBOM, build,
sanitizers, and tests on the final clean commit. Release provenance and signed
Sigstore bundles are produced only by the GitHub OIDC workflow; this local run
has neither an identity-bearing signature nor provenance.
