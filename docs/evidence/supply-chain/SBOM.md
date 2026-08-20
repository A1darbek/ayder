# SBOM evidence status

On 2026-08-16, Syft 1.51.0 (`linux/arm64`, release checksum verified) generated
SPDX JSON catalogs for this working tree and the measured ARM64 binary.
Validation confirmed SPDX version strings, 143 source-tree package entries,
and one binary-artifact package entry.

The generated files are intentionally retained in ignored local evidence
because the tree is dirty and is not a release candidate:

```text
bench/results/rrv1-supply-chain-20260816/ayder-source.spdx.json
bench/results/rrv1-supply-chain-20260816/ayder-arm64-binary.spdx.json
```

The measured server binary SHA-256 was
`de14a5e1f46785785434d059dde9c4fc3b1b152eeafe36dd101151103c298008`.
The evidence is checksummed but unsigned and has no local provenance
attestation. Scanner details and exceptions are in
[SCANNER_TRIAGE.md](SCANNER_TRIAGE.md).

Reproduce the local evidence after building a candidate:

```bash
syft version
bash scripts/supply-chain/generate_local_evidence.sh ./ayder /tmp/ayder-evidence
```

For a clean commit, the
[`Supply-chain checks`](../../../.github/workflows/supply-chain.yml) workflow
generates and uploads an SPDX artifact and checksum. The
[`Release provenance`](../../../.github/workflows/provenance.yml) workflow
builds ARM64 and amd64 artifacts and requests GitHub OIDC-backed provenance and
SBOM attestations. On 2026-08-20 those workflows passed for clean commit
`2e334d1`; source SPDX and checksums were retained, and both native packages had
verified SLSA provenance and SPDX SBOM attestations. See
[clean-commit evidence](CLEAN_COMMIT_2e334d1.md). The earlier dirty-tree local
catalogs remain unsigned and must not be substituted for those exact artifacts.
