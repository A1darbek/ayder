# Clean-commit CI and provenance evidence

Evidence date: 2026-08-20. Candidate commit:
`2e334d1f20e446024294c6494e2478275f5f4038` on
`hardening/revenue-readiness-v1`.

All required push workflows completed successfully:

| Gate | Result | Immutable run |
|---|---|---|
| Native amd64/arm64 build, regressions and API smoke | PASS | [Build and smoke 32395448055](https://github.com/A1darbek/ayder/actions/runs/32395448055) |
| Clang ASan/UBSan, bounded JSON and live HTTP smoke | PASS | [Sanitizers 32395448052](https://github.com/A1darbek/ayder/actions/runs/32395448052) |
| CodeQL C/C++ | PASS | [CodeQL 32395447996](https://github.com/A1darbek/ayder/actions/runs/32395447996) |
| Trivy, complete-history Gitleaks and source SPDX | PASS | [Supply chain 32395448058](https://github.com/A1darbek/ayder/actions/runs/32395448058) |
| Native amd64/arm64 packages, checksums and attestations | PASS | [Release provenance 32395447990](https://github.com/A1darbek/ayder/actions/runs/32395447990) |

The Trivy HIGH/CRITICAL gate reported zero vulnerabilities and zero
misconfigurations for every scanned target. Gitleaks reported no leaks after
the exact historical fixture-key fingerprints documented in
[SCANNER_TRIAGE.md](SCANNER_TRIAGE.md). The source SPDX artifact and checksum
were uploaded by the supply-chain run.

## Package digests

```text
b60b9140e33e5a78cc00111f3c98a5eaa3f92a69135f74956d3c5a54cf36cafe  ayder-linux-amd64-2e334d1f20e446024294c6494e2478275f5f4038.tar.gz
435f32cb81b0d658cf26aa43f97f86a8a1d623a64cf1f1953b8e5502d3ac1597  ayder-linux-arm64-2e334d1f20e446024294c6494e2478275f5f4038.tar.gz
```

Both downloaded packages passed `sha256sum -c`. Each artifact contains its
archive, `SHA256SUMS`, binary SPDX JSON, SLSA provenance Sigstore bundle and
SPDX SBOM Sigstore bundle.

Offline `gh attestation verify` against the downloaded bundles verified one
`https://slsa.dev/provenance/v1` statement and one
`https://spdx.dev/Document/v2.3` statement per architecture. Verification
enforced repository `A1darbek/ayder`, signer workflow
`.github/workflows/provenance.yml`, and source digest
`2e334d1f20e446024294c6494e2478275f5f4038`.

Example verification after downloading the run artifacts:

```bash
gh attestation verify "$archive" \
  --bundle "$artifact_dir/provenance.sigstore.json" \
  --repo A1darbek/ayder \
  --signer-workflow A1darbek/ayder/.github/workflows/provenance.yml \
  --source-digest 2e334d1f20e446024294c6494e2478275f5f4038

gh attestation verify "$archive" \
  --bundle "$artifact_dir/sbom.sigstore.json" \
  --repo A1darbek/ayder \
  --signer-workflow A1darbek/ayder/.github/workflows/provenance.yml \
  --source-digest 2e334d1f20e446024294c6494e2478275f5f4038 \
  --predicate-type https://spdx.dev/Document/v2.3
```

This proves the stated CI and supply-chain properties for this exact candidate
and the downloaded artifact digests. It is not a signed release tag, an image
or runtime-package vulnerability inventory, production approval, or evidence
for later commits.
