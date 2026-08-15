# Local HA certificates

No certificate or private key in this directory is distributed by the
repository. Generate ephemeral local fixtures from the repository root:

```bash
./cluster/generate_certs.sh 5
```

The generator creates a local development CA and unique node certificates. The
resulting `*.key`, `*.crt`, and serial files are ignored by Git and must never be
reused for a production trust domain.

For a pilot, provision the CA and node identities from the organization's
secret-management/PKI process, mount them read-only, restrict key permissions,
and define a rotation and revocation procedure before exposure.
