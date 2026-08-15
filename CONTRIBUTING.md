# Contributing to Ayder

Ayder is a C broker with persistence and HA paths where small changes can alter
acknowledgement, durability, or recovery semantics. Keep pull requests narrow,
state the invariant being protected, and attach reproducible evidence.

## Local setup

On Debian or Ubuntu, install the current build dependencies:

```bash
sudo apt-get update
sudo apt-get install build-essential curl jq libcurl4-openssl-dev \
  libevent-dev libssl-dev liburing-dev libuv1-dev pkg-config zlib1g-dev
```

Build and run the route-complete smoke test:

```bash
make -j"$(nproc)"
START_SERVER=1 SERVER_BIN=./ayder PORT=1109 ROUTE_COVERAGE=1 \
  tests/smoke/api_smoke.sh
```

The smoke harness uses a temporary working directory when it starts the server,
so it does not write test AOF data into the repository. See
[`tests/smoke/README.md`](tests/smoke/README.md) for focused options.

The normal Makefile uses `-march=native`; always report `uname -m`, compiler
version, and the exact commit with build or benchmark results. Do not assume an
x86-64 host. CI defines native x86-64 and ARM64 builds; the
[GitHub-hosted ARM64 runner](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)
is in public preview, and a workflow definition is not a passing result.

## Before opening a pull request

- Run the build and smoke command above.
- If C code changed, run the ASan/UBSan workflow or reproduce its compile and
  test commands from [`.github/workflows/sanitizers.yml`](.github/workflows/sanitizers.yml).
- For HTTP/authentication/CORS/timeout/shutdown changes, run
  `python3 tests/security/http_live_security_test.py --server ./ayder --label local`
  and repeat with the sanitizer binary using the harness's `--sanitized` mode.
- Run focused persistence, broker, HA, or security regressions for the area
  changed. Do not claim a Jepsen or fuzz campaign unless you record its exact
  command, duration, commit, environment, and result.
- Keep formatting consistent with [`.clang-format`](.clang-format) for C/C++
  changes; avoid unrelated mechanical rewrites.
- Update documentation when a configuration, metric, API, failure mode, or
  durability contract changes.
- Check `git diff --check` and review the final diff for generated artifacts,
  secrets, keys, test data, and accidental benchmark output.
- When release/supply-chain inputs change and Syft is installed, run
  `bash scripts/supply-chain/generate_local_evidence.sh ./ayder /tmp/ayder-evidence`
  and retain the output outside the repository. It records dirty-tree state;
  do not present a dirty result as release evidence.

For persistence, acknowledgements, replication, or recovery changes, explain:

1. the old and new contract;
2. when a client receives success;
3. what survives process, host, and leader failure;
4. the tests that demonstrate the claim; and
5. any migration or rollback constraint.

Benchmark changes must preserve the same payload, topology, connection model,
authentication, rate limiting, durability, batching, and fsync semantics for
before/after comparisons. WSL2 results must be labeled as development/regression
evidence rather than universal production performance.

## Pull request shape

Use a descriptive title and include:

- problem and scope;
- design and alternatives considered;
- risk and rollback plan;
- commands run with their results;
- performance evidence when performance is affected; and
- known follow-up work.

Do not add credentials or production certificates. Demo credentials must be
obviously non-production, but private-key fixtures are discouraged because they
are routinely copied into real deployments.

## Issues and security reports

Public issues are appropriate for non-sensitive bugs, documentation gaps, and
reproducible feature requests. Search for an existing issue and provide the
commit, environment, logs with secrets removed, and a minimal reproducer.

Report suspected vulnerabilities using the private process in
[`SECURITY.md`](SECURITY.md), not a public issue.
