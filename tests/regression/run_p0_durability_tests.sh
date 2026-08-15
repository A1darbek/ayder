#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BUILD_DIR="$(mktemp -d)"
trap 'rm -rf -- "${BUILD_DIR}"' EXIT

python3 "${SCRIPT_DIR}/p0_durability_contract_test.py"

cc -std=gnu11 -O2 -Wall -Wextra -Werror \
  -I"${REPO_ROOT}/src" \
  "${SCRIPT_DIR}/ha_protocol_test.c" \
  "${REPO_ROOT}/src/ramforge_ha_protocol.c" \
  -o "${BUILD_DIR}/ha-protocol-test"
"${BUILD_DIR}/ha-protocol-test"

cc -std=gnu11 -O2 -Wall -Wextra -Werror \
  -I"${REPO_ROOT}/src" \
  "${SCRIPT_DIR}/ha_fault_injection_test.c" \
  "${REPO_ROOT}/src/ramforge_ha_fault_injection.c" \
  -o "${BUILD_DIR}/ha-fault-injection-test"
"${BUILD_DIR}/ha-fault-injection-test"

cc -std=gnu11 -O2 -Wall -Wextra -Werror \
  -I"${REPO_ROOT}/src" \
  "${SCRIPT_DIR}/ha_store_recovery_test.c" \
  "${REPO_ROOT}/src/ramforge_ha_store.c" \
  "${REPO_ROOT}/src/crc32c.c" \
  -pthread -o "${BUILD_DIR}/ha-store-recovery-test"
"${BUILD_DIR}/ha-store-recovery-test"

cc -std=gnu11 -O2 -ffunction-sections -fdata-sections \
  -I"${REPO_ROOT}/src" \
  "${SCRIPT_DIR}/aof_always_enable_test.c" \
  -Wl,--gc-sections -o "${BUILD_DIR}/aof-always-enable-test"
"${BUILD_DIR}/aof-always-enable-test"

cc -std=gnu11 -O2 -ffunction-sections -fdata-sections \
  -I"${REPO_ROOT}/src" -I"${REPO_ROOT}/deps/picohttpparser" \
  "${SCRIPT_DIR}/sealed_writev_partial_test.c" \
  -Wl,--gc-sections -o "${BUILD_DIR}/sealed-writev-test" \
  -luring -luv -lz -lm -lcurl -lssl -lcrypto
"${BUILD_DIR}/sealed-writev-test"

cc -std=gnu11 -O2 -ffunction-sections -fdata-sections \
  -I"${REPO_ROOT}/src" -I"${REPO_ROOT}/deps/picohttpparser" \
  "${SCRIPT_DIR}/aof_rewrite_partial_write_test.c" \
  -Wl,--gc-sections -o "${BUILD_DIR}/aof-rewrite-write-test" \
  -luring -luv -lz -lm -lcurl -lssl -lcrypto
"${BUILD_DIR}/aof-rewrite-write-test"

cc -std=gnu11 -O2 -ffunction-sections -fdata-sections \
  -I"${REPO_ROOT}/src" -I"${REPO_ROOT}/deps/picohttpparser" \
  "${SCRIPT_DIR}/sealed_replay_recovery_test.c" \
  "${REPO_ROOT}/src/crc32c.c" \
  -Wl,--gc-sections -o "${BUILD_DIR}/sealed-replay-recovery-test" \
  -luring -luv -lz -lm -lcurl -lssl -lcrypto
"${BUILD_DIR}/sealed-replay-recovery-test"

cc -std=gnu11 -O2 -ffunction-sections -fdata-sections \
  -I"${REPO_ROOT}/src" \
  "${SCRIPT_DIR}/persistence_replay_failure_test.c" \
  -Wl,--gc-sections -o "${BUILD_DIR}/persistence-replay-failure-test"
"${BUILD_DIR}/persistence-replay-failure-test"

cc -std=gnu11 -O2 -ffunction-sections -fdata-sections \
  -I"${REPO_ROOT}/src" -I"${REPO_ROOT}/deps/picohttpparser" \
  "${SCRIPT_DIR}/sealed_shared_failure_gc_test.c" \
  "${REPO_ROOT}/src/crc32c.c" \
  -Wl,--gc-sections -o "${BUILD_DIR}/sealed-shared-failure-gc-test" \
  -luring -luv -lz -lm -lcurl -lssl -lcrypto
"${BUILD_DIR}/sealed-shared-failure-gc-test"

cc -std=gnu11 -O2 -ffunction-sections -fdata-sections \
  -I"${REPO_ROOT}/src" -I"${REPO_ROOT}/deps/picohttpparser" \
  "${SCRIPT_DIR}/sync_timeout_contract_test.c" \
  -Wl,--gc-sections -o "${BUILD_DIR}/sync-timeout-contract-test" \
  -luring -luv -lz -lm -lcurl -lssl -lcrypto
"${BUILD_DIR}/sync-timeout-contract-test"

cc -std=gnu11 -O2 -ffunction-sections -fdata-sections \
  -I"${REPO_ROOT}/src" \
  "${SCRIPT_DIR}/shared_storage_atomic_max_test.c" \
  "${REPO_ROOT}/src/shared_storage.c" \
  -Wl,--gc-sections -pthread -o "${BUILD_DIR}/shared-storage-atomic-max-test"
"${BUILD_DIR}/shared-storage-atomic-max-test"

cc -std=gnu11 -O2 -ffunction-sections -fdata-sections \
  -I"${REPO_ROOT}/src" -I"${REPO_ROOT}/deps/picohttpparser" \
  "${SCRIPT_DIR}/http_write_gate_test.c" \
  -Wl,--gc-sections -o "${BUILD_DIR}/http-write-gate-test" \
  -luring -luv -lz -lm -lcurl -lssl -lcrypto
"${BUILD_DIR}/http-write-gate-test"

cc -std=gnu11 -O2 -ffunction-sections -fdata-sections \
  -I"${REPO_ROOT}/src" \
  "${REPO_ROOT}/tests/security/ha_tls_node_identity_test.c" \
  -Wl,--gc-sections -o "${BUILD_DIR}/ha-tls-node-identity-test" \
  -lssl -lcrypto
"${BUILD_DIR}/ha-tls-node-identity-test"
