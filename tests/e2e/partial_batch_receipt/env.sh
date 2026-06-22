#!/usr/bin/env bash
# Shared environment for the partial-batch recovery-receipt demo.
#
# It reuses the exactly-once harness's lib.sh (curl/pg helpers, docker compose
# wrappers) but scopes everything to this demo's own topic/group so the two
# harnesses never touch each other's offsets or business effects.
set -euo pipefail

PBR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EOS_DIR="$(cd "${PBR_DIR}/../exactly_once" && pwd)"

# Scope BEFORE sourcing lib.sh so its ${TOPIC:-...} defaults pick these up.
export TOPIC="${TOPIC:-pbr_demo}"
export GROUP="${GROUP:-pbr_g1}"
export PARTITION="${PARTITION:-0}"

# Reuse the same Postgres compose stack and helpers as the exactly-once harness.
export COMPOSE_FILE="${COMPOSE_FILE:-${EOS_DIR}/docker-compose.pg.yml}"

# shellcheck source=tests/e2e/exactly_once/lib.sh
source "${EOS_DIR}/lib.sh"

# lib.sh defines its own SCRIPT_DIR for the exactly-once harness. Restore the
# partial-batch directory so scripts that source this file keep resolving their
# local schema/producer/consumer files.
SCRIPT_DIR="${PBR_DIR}"
