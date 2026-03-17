#!/usr/bin/env bash
set -euo pipefail
cd /home/ayderbek/project/ayder

export STRICT_CLAIM=1
export START_CLUSTER=1
export TOKEN=dev
export AYDER_JEPSEN_WORKLOAD=broker-log
export AYDER_JEPSEN_PROFILE=hn-ready
export AYDER_JEPSEN_MODES="mixed partition-only kill-only"

# Avoid inherited shell exports accidentally forcing heavy/all history mode.
unset AYDER_JEPSEN_LINEARIZABLE_HISTORY_MODE
unset AYDER_JEPSEN_MIXED_LINEARIZABLE_HISTORY_MODE

# Keep mixed checker heap high for long runs.
export AYDER_JEPSEN_MIXED_LEIN_JAVA_OPTS='-Xms1024m -Xmx8192m -XX:+UseG1GC -XX:MaxGCPauseMillis=200'

export AYDER_JEPSEN_NEMESIS_STARTUP_SEC=10
export AYDER_JEPSEN_MIXED_NEMESIS_STARTUP_SEC=15
export AYDER_JEPSEN_PRE_READY_TIMEOUT_SEC=300
export AYDER_JEPSEN_PRE_READY_RECOVERY_TIMEOUT_SEC=300
export AYDER_JEPSEN_PRE_CELL_RECOVERY_TIMEOUT_SEC=300
export AYDER_JEPSEN_RUN_WALL_TIMEOUT_SEC=1800
export AYDER_JEPSEN_MIXED_RUN_WALL_TIMEOUT_SEC=2400

# Warn if host sizing is too low for strict mixed runs.
export AYDER_JEPSEN_HOST_MIN_RAM_GIB=20
export AYDER_JEPSEN_HOST_MIN_SWAP_GIB=8

export PRE_MATRIX_RECOVERY_ENABLED=1
export PRE_MATRIX_RECOVERY_WAIT_SEC=240
exec bash ./tests/demo/ha_broker_jepsen_gold.sh
