#!/usr/bin/env bash
set -euo pipefail

CHAOS_BIN=tests/chaos_latency_test     # zero-pause hammer
DURATION_SEC=120
FIO_RUNTIME=$(( DURATION_SEC + 10 ))

[[ -x "$CHAOS_BIN" ]] || { echo "❌ $CHAOS_BIN missing"; exit 1; }

# 1️⃣  optional SSD burn
if command -v fio >/dev/null 2>&1; then
  echo "🏃  fio burn (${FIO_RUNTIME}s)…"
  fio --name burn --rw=write --bs=4k --direct=1 \
      --ioengine=libaio --iodepth=64            \
      --size=1G --time_based --runtime=${FIO_RUNTIME} \
      --eta=never --output=/tmp/fio.log &
  FIO_PID=$!
else
  FIO_PID=""
fi

# 2️⃣  chaos hammer
echo "🔥  ${CHAOS_BIN} …"
"${CHAOS_BIN}"  --duration ${DURATION_SEC} \
                --writers 8 --readers 4 \
                --snapshot-interval 10

# 3️⃣  stop fio
[[ -n "${FIO_PID:-}" ]] && kill "$FIO_PID" >/dev/null 2>&1 || true
