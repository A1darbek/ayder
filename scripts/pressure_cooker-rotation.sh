#!/usr/bin/env bash
set -euo pipefail
CHAOS_BIN=tests/live_rotation
DURATION_SEC=110
FIO_RUNTIME=$(( DURATION_SEC + 10 ))
[[ -x "$CHAOS_BIN" ]] || { echo "❌ $CHAOS_BIN missing"; exit 1; }
# ---------- optional SSD burn ----------
if command -v fio >/dev/null 2>&1; then
  echo "🏃  fio burn (${FIO_RUNTIME}s)…"
  fio --name burn --rw=write --bs=4k --direct=1 \
        --ioengine=libaio --iodepth=64            \
        --size=256M --time_based --runtime=${FIO_RUNTIME} \
        --output=/tmp/fio.log &
  FIO_PID=$!
else
  FIO_PID=""
fi
# ---------- chaos hammer ---------------
echo "🔥  ${CHAOS_BIN} …"
"${CHAOS_BIN}"
# ensure fio is gone even if chaos exits early
[[ -n "${FIO_PID:-}" ]] && kill "$FIO_PID" >/dev/null 2>&1 || true
