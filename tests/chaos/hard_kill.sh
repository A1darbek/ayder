#!/usr/bin/env bash
set -euo pipefail
echo "▶️  hard-kill test"

# ─────────────────────────────────────────────────────────────
# 1) Locate built binary (quote-safe even with spaces in path)
ROOT="$( cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." &>/dev/null && pwd )"
BIN="$ROOT/ramforge"
[[ -x "$BIN" ]] || { echo "❌ ramforge binary not found"; exit 1; }

# ─────────────────────────────────────────────────────────────
# 2) Ensure the port is free and no stray ramforge processes
pkill -9 -f '[r]amforge' 2>/dev/null || true
until ! lsof -i:1109 &>/dev/null; do sleep 0.1; done

rm -f append.aof dump.rdb      # clean start

# ─────────────────────────────────────────────────────────────
# 3) Start server (single process, own process-group)
setsid "$BIN" --aof always --workers 0 2>/dev/null &
PGID=$!                         # pg leader == pgid
sleep 0.4                       # brief warm-up

# 4) Create one record
curl -s -XPOST -d '{"id":1,"name":"neo"}' \
     -H "Content-Type: application/json" \
     http://localhost:1109/users >/dev/null

# 5) Brutal power-cut: kill entire PG
kill -9 -"$PGID"
sleep 0.3                       # allow OS to release port

# ─────────────────────────────────────────────────────────────
# 6) Reboot server
setsid "$BIN" --aof always --workers 0 2>/dev/null &
PGID2=$!
sleep 0.4

BODY=$(curl -s http://localhost:1109/users/1)

# 7) Shutdown cleanly
kill -9 -"$PGID2" 2>/dev/null || true
pkill -9 -f '[r]amforge' 2>/dev/null || true

# ─────────────────────────────────────────────────────────────
# 8) Verdict
if [[ "$BODY" == *'"neo"'* ]]; then
  echo "✅ hard-kill passed"
else
  echo "❌ hard-kill failed"
  exit 1
fi