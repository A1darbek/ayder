#!/usr/bin/env bash
set -euo pipefail
echo "▶️  power-loss / torn-write test"

ROOT="$( cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." &>/dev/null && pwd )"
BIN="$ROOT/ramforge"; [[ -x "$BIN" ]] || { echo "no binary"; exit 1; }

pkill -9 -f '[r]amforge' 2>/dev/null || true
until ! lsof -i:1109 &>/dev/null; do sleep 0.1; done

TMP=$(mktemp -d)
pushd "$TMP" >/dev/null

setsid "$BIN" --aof always --workers 0 2>/dev/null &
PG=$!
sleep 0.4

curl -s -XPOST -d '{"id":7,"name":"smith"}' \
     -H "Content-Type: application/json" \
     http://localhost:1109/users >/dev/null

kill -9 -"$PG"
truncate -s -1 append.aof              # simulate torn write

set +e
"$BIN" --aof always --workers 0
RC=$?
set -e

popd >/dev/null; rm -rf "$TMP"

if [[ $RC -eq 2 ]]; then
  echo "✅ power-loss passed (loader detected corrupt AOF)"
else
  echo "❌ power-loss failed (exit $RC)"
  exit 1
fi