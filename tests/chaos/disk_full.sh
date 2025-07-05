#!/usr/bin/env bash
set -euo pipefail
echo "▶️  disk-full test"

ROOT="$( cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." &>/dev/null && pwd )"
BIN="$ROOT/ramforge"; [[ -x "$BIN" ]] || { echo "no bin"; exit 1; }

# ─── kill any stray servers and wait for port 1109 to be free ───
pkill -9 -f '[r]amforge' 2>/dev/null || true
until ! lsof -i:1109 &>/dev/null; do sleep 0.2; done

# ─── 64-MB ext4 FS with zero root reserve ───
WORK=$(mktemp -d)
truncate -s 64M "$WORK/vol.img"
mkfs.ext4 -F -m 0 "$WORK/vol.img" > /dev/null
mkdir "$WORK/mnt"
sudo mount -o loop "$WORK/vol.img" "$WORK/mnt"

pushd "$WORK/mnt" >/dev/null
sudo mkdir data
sudo chown "$(id -u):$(id -g)" data
cd data

# ─── start server (unprivileged) in its own PG ───
setsid "$BIN" --aof always --workers 0 2>/dev/null &
PG=$!          # pg leader
sleep 0.2

# wait until /health responds or 5 s timeout
for _ in {1..50}; do
  curl -sf http://localhost:1109/health >/dev/null && break || sleep 0.1
done

# ─── fill disk completely ───
while dd if=/dev/zero of=filler.bin bs=1M conv=notrunc oflag=append 2>/dev/null; do
  :
done

STATUS=$(curl -s -o /dev/null -w '%{http_code}' \
         -XPOST -d '{"id":99,"name":"diskfull"}' \
         -H "Content-Type: application/json" \
         http://localhost:1109/users || true)

# ─── cleanup ───
kill -9 -"$PG" 2>/dev/null || true
popd >/dev/null
sudo umount "$WORK/mnt"
rm -rf "$WORK"

if [[ "$STATUS" == "503" || "$STATUS" == "000" ]]; then
  echo "✅ disk-full passed"
  exit 0
else
  echo "❌ disk-full failed (got $STATUS)"
  exit 1
fi