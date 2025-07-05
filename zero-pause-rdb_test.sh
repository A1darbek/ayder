#!/usr/bin/env bash
set -euo pipefail

echo "🔍  Waiting for /metrics (max 30 s)…"
for i in {1..30}; do
  curl -fsS http://127.0.0.1:1109/metrics | \
    grep -q ramforge_latency_microseconds && break
  sleep 1
  [[ $i == 30 ]] && { echo "💥 RamForge never became ready"; exit 1; }
done
echo "✓ RamForge is up"

echo "🔥  Running pressure-cooker…"
scripts/pressure_cooker-zero-pause.sh

curl -s http://127.0.0.1:1109/metrics > metrics.prom

# zero-pause snapshots counter (must be ≥ 4)
zp=$(grep '^zero_pause_snapshots_total ' metrics.prom | awk '{print $2}')

echo "📸 zero-pause snapshots = $zp"

([ -n "$zp" ] && [ "${zp%.*}" -ge 4 ]) \
  || { echo "❌ too few ZP snapshots"; exit 1; }

echo "✅ PASS – zero-pause RDB survives the pressure-cooker"
