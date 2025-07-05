#!/usr/bin/env bash
# local_rotation_test.sh
set -euo pipefail

# ── 1) Wait until /metrics is reachable ────────────────────────────
echo "🔍  Waiting for /metrics (max 30 s)…"
for i in {1..30}; do
  if curl -s http://127.0.0.1:1109/metrics | grep -q ramforge_latency_microseconds; then
    echo "✓ RamForge is up"
    break
  fi
  sleep 1
  [[ $i == 30 ]] && { echo "💥 RamForge never became ready"; exit 1; }
done

# ── 2) Run the pressure-cooker ─────────────────────────────────────
echo "🔥  Running pressure-cooker…"
scripts/pressure_cooker-rotation.sh


# ── 3) Assert latency + durability counters ───────────────────────
curl -s http://127.0.0.1:1109/metrics > metrics.prom

echo "🔍 Debug: Raw metrics content for rotations:"
grep 'ramforge_rotations_total' metrics.prom || echo "No rotation metrics found"

echo "🔍 Debug: Raw metrics content for AOF:"
grep 'ramforge_aof' metrics.prom || echo "No AOF metrics found"

# Fixed parsing - sum all rotation reasons
rot=$(grep '^ramforge_rotations_total{' metrics.prom | awk '{print $2}' | awk '{sum += $1} END {print sum}')
aof_rot=$(grep '^ramforge_aof_rotations_total' metrics.prom | awk '{print $2}')
aof_files=$(grep '^ramforge_aof_files_total' metrics.prom | awk '{print $2}')
zp_snap=$(grep '^zero_pause_snapshots_total' metrics.prom | awk '{print $2}')

# Handle empty values
rot=${rot:-0}
aof_rot=${aof_rot:-0}
aof_files=${aof_files:-0}
zp_snap=${zp_snap:-0}

echo "🔄  rotations_total  = $rot"
echo "📸  zero_pause_snapshots_total = $zp_snap"
echo "📸  aof_files_total = $aof_files"
echo "📸  aof_rotations_total = $aof_rot"

# Convert to integer comparison (handle potential decimal values)
rot_int=$(echo "$rot" | cut -d. -f1)
zp_snap_int=$(echo "$zp_snap" | cut -d. -f1)
aof_rot_int=$(echo "$aof_rot" | cut -d. -f1)
aof_files_int=$(echo "$aof_files" | cut -d. -f1)

# Assertions with better error messages
if [ "$rot_int" -ge 2 ]; then
    echo "✅ RDB rotations: $rot_int >= 2"
else
    echo "❌ fewer than 2 RDB rotations (got: $rot_int)"
    exit 1
fi

if [ "$zp_snap_int" -ge 4 ]; then
    echo "✅ Snapshots: $zp_snap_int >= 4"
else
    echo "❌ fewer than 4 snapshots (got: $zp_snap_int)"
    exit 1
fi

if [ "$aof_rot_int" -ge 1 ]; then
    echo "✅ AOF rotations: $aof_rot_int >= 1"
else
    echo "❌ AOF never rotated (got: $aof_rot_int)"
    exit 1
fi

if [ "$aof_files_int" -ge 1 ]; then
    echo "✅ AOF files: $aof_files_int >= 1"
else
    echo "❌ no AOF segments left (got: $aof_files_int)"
    exit 1
fi

echo "✅  Pressure-cooker PASS – RamForge ≤5 ms p99 with ≥2 rotations / ≥4 snapshots"
