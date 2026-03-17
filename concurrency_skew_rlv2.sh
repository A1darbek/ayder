#!/usr/bin/env bash
set -euo pipefail

# --- config
TOKEN_CAS="${TOKEN_CAS:-nocap}"
WORKERS="${WORKERS:-32}"
ATTEMPTS="${ATTEMPTS:-50}"
NS="${NS:-enterprise}"
KEY="${NS}/race/customer/CUST-RACE-1"
BASE="${BASE:-http://127.0.0.1:1109}"

say(){ printf "\n\033[1;36m%s\033[0m\n" "$*"; }
hdr_cas=(-H "Authorization: Bearer ${TOKEN_CAS}" -H 'Content-Type: application/json')

say "1) CAS race (${WORKERS} workers x ${ATTEMPTS})"

# Seed a document (idempotent)
curl -sS -X POST "${BASE}/kv/${KEY}?wait_ms=5" "${hdr_cas[@]}" \
  --data-binary '{"v":1,"note":"seed"}' >/dev/null

# Snapshot a single CAS everyone will race with
CAS0=$(curl -sS "${BASE}/kv/${KEY}/meta" -H "Authorization: Bearer ${TOKEN_CAS}" | jq -r '.cas')
[ -n "$CAS0" ] || { echo "failed to read CAS0"; exit 1; }

out="$(mktemp)"; trap 'rm -f "$out"' EXIT

# All workers try to write with the *same* CAS0
for w in $(seq 1 "$WORKERS"); do
  (
    for i in $(seq 1 "$ATTEMPTS"); do
      body=$(jq -nc --arg w "$w" --arg i "$i" '{note:"race",w:($w|tonumber),i:($i|tonumber)}')
      curl -sS -m 3 --connect-timeout 1 -H 'Connection: close' \
        -X POST "${BASE}/kv/${KEY}?cas=${CAS0}&wait_ms=5" "${hdr_cas[@]}" \
        --data-binary "$body" \
      | jq -r 'if .ok==true then "ok" else (.error // "other") end'
    done
  ) >>"$out" &
done
wait

okc=$(grep -c '^ok$' "$out" || true)
cmc=$(grep -c '^cas_mismatch$' "$out" || true)
rll=$(grep -c '^rate_limited$' "$out" || true)
oth=$(grep -cvE '^(ok|cas_mismatch|rate_limited)$' "$out" || true)

printf "  • ok: %s, cas_mismatch: %s, 429(rate_limited): %s, other: %s\n" "$okc" "$cmc" "$rll" "$oth"


TOKEN_RL="${TOKEN_RL:-dev}"
hdr_rl=(-H "Authorization: Bearer ${TOKEN_RL}" -H 'Content-Type: application/json')

say "2) Rate limit check (token ${TOKEN_RL})"
RL_KEY_PREFIX="${NS}/rl/test"
RL_N="${RL_N:-60}"
tmp="$(mktemp)"; trap 'rm -f "$tmp"' RETURN

# Give bucket time to refill
sleep 1

# Phase A: a small burst equals burst capacity (10) → expect ~10×200
for i in $(seq 1 10); do
  ( curl -sS -m 3 --connect-timeout 1 -o /dev/null -w "%{http_code}\n" \
      -X POST "${BASE}/kv/${RL_KEY_PREFIX}-A-$i" "${hdr_rl[@]}" \
      --data-binary '{}' ) >>"$tmp" &
done; wait

# Phase B: immediate flood → mostly 429
for i in $(seq 1 "$RL_N"); do
  ( curl -sS -m 3 --connect-timeout 1 -o /dev/null -w "%{http_code}\n" \
      -X POST "${BASE}/kv/${RL_KEY_PREFIX}-B-$i" "${hdr_rl[@]}" \
      --data-binary '{}' ) >>"$tmp" &
done; wait

c200=$(grep -c '^200$' "$tmp" || true)
c429=$(grep -c '^429$' "$tmp" || true)
printf "  • HTTP 200: %s, 429: %s\n" "$c200" "$c429"

