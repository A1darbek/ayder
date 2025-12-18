#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-1109}"
HOST="${HOST:-127.0.0.1}"
BASE="http://${HOST}:${PORT}"
TOKEN="${TOKEN:-dev}"
NS="${NS:-enterprise}"

# concurrency + attempts per worker (customize with: C=32 NITER=50 ./script.sh)
C="${C:-32}"
NITER="${NITER:-50}"

say(){ printf "\n\033[1;36m%s\033[0m\n" "$*"; }
ok(){  printf "  ✅ %s\n" "$*\n"; }
info(){ printf "  • %s\n" "$*\n"; }

# --------------- CAS race ---------------
KEY="${NS}/casracer/customer/RACER-1"
say "1) CAS race (${C} workers x ${NITER})"

# Seed value
DOC='{"v":1,"customer_id":"RACER-1","tags":["seed"]}'
curl -sS -X POST "${BASE}/kv/${KEY}" \
  -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" \
  --data-binary "$DOC" >/dev/null

tmpout="$(mktemp)"; trap 'rm -f "$tmpout"' EXIT

for w in $(seq 1 "$C"); do
  (
    for i in $(seq 1 "$NITER"); do
      cas=$(curl -sS "${BASE}/kv/${KEY}/meta" -H "Authorization: Bearer ${TOKEN}" | jq -r '.cas')
      body=$(jq -nc --arg t "w'"$w"'_i'"$i"'" '{tags: [$t]}')
      curl -sS -X POST "${BASE}/kv/${KEY}?cas=${cas}&wait_ms=1" \
        -H "Authorization: Bearer ${TOKEN}" \
        -H "Content-Type: application/json" \
        --data-binary "$body"
      echo
    done
  ) >>"$tmpout" &
done
wait

# Classify results
OK=$(jq -r 'select(.ok==true) | 1' <"$tmpout" | wc -l | tr -d ' ')
MM=$(jq -r 'select(.error=="cas_mismatch") | 1' <"$tmpout" | wc -l | tr -d ' ')
RL=$(jq -r 'select(.error=="rate_limited") | 1' <"$tmpout" | wc -l | tr -d ' ')
BP=$(jq -r 'select(.error=="overloaded") | 1' <"$tmpout" | wc -l | tr -d ' ')
OT=$(jq -r 'select(.ok!=true and .error!="cas_mismatch" and .error!="rate_limited" and .error!="overloaded") | 1' <"$tmpout" | wc -l | tr -d ' ')
info "ok: ${OK}, cas_mismatch: ${MM}, 429(rate_limited): ${RL}, overloaded: ${BP}, other: ${OT}"

# --------------- Rate limit ---------------
say "2) Rate limit check (set RF_BEARER_TOKENS='dev@5:10:1000' before server start)"

RL_N="${RL_N:-60}"                            # how many requests to fire
rl_tmp="$(mktemp)"; trap 'rm -f "$rl_tmp"' EXIT

# Fire RL_N concurrent writes and record only status codes
for i in $(seq 1 "$RL_N"); do
  (
    curl -sS -m 3 --connect-timeout 1 \
      -o /dev/null -w "%{http_code}\n" \
      -X POST "${BASE}/kv/${NS}/rl/test-$i" \
      -H "Authorization: Bearer ${TOKEN}" \
      -H "Content-Type: application/json" \
      --data-binary '{}'
  ) >>"$rl_tmp" &
done
wait

c200=$(grep -c '^200$' "$rl_tmp" || true)
c429=$(grep -c '^429$' "$rl_tmp" || true)
info "HTTP 200: ${c200}, 429: ${c429}"

# --------------- Backpressure 503 ---------------
say "3) Backpressure 503 (debug hook)"
bp=$(curl -sS -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer ${TOKEN}" -H "X-Force-Overload: 1" \
  "${BASE}/kv/${NS}/bp/now")
[ "$bp" = "503" ] && ok "503 path reachable" || info "503 not returned (ok if not forcing)"
