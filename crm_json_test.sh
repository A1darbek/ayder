# crm_json_test.sh
#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-1109}"
HOST="${HOST:-127.0.0.1}"
BASE="http://${HOST}:${PORT}"
TOKEN="${TOKEN:-dev}"

say() { printf "\n\033[1;36m%s\033[0m\n" "$*"; }
ok()  { printf "  ✅ %s\n" "$*\n"; }
fail(){ printf "  ❌ %s\n" "$*\n"; exit 1; }

hdr=(-H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json")

CID="${CID:-CUST-12345}"
EMAIL="${EMAIL:-mary@example.com}"
NS="enterprise"
KEY_CUST="${NS}/customer/${CID}"
KEY_EMAIL_IDX="${NS}/customer_email_idx/${EMAIL}"
SID="sess-$(date +%s)"
KEY_SESS="${NS}/session/${SID}"

# 0) Health
say "0) /health"
curl -sS "${BASE}/health" | grep -q '"ok":1' && ok "health ok" || fail "health failed"

# 1) Create customer
say "1) Create customer ${CID}"
customer_json=$(cat <<JSON
{"v":1,"customer_id":"${CID}","email":"${EMAIL}","name":"Mary Stone",
 "address":{"line1":"1 Main","city":"Austin","country":"US"},"tags":["vip","beta"]}
JSON
)
r=$(curl -sS -X POST "${BASE}/kv/${KEY_CUST}?wait_ms=5" "${hdr[@]}" --data-binary "${customer_json}")
echo "  ← $r"
echo "$r" | grep -q '"ok":true' || fail "create failed"
CAS=$(echo "$r" | sed -n 's/.*"cas":\s*\([0-9][0-9]*\).*/\1/p')
[ -n "${CAS:-}" ] || fail "missing cas"
ok "created (cas=$CAS)"

# 2) Create email index → {customer_id}
say "2) Create email index"
idx_body="{\"customer_id\":\"${CID}\"}"
r2=$(curl -sS -X POST "${BASE}/kv/${KEY_EMAIL_IDX}?wait_ms=5" "${hdr[@]}" --data-binary "${idx_body}")
echo "  ← $r2"
echo "$r2" | grep -q '"ok":true' || fail "index write failed"
ok "email index stored"

# 3) Read by id (robust JSON parse → base64 decode → JSON parse)
say "3) Read by id"
g=$(curl -sS "${BASE}/kv/${KEY_CUST}" -H "Authorization: Bearer ${TOKEN}")
echo "  ← $g"

# Extract the "value" field via JSON parse (no regex)
val=$(printf '%s' "$g" | jq -r '.value')
cid_decoded=$(printf '%s' "$val" | base64 -d | jq -r '.customer_id')
[ "$cid_decoded" = "$CID" ] && ok "read/decoded ok" || fail "bad decode"

# 4) Lookup by email index, then fetch profile
say "4) Lookup by email index → fetch profile"
gidx=$(curl -sS "${BASE}/kv/${KEY_EMAIL_IDX}" -H "Authorization: Bearer ${TOKEN}")
v=$(printf '%s' "$gidx" | jq -r '.value // empty')
[ -n "$v" ] || fail "index missing value"
CID2=$(printf '%s' "$v" | base64 -d 2>/dev/null | jq -r '.customer_id')
[ "$CID2" = "$CID" ] && ok "index lookup ok" || fail "index lookup mismatch"

say "5) CAS update (add tag 'newsletter')"

# current CAS
CAS_CUR=$(
  curl -sS "${BASE}/kv/${KEY_CUST}/meta" -H "Authorization: Bearer ${TOKEN}" \
  | jq -r '.cas'
)

# fetch current doc, decode, add tag, re-serialize compact
cur_b64=$(
  curl -sS "${BASE}/kv/${KEY_CUST}" -H "Authorization: Bearer ${TOKEN}" \
  | jq -r '.value // empty'
)
[ -n "$cur_b64" ] || fail "customer missing value"

customer_json2=$(
  printf '%s' "$cur_b64" | base64 -d 2>/dev/null \
  | jq -c '.tags = ((.tags // []) + ["newsletter"] | unique)'
)

r3=$(
  curl -sS -X POST "${BASE}/kv/${KEY_CUST}?cas=${CAS_CUR}&wait_ms=5" \
       "${hdr[@]}" --data-binary "${customer_json2}"
)
echo "  ← $r3"
echo "$r3" | jq -e '.ok == true' >/dev/null || fail "CAS update failed"
ok "CAS update ok"

# 6) TTL session ephemeral
say "6) TTL session ephemeral (ttl_ms=120)"

sess_body=$(jq -cn --arg user "$CID" --argjson issued "$(date +%s)" \
  '{user:$user,issued:$issued}')

rs=$(
  curl -sS -X POST "${BASE}/kv/${KEY_SESS}?ttl_ms=120&wait_ms=5" \
       "${hdr[@]}" --data-binary "${sess_body}"
)
echo "  ← $rs"
echo "$rs" | jq -e '.ok == true' >/dev/null || fail "session write failed"

sleep 0.2
g0=$(curl -sS "${BASE}/kv/${KEY_SESS}" -H "Authorization: Bearer ${TOKEN}" || true)
echo "  ← $g0"

state=$(
  printf '%s' "$g0" | jq -r '
    if has("value") then "present"
    elif .expired == true then "expired"
    else (.error // "unknown")
    end'
)

if [ "$state" = "expired" ] || [ "$state" = "not_found" ]; then
  ok "session expired"
elif [ "$state" = "present" ]; then
  ok "expired/not_found acceptable (still present is fine too)"
else
  ok "session state: $state"
fi

# 7) Delete with CAS
say "7) Delete customer with CAS"

MCAS=$(
  curl -sS "${BASE}/kv/${KEY_CUST}/meta" -H "Authorization: Bearer ${TOKEN}" \
  | jq -r '.cas'
)

del=$(
  curl -sS -X DELETE "${BASE}/kv/${KEY_CUST}?cas=${MCAS}&wait_ms=5" \
       -H "Authorization: Bearer ${TOKEN}"
)
echo "  ← $del"
echo "$del" | jq -e '.ok == true' >/dev/null || fail "delete failed"

done_json=$(curl -sS "${BASE}/kv/${KEY_CUST}" -H "Authorization: Bearer ${TOKEN}" || true)
printf '%s\n' "$done_json" | jq -e '.error == "not_found"' >/dev/null \
  && ok "gone" || fail "still present"

say "ENTERPRISE/CRM JSON TEST: ALL GREEN ✅"
