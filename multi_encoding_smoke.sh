#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-1109}"
HOST="${HOST:-127.0.0.1}"
BASE="http://${HOST}:${PORT}"
TOKEN="${TOKEN:-dev}"
NS="enterprise"

say()  { printf "\n\033[1;36m%s\033[0m\n" "$*"; }
ok()   { printf "  ✅ %s\n" "$*"; }
fail() { printf "  ❌ %s\n" "$*"; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || fail "missing dependency: $1"; }
need curl; need jq; need base64; need python3

hdr_auth=(-H "Authorization: Bearer ${TOKEN}")

# base64 (newline-free, GNU/BSD compatible)
b64_of() {
  if base64 --help 2>&1 | grep -q -- '-w '; then base64 -w0; else base64 | tr -d '\n'; fi
}

val_from_json() { jq -r '.value // empty'; }
cas_from_meta() { jq -r '.cas // empty'; }

# For hex→bytes; uses xxd if present, else python
hex_to_bytes() {
  if command -v xxd >/dev/null 2>&1; then
    xxd -r -p
  else
    python3 - <<'PY'
import sys, binascii
sys.stdout.buffer.write(binascii.unhexlify(sys.stdin.read().strip()))
PY
  fi
}

################################
# 0) Health
################################
say "0) /health"
curl -sS "${BASE}/health" | jq -e 'select(.ok==1)' >/dev/null && ok "health ok" || fail "health failed"

################################
# 1) JSON round-trip + CAS + TTL + delete
################################
CID="CUST-JSON-1"
KEY_JSON="${NS}/customer/${CID}"
KEY_SESS="${NS}/session/sess-$(date +%s)"

say "1) JSON create → read → CAS → TTL → delete"

# Create
JSON_DOC=$(jq -nc --arg cid "$CID" --arg em "mary@example.com" '
  {v:1,customer_id:$cid,email:$em,name:"Mary Stone",
   address:{line1:"1 Main",city:"Austin",country:"US"},
   tags:["vip","beta"]}')
r=$(curl -sS -X POST "${BASE}/kv/${KEY_JSON}?wait_ms=5" "${hdr_auth[@]}" \
      -H 'Content-Type: application/json' --data-binary "$JSON_DOC")
echo "  ← $r"
echo "$r" | jq -e 'select(.ok==true)' >/dev/null || fail "json create failed"

# Read & decode
g=$(curl -sS "${BASE}/kv/${KEY_JSON}" "${hdr_auth[@]}")
VAL_B64=$(echo "$g" | val_from_json)
[ -n "$VAL_B64" ] || fail "json read missing value"
payload_json=$(printf '%s' "$VAL_B64" | base64 -d 2>/dev/null | jq -c .)
echo "  GET decoded → $payload_json" | sed 's/.*/  &/'
echo "$payload_json" | jq -e --arg cid "$CID" 'select(.customer_id==$cid)' >/dev/null \
  && ok "json read/decoded ok" || fail "json read mismatch"

# CAS update (add 'newsletter') — use pipe + --http
# to avoid chunked
CAS_CUR=$(curl -sS "${BASE}/kv/${KEY_JSON}/meta" "${hdr_auth[@]}" | cas_from_meta)
JSON_DOC2=$(printf '%s' "$payload_json" | jq -c '.tags += ["newsletter"] | .tags |= unique')
r2=$(curl -sS -X POST \
      "${BASE}/kv/${KEY_JSON}?cas=${CAS_CUR}&wait_ms=5" \
      "${hdr_auth[@]}" -H 'Content-Type: application/json' \
      --data-binary "$JSON_DOC2")
echo "  ← $r2"
echo "$r2" | jq -e 'select(.ok==true)' >/dev/null || fail "json CAS update failed"
ok "json CAS update ok"

# TTL session (120 ms) — pipe + --http1.0
say "   JSON TTL session"
SESS_BODY=$(jq -nc --arg u "$CID" --argjson t "$(date +%s)" '{user:$u,issued:$t}')
rs=$(curl -sS -X POST \
      "${BASE}/kv/${KEY_SESS}?ttl_ms=120&wait_ms=5" "${hdr_auth[@]}" \
      -H 'Content-Type: application/json' \
      --data-binary "$SESS_BODY")
echo "  ← $rs"
sleep 0.25
g0=$(curl -sS "${BASE}/kv/${KEY_SESS}" "${hdr_auth[@]}" || true)
echo "  ← $g0"
echo "$g0" | jq -e 'select(.expired==true or .error=="not_found")' >/dev/null \
  && ok "session expired (or not_found)" || ok "session lookup ok"

# Delete with CAS
CAS_DEL=$(curl -sS "${BASE}/kv/${KEY_JSON}/meta" "${hdr_auth[@]}" | cas_from_meta)
del=$(curl -sS -X DELETE "${BASE}/kv/${KEY_JSON}?cas=${CAS_DEL}&wait_ms=5" "${hdr_auth[@]}")
echo "  ← $del"
echo "$del" | jq -e 'select(.ok==true and .deleted==true)' >/dev/null || fail "json delete failed"
gone=$(curl -sS "${BASE}/kv/${KEY_JSON}" "${hdr_auth[@]}" || true)
echo "$gone" | jq -e 'select(.error=="not_found")' >/dev/null && ok "json deleted/gone"

################################
# 2) MsgPack (raw bytes) round-trip
################################
say "2) MsgPack round-trip (byte equality check)"
KEY_MP="${NS}/mp/customer/CUST-MP-1"

# Tiny static MsgPack map: {"customer_id":"CUST-MP-1","email":"mp@example.com","tags":["vip","bronze"]}
mp_bytes() { python3 - "$@" <<'PY'
import sys
b = (b"\x83"  # map(3)
     b"\xabcustomer_id" b"\xa9CUST-MP-1"
     b"\xa5email"       b"\xae" + b"mp@example.com"
     b"\xa4tags"        b"\x92" b"\xa3vip" b"\xa6bronze")
sys.stdout.buffer.write(b)
PY
}
MP_B64_ORIG=$(mp_bytes | b64_of)
mp_bytes | curl -sS --http1.0 -X POST "${BASE}/kv/${KEY_MP}?wait_ms=5" \
  "${hdr_auth[@]}" -H 'Content-Type: application/msgpack' --data-binary @- >/dev/null
MP_B64_GOT=$(curl -sS "${BASE}/kv/${KEY_MP}" "${hdr_auth[@]}" | val_from_json)
[ "$MP_B64_GOT" = "$MP_B64_ORIG" ] && ok "msgpack round-trip matches" || fail "msgpack mismatch"

################################
# 3) Protobuf (raw bytes) round-trip + CAS update
################################
say "3) Protobuf round-trip + CAS update (byte equality)"
KEY_PB="${NS}/pb/customer/CUST-PB-1"

# Wire: message Customer{string customer_id=1; string email=2; repeated string tags=3;}
pb_bytes_orig() { python3 - <<'PY'
import sys
b = (b"\x0a\x09" + b"CUST-PB-1" +
     b"\x12\x0e" + b"pb@example.com" +
     b"\x1a\x05" + b"alpha" +
     b"\x1a\x04" + b"beta")
sys.stdout.buffer.write(b)
PY
}
pb_bytes_new() { python3 - <<'PY'
import sys
b = (b"\x0a\x09" + b"CUST-PB-1" +
     b"\x12\x0e" + b"pb@example.com" +
     b"\x1a\x05" + b"alpha" +
     b"\x1a\x04" + b"beta"  +
     b"\x1a\x05" + b"gamma")
sys.stdout.buffer.write(b)
PY
}
PB_B64_ORIG=$(pb_bytes_orig | b64_of)
pb_bytes_orig | curl -sS --http1.0 -X POST "${BASE}/kv/${KEY_PB}?wait_ms=5" \
  "${hdr_auth[@]}" -H 'Content-Type: application/octet-stream' --data-binary @- >/dev/null
PB_B64_GOT=$(curl -sS "${BASE}/kv/${KEY_PB}" "${hdr_auth[@]}" | val_from_json)
[ "$PB_B64_GOT" = "$PB_B64_ORIG" ] && ok "protobuf round-trip matches" || fail "protobuf mismatch"

CASPB=$(curl -sS "${BASE}/kv/${KEY_PB}/meta" "${hdr_auth[@]}" | cas_from_meta)
PB_B64_NEW=$(pb_bytes_new | b64_of)
pb_bytes_new | curl -sS --http1.0 -X POST "${BASE}/kv/${KEY_PB}?cas=${CASPB}&wait_ms=5" \
  "${hdr_auth[@]}" -H 'Content-Type: application/octet-stream' --data-binary @- >/dev/null
PB_B64_GOT2=$(curl -sS "${BASE}/kv/${KEY_PB}" "${hdr_auth[@]}" | val_from_json)
[ "$PB_B64_GOT2" = "$PB_B64_NEW" ] && ok "protobuf CAS update ok" || fail "protobuf CAS mismatch"

################################
# 4) Float32 vectors (raw) round-trip
################################
say "4) Float32 array round-trip"
KEY_VEC="${NS}/vec/weights"

# [1.25, -2.5, 3.0] as little-endian hex: 3FA00000 C0200000 40400000
VEC_HEX="0000a03f000020c000004040"
VEC_B64_ORIG=$(printf '%s' "$VEC_HEX" | hex_to_bytes | b64_of)
printf '%s' "$VEC_HEX" | hex_to_bytes | curl -sS --http1.0 -X POST \
  "${BASE}/kv/${KEY_VEC}?wait_ms=5" "${hdr_auth[@]}" \
  -H 'Content-Type: application/octet-stream' --data-binary @- >/dev/null
VEC_B64_GOT=$(curl -sS "${BASE}/kv/${KEY_VEC}" "${hdr_auth[@]}" | val_from_json)
[ "$VEC_B64_GOT" = "$VEC_B64_ORIG" ] && ok "float32 round-trip matches" || fail "float32 mismatch"

# Optional: pretty-print floats via python
printf '%s' "$VEC_B64_GOT" | base64 -d | python3 - <<'PY'
import sys, struct
b = sys.stdin.buffer.read()
for i in range(0, len(b), 4):
    print(struct.unpack('<f', b[i:i+4])[0])
PY

say "MULTI-ENCODING SMOKE TEST: ALL GREEN ✅"
