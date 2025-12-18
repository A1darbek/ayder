#!/usr/bin/env bash
set -Eeuo pipefail

APP_BIN=${APP_BIN:-"./ramforge"}
PORT=${PORT:-1109}
BASE_URL="http://127.0.0.1:${PORT}"

# auth (same pattern as broker)
export RF_BEARER_TOKENS="${RF_BEARER_TOKENS:-dev@5000:5000:100000}"
TOKEN="${TOKEN:-dev}"
TOKENS="${TOKENS:-}"
first_token(){ [[ -n "${TOKENS}" ]] && printf '%s' "$TOKENS" | tr ';' ',' | cut -d',' -f1 || printf '%s' "$TOKEN"; }
FIRST="$(first_token)"
HDR_AUTH=(); [[ -n "$FIRST" ]] && HDR_AUTH=(-H "Authorization: Bearer ${FIRST}")

NS="kvproof"
TS=$(date +%s)
KEY_DURABLE="key.${TS}.durable"
KEY_ROCKET="key.${TS}.rocket"
KEY_TOMBSTONE="key.${TS}.tomb"
KEY_TOMBSTONE_ND="key.${TS}.tomb_nd"
VAL_DURABLE="sealed-durable-kv-1"
VAL_ROCKET="rocket-kv-ephemeral-1"
VAL_TOMB="tomb-me"
VAL_TOMB_ND="tomb-me-nd"
DURABLE_TIMEOUT_MS=${DURABLE_TIMEOUT_MS:-5000}

log(){ echo -e "$*"; }
b64(){
  if command -v base64 >/dev/null 2>&1; then printf '%s' "$1" | base64 | tr -d '\n'
  else printf '%s' "$1" | openssl base64 -A
  fi
}
wait_health(){ until curl -sS "${BASE_URL}/health" | grep -q '"ok":1'; do sleep 0.05; done; log "✅ /health OK"; }

APP_PID=""; PGID=""
start_cluster(){
  local rocket="${1:-}"
  if [[ -n "${rocket}" ]]; then
    log "🚀 starting with RF_ROCKET=1"
    setsid env RF_ROCKET=1 "${APP_BIN}" --port "${PORT}" > ./kv_durability.log 2>&1 & APP_PID=$!
  else
    log "🟢 starting (sealed/durable mode default)"
    setsid "${APP_BIN}" --port "${PORT}" > ./kv_durability.log 2>&1 & APP_PID=$!
  fi
  PGID=$(ps -o pgid= -p "${APP_PID}" | tr -d ' ')
  log "   pid=${APP_PID} pgid=${PGID}"
}
kill_cluster_hard(){ [[ -n "${PGID:-}" ]] && kill -9 -"${PGID}" || true; sleep 0.2; }

kv_put(){ # POST /kv/:ns/:key
  local ns="$1" key="$2" val="$3" q="$4"
  curl -sS --http1.0 -X POST \
    "${BASE_URL}/kv/${ns}/${key}?${q}" \
    -H 'content-type: application/octet-stream' \
    "${HDR_AUTH[@]}" \
    --data-binary "$val"
}
kv_get(){ # GET /kv/:ns/:key
  local ns="$1" key="$2"
  curl -sS "${BASE_URL}/kv/${ns}/${key}" "${HDR_AUTH[@]}"
}
kv_del(){ # DELETE /kv/:ns/:key
  local ns="$1" key="$2" q="$3"
  curl -sS -X DELETE "${BASE_URL}/kv/${ns}/${key}?${q}" "${HDR_AUTH[@]}"
}
sealed_status_last(){ curl -sS "${BASE_URL}/admin/sealed/status" "${HDR_AUTH[@]}" | jq -r '.last_synced_batch_id // 0'; }
wait_synced_bid(){
  local bid="$1"
  [[ -z "${bid}" || "${bid}" == "0" ]] && return 0
  local t0; t0=$(date +%s)
  while :; do
    local last; last=$(sealed_status_last || echo 0)
    [[ "$last" -ge "$bid" ]] && return 0
    [[ $(( $(date +%s) - t0 )) -ge $(( (DURABLE_TIMEOUT_MS/1000)+1 )) ]] && break
    sleep 0.05
  done
}

# clean any persisted state for a deterministic run
rm -f ./append.aof ./append.aof.sealed ./append.aof.sealed.lock ./zp_dump.rdb || true

# ──────────────────────────────────────────────────────────────────────────────
# TEST 1: KV sealed + durable PUT survives crash
# ──────────────────────────────────────────────────────────────────────────────
start_cluster
wait_health

log "\n=== TEST 1: KV sealed + durable survives crash ==="
RESP=$(kv_put "$NS" "$KEY_DURABLE" "$VAL_DURABLE" "durable=1&timeout_ms=${DURABLE_TIMEOUT_MS}")
log "kv_put(sealed,durable) → $RESP"

BID=$(echo "$RESP" | jq -r '.batch_id // 0')
SYNCED=$(echo "$RESP" | jq -r '.synced // empty')
[[ "$SYNCED" != "true" ]] && wait_synced_bid "$BID"

kill_cluster_hard; log "💥 crashed cluster"
start_cluster; wait_health

READ=$(kv_get "$NS" "$KEY_DURABLE"); log "get(${KEY_DURABLE}) → $READ"
VAL_B64=$(echo "$READ" | jq -r '.value // empty')
[[ -n "$VAL_B64" && "$VAL_B64" == "$(b64 "$VAL_DURABLE")" ]] && log "✅ PASS" || { log "❌ FAIL"; kill_cluster_hard; exit 1; }

# ──────────────────────────────────────────────────────────────────────────────
# TEST 2: KV rocket (non-durable) does NOT survive crash
# ──────────────────────────────────────────────────────────────────────────────
kill_cluster_hard
start_cluster "1" # RF_ROCKET=1
wait_health

log "\n=== TEST 2: KV rocket (non-durable) does NOT survive crash ==="
RESP2=$(kv_put "$NS" "$KEY_ROCKET" "$VAL_ROCKET" "timeout_ms=0")
log "kv_put(rocket) → $RESP2"
kill_cluster_hard; log "💥 crashed cluster"
start_cluster; wait_health

READ2=$(kv_get "$NS" "$KEY_ROCKET"); log "get(${KEY_ROCKET}) → $READ2"
ERR2=$(echo "$READ2" | jq -r '.error // empty')
[[ "$ERR2" == "not_found" || -z "$(echo "$READ2" | jq -r '.value // empty')" ]] && log "✅ PASS" || { log "❌ FAIL"; kill_cluster_hard; exit 1; }

# ──────────────────────────────────────────────────────────────────────────────
# TEST 3: KV sealed + durable DELETE (tombstone) survives crash
# ──────────────────────────────────────────────────────────────────────────────
kill_cluster_hard
start_cluster          # back to sealed/durable mode
wait_health

log "\n=== TEST 3: KV durable tombstone (DELETE) survives crash ==="
# 3a) create the key durably
RESP3P=$(kv_put "$NS" "$KEY_TOMBSTONE" "$VAL_TOMB" "durable=1&timeout_ms=${DURABLE_TIMEOUT_MS}")
log "kv_put(sealed,durable,tomb target) → $RESP3P"
BIDP=$(echo "$RESP3P" | jq -r '.batch_id // 0'); SYNP=$(echo "$RESP3P" | jq -r '.synced // empty')
[[ "$SYNP" != "true" ]] && wait_synced_bid "$BIDP"

# 3b) DELETE with durable=1
RESP3D=$(kv_del "$NS" "$KEY_TOMBSTONE" "durable=1&timeout_ms=${DURABLE_TIMEOUT_MS}")
log "kv_del(sealed,durable) → $RESP3D"
BIDD=$(echo "$RESP3D" | jq -r '.batch_id // 0'); SYND=$(echo "$RESP3D" | jq -r '.synced // empty')
[[ "$SYND" != "true" ]] && wait_synced_bid "$BIDD"

# 3c) crash → restart
kill_cluster_hard; log "💥 crashed cluster"
start_cluster; wait_health

# 3d) verify key remains deleted after restart
READ3=$(kv_get "$NS" "$KEY_TOMBSTONE"); log "get(${KEY_TOMBSTONE}) → $READ3"
ERR3=$(echo "$READ3" | jq -r '.error // empty')
if [[ "$ERR3" == "not_found" || -z "$(echo "$READ3" | jq -r '.value // empty')" ]]; then
  log "✅ PASS (durable tombstone persisted across crash)"
else
  log "❌ FAIL (deleted key reappeared or read returned a value)"
  kill_cluster_hard; exit 1
fi

# ──────────────────────────────────────────────────────────────────────────────
# TEST 4: Rocket-mode non-durable DELETE must NOT persist a tombstone
#         (after crash+restart in sealed mode, the key should still exist)
# ──────────────────────────────────────────────────────────────────────────────
kill_cluster_hard
start_cluster          # sealed to create a durable baseline
wait_health

log "\n=== TEST 4: Rocket non-durable DELETE does not persist tombstone ==="
# 4a) create a durable key that we'll try to delete non-durably
RESP4P=$(kv_put "$NS" "$KEY_TOMBSTONE_ND" "$VAL_TOMB_ND" "durable=1&timeout_ms=${DURABLE_TIMEOUT_MS}")
log "kv_put(sealed,durable,nd target) → $RESP4P"
BID4P=$(echo "$RESP4P" | jq -r '.batch_id // 0'); SYN4P=$(echo "$RESP4P" | jq -r '.synced // empty')
[[ "$SYN4P" != "true" ]] && wait_synced_bid "$BID4P"

# 4b) switch to rocket mode and issue a non-durable DELETE (no durable=1)
kill_cluster_hard
start_cluster "1"      # RF_ROCKET=1
wait_health
RESP4D=$(kv_del "$NS" "$KEY_TOMBSTONE_ND" "timeout_ms=0")
log "kv_del(rocket,non-durable) → $RESP4D"

# 4c) crash → restart sealed
kill_cluster_hard; log "💥 crashed cluster"
start_cluster; wait_health

# 4d) key should still exist because the non-durable delete didn't write a tombstone
READ4=$(kv_get "$NS" "$KEY_TOMBSTONE_ND"); log "get(${KEY_TOMBSTONE_ND}) → $READ4"
VAL4_B64=$(echo "$READ4" | jq -r '.value // empty')
if [[ -n "$VAL4_B64" && "$VAL4_B64" == "$(b64 "$VAL_TOMB_ND")" ]]; then
  log "✅ PASS (non-durable delete did not persist a tombstone)"
else
  log "❌ FAIL (key missing or value mismatch after non-durable delete)"
  kill_cluster_hard; exit 1
fi

log "\n🎉 All KV durability tests (sealed put, rocket put loss, durable tombstone, non-durable tombstone) passed."
kill_cluster_hard
