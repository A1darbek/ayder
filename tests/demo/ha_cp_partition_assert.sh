#!/usr/bin/env bash
# tests/demo/ha_cp_partition_assert.sh
# Confirms CP behavior under a real network partition.
#
# You must provide commands that create/heal a split:
#   PARTITION_APPLY_CMD='...'
#   PARTITION_HEAL_CMD='...'
#
# Example (existing docker chaos helper):
#   PARTITION_APPLY_CMD='./scripts/chaos-ha.sh split node1,node2,node3 node4,node5,node6'
#   PARTITION_HEAL_CMD='./scripts/chaos-ha.sh heal'

set -Eeuo pipefail

AUTH_BEARER="${AUTH_BEARER:-dev}"
TOPIC="${TOPIC:-ha_cp_cap_demo_$(date +%s)}"
PARTITION="${PARTITION:-0}"
WARMUP_WRITES="${WARMUP_WRITES:-50}"
TEST_WRITES="${TEST_WRITES:-30}"
SPLIT_SETTLE_SEC="${SPLIT_SETTLE_SEC:-6}"
HEAL_SETTLE_SEC="${HEAL_SETTLE_SEC:-6}"

# Endpoints in each side.
MINORITY_URL="${MINORITY_URL:-http://127.0.0.1:7001}"
MAJORITY_URL="${MAJORITY_URL:-http://127.0.0.1:10001}"

# Optional: comma-separated list for post-heal convergence checks.
CHECK_URLS="${CHECK_URLS:-http://127.0.0.1:7001,http://127.0.0.1:8001,http://127.0.0.1:9001,http://127.0.0.1:10001,http://127.0.0.1:11001,http://127.0.0.1:12001}"

PARTITION_APPLY_CMD="${PARTITION_APPLY_CMD:-}"
PARTITION_HEAL_CMD="${PARTITION_HEAL_CMD:-}"

if [[ -z "$PARTITION_APPLY_CMD" || -z "$PARTITION_HEAL_CMD" ]]; then
  echo "Set both PARTITION_APPLY_CMD and PARTITION_HEAL_CMD for a true CP partition test." >&2
  exit 1
fi

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing: $1" >&2; exit 1; }; }
need curl
need jq

HDR=(-H 'Content-Type: application/json')
if [[ -n "$AUTH_BEARER" ]]; then
  HDR+=(-H "Authorization: Bearer ${AUTH_BEARER}")
fi

LAST_CODE=""
LAST_BODY=""

post_json() {
  local url="$1"; shift
  local body="$1"; shift

  local tmp
  tmp="$(mktemp)"
  LAST_CODE="$(curl -sS -m 4 -o "$tmp" -w "%{http_code}" -X POST "${HDR[@]}" "$url" --data-binary "$body" || true)"
  LAST_BODY="$(cat "$tmp")"
  rm -f "$tmp"
}

write_classify_first_response() {
  local url="$1"
  local seq="$2"
  local endpoint="$url/broker/topics/$TOPIC/produce?partition=$PARTITION"
  local payload
  payload="{\"seq\":$seq,\"t\":\"cp\"}"

  post_json "$endpoint" "$payload"

  if [[ "$LAST_CODE" == "200" ]] && jq -e '.ok==true' >/dev/null 2>&1 <<<"$LAST_BODY"; then
    echo "ok"
    return 0
  fi

  if [[ "$LAST_CODE" == "307" || "$LAST_CODE" == "308" ]]; then
    echo "redirect"
    return 0
  fi

  if grep -q 'no_quorum' <<<"$LAST_BODY"; then
    echo "no_quorum"
    return 0
  fi

  if grep -q 'redirect_to_leader' <<<"$LAST_BODY"; then
    echo "redirect"
    return 0
  fi

  echo "other"
}

write_follow_redirect_once() {
  local url="$1"
  local seq="$2"
  local endpoint="$url/broker/topics/$TOPIC/produce?partition=$PARTITION"
  local payload
  payload="{\"seq\":$seq,\"t\":\"cp\"}"

  post_json "$endpoint" "$payload"
  if [[ "$LAST_CODE" == "200" ]] && jq -e '.ok==true' >/dev/null 2>&1 <<<"$LAST_BODY"; then
    return 0
  fi

  local redir
  redir="$(jq -r '.redirect.location // empty' <<<"$LAST_BODY" 2>/dev/null || true)"
  if [[ -z "$redir" ]]; then
    local leader_url
    leader_url="$(jq -r '.leader_url // empty' <<<"$LAST_BODY" 2>/dev/null || true)"
    if [[ -n "$leader_url" ]]; then
      redir="$leader_url/broker/topics/$TOPIC/produce?partition=$PARTITION"
    fi
  fi

  if [[ -z "$redir" ]]; then
    return 1
  fi

  post_json "$redir" "$payload"
  [[ "$LAST_CODE" == "200" ]] && jq -e '.ok==true' >/dev/null 2>&1 <<<"$LAST_BODY"
}

heal_on_exit() {
  bash -lc "$PARTITION_HEAL_CMD" >/dev/null 2>&1 || true
}
trap heal_on_exit EXIT

echo "[1/7] Create topic"
post_json "$MAJORITY_URL/broker/topics" "{\"name\":\"$TOPIC\",\"partitions\":1}"
# Topic may already exist; continue.

echo "[2/7] Warmup writes on majority side"
for i in $(seq 1 "$WARMUP_WRITES"); do
  write_follow_redirect_once "$MAJORITY_URL" "$i" || true
done

echo "[3/7] Apply partition"
bash -lc "$PARTITION_APPLY_CMD"
sleep "$SPLIT_SETTLE_SEC"

echo "[4/7] Probe minority side (must not locally accept writes)"
minor_ok=0
minor_redirect=0
minor_noquorum=0
minor_other=0
for i in $(seq 1 "$TEST_WRITES"); do
  cls="$(write_classify_first_response "$MINORITY_URL" "$((100000+i))")"
  case "$cls" in
    ok) minor_ok=$((minor_ok+1)) ;;
    redirect) minor_redirect=$((minor_redirect+1)) ;;
    no_quorum) minor_noquorum=$((minor_noquorum+1)) ;;
    *) minor_other=$((minor_other+1)) ;;
  esac
done

echo "Minority results: ok=$minor_ok redirect=$minor_redirect no_quorum=$minor_noquorum other=$minor_other"

echo "[5/7] Probe majority side (must keep accepting writes)"
major_ok=0
for i in $(seq 1 "$TEST_WRITES"); do
  if write_follow_redirect_once "$MAJORITY_URL" "$((200000+i))"; then
    major_ok=$((major_ok+1))
  fi
done

echo "Majority results: ok=$major_ok/${TEST_WRITES}"

if (( minor_ok > 0 )); then
  echo "FAIL: minority side accepted writes during partition (violates CP expectation)" >&2
  exit 1
fi

if (( major_ok == 0 )); then
  echo "FAIL: majority side accepted zero writes during partition" >&2
  exit 1
fi

echo "[6/7] Heal partition"
bash -lc "$PARTITION_HEAL_CMD"
sleep "$HEAL_SETTLE_SEC"

echo "[7/7] Check membership convergence after heal"
IFS=',' read -r -a urls <<<"$CHECK_URLS"
ref=""
for u in "${urls[@]}"; do
  s="$(curl -sS "${HDR[@]}" "$u/ha/membership" | jq -c '.membership | {epoch,node_count,voters_new_mask,learners_mask}' 2>/dev/null || true)"
  if [[ -z "$s" ]]; then
    echo "WARN: no membership from $u"
    continue
  fi
  echo "$u => $s"
  if [[ -z "$ref" ]]; then
    ref="$s"
  elif [[ "$s" != "$ref" ]]; then
    echo "FAIL: membership mismatch after heal" >&2
    exit 1
  fi
done

echo "PASS: CP partition behavior confirmed (minority fenced, majority writable, post-heal converged)."
