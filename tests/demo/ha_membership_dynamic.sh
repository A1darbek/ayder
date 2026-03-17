#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:7001}"
TOKEN="${TOKEN:-}"

hdr=(-H "Content-Type: application/json")
if [[ -n "$TOKEN" ]]; then
  hdr+=(-H "Authorization: Bearer $TOKEN")
fi

echo "== membership =="
curl -sS "$BASE_URL/ha/membership" "${hdr[@]}" | jq . || true

echo "== add learner node6 =="
curl -sS -X POST "$BASE_URL/ha/membership/add" "${hdr[@]}" \
  -d '{"node_id":"node6","advertise_addr":"127.0.0.1:12000","http_addr":"http://127.0.0.1:12001","priority":50}' | jq . || true

echo "== promote node6 =="
curl -sS -X POST "$BASE_URL/ha/membership/promote" "${hdr[@]}" \
  -d '{"node_id":"node6"}' | jq . || true

echo "== remove node6 =="
curl -sS -X POST "$BASE_URL/ha/membership/remove" "${hdr[@]}" \
  -d '{"node_id":"node6"}' | jq . || true
