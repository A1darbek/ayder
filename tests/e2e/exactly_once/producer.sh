#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/e2e/exactly_once/lib.sh
source "${SCRIPT_DIR}/lib.sh"

TOTAL_EVENTS="${TOTAL_EVENTS:-200}"
DUP_EVERY="${DUP_EVERY:-10}"
PRODUCE_TIMEOUT_MS="${PRODUCE_TIMEOUT_MS:-5000}"
MAX_RETRIES="${MAX_RETRIES:-8}"
RETRY_SLEEP_MS="${RETRY_SLEEP_MS:-120}"

require_cmd jq
require_cmd curl
require_cmd "$DOCKER_BIN"

ensure_topic

log "producer start events=${TOTAL_EVENTS} dup_every=${DUP_EVERY}"

for i in $(seq 1 "$TOTAL_EVENTS"); do
  event_id="evt-$(printf '%06d' "$i")"
  account_id="acct-$(printf '%02d' $(( (i % 5) + 1 )))"
  delta=1
  payload="$(jq -cn --arg event_id "$event_id" --arg account_id "$account_id" --argjson delta "$delta" '{event_id:$event_id,account_id:$account_id,delta:$delta}')"

  pg_exec_sql "INSERT INTO expected_events(event_id, account_id, delta) VALUES ('${event_id}', '${account_id}', ${delta}) ON CONFLICT (event_id) DO NOTHING;"

  attempt=1
  while true; do
    set +e
    resp="$(curl -sS -X POST "${AYDER_BASE}/broker/topics/${TOPIC}/produce?partition=${PARTITION}&timeout_ms=${PRODUCE_TIMEOUT_MS}&idempotency_key=${event_id}" \
      -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$payload")"
    rc=$?
    set -e

    if [[ $rc -eq 0 ]] && echo "$resp" | jq -e '.ok==true' >/dev/null 2>&1; then
      break
    fi

    if (( attempt >= MAX_RETRIES )); then
      echo "producer failed for ${event_id} after ${MAX_RETRIES} retries: ${resp}" >&2
      exit 1
    fi

    sleep_s="$(awk "BEGIN{printf \"%.3f\", ${RETRY_SLEEP_MS} / 1000.0}")"
    sleep "$sleep_s"
    attempt=$((attempt + 1))
  done

  if (( DUP_EVERY > 0 )) && (( i % DUP_EVERY == 0 )); then
    curl -fsS -X POST "${AYDER_BASE}/broker/topics/${TOPIC}/produce?partition=${PARTITION}&timeout_ms=${PRODUCE_TIMEOUT_MS}&idempotency_key=${event_id}" \
      -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$payload" >/dev/null
  fi

done

log "producer complete"
