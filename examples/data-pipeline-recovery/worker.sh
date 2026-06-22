#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=examples/data-pipeline-recovery/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd curl
require_cmd base64

SCENARIO="${SCENARIO:-partial_download}"
REPLAY_NUMBER="${REPLAY_NUMBER:-0}"

consume_url="${AYDER_BASE}/broker/consume/${TOPIC}/${GROUP}/${PARTITION}?offset=0&limit=1&encoding=b64"
raw="$(curl -sS -w $'\n%{http_code}' -H "$AUTH_HEADER" "$consume_url")"
http_code="${raw##*$'\n'}"
body="${raw%$'\n'*}"
[[ "$http_code" == "200" ]] || { echo "consume failed http=${http_code}: ${body}" >&2; exit 1; }

count="$(echo "$body" | jq -r '.count // 0')"
[[ "$count" == "1" ]] || { echo "expected original Ayder event at offset 0, got ${count}" >&2; exit 1; }

offset="$(echo "$body" | jq -r '.messages[0].offset')"
value_b64="$(echo "$body" | jq -r '.messages[0].value_b64')"
payload_json="$(printf '%s' "$value_b64" | base64 -d)"
job_run_id="$(printf '%s' "$payload_json" | jq -r '.job_run_id')"
job_name="$(printf '%s' "$payload_json" | jq -r '.job_name')"
stage="$(printf '%s' "$payload_json" | jq -r '.stage')"
external_source="$(printf '%s' "$payload_json" | jq -r '.external_source')"
expected_pages="$(printf '%s' "$payload_json" | jq -r '.expected_pages')"

if [[ "$REPLAY_NUMBER" == "0" ]]; then
  pg_exec_sql "
INSERT INTO pipeline_recovery_runs(job_run_id, scenario, job_name, stage, external_source,
  expected_pages, status)
VALUES ('${job_run_id}', '${SCENARIO}', '${job_name}', '${stage}', '${external_source}',
  ${expected_pages}, 'RUNNING');
"

  confirmed_to=10
  case "$SCENARIO" in
    full_success) confirmed_to=10 ;;
    partial_download|partial_usable|replay_missing_partition) confirmed_to=8 ;;
    hard_failure) confirmed_to=4 ;;
    *) echo "invalid SCENARIO=${SCENARIO}" >&2; exit 2 ;;
  esac

  for page in $(seq 1 10); do
    critical=false
    if [[ "$SCENARIO" == "hard_failure" && "$page" -ge 5 ]]; then critical=true; fi

    if (( page <= confirmed_to )); then
      pg_exec_sql "
INSERT INTO pipeline_page_downloads(job_run_id, page_no, critical, status, records_loaded)
VALUES ('${job_run_id}', ${page}, ${critical}, 'CONFIRMED', 100);
"
    else
      pg_exec_sql "
INSERT INTO pipeline_page_downloads(job_run_id, page_no, critical, status, records_loaded, last_error)
VALUES ('${job_run_id}', ${page}, ${critical}, 'MISSING', 0, 'API timeout');
"
    fi
  done

  if (( confirmed_to < 10 )); then
    pg_exec_sql "
INSERT INTO pipeline_retry_attempts(retry_id, job_run_id, page_no, attempt_no, outcome, error)
VALUES
  ('retry_${job_run_id}_1', '${job_run_id}', $((confirmed_to + 1)), 1, 'FAILED', 'API timeout'),
  ('retry_${job_run_id}_2', '${job_run_id}', $((confirmed_to + 1)), 2, 'FAILED', 'API timeout'),
  ('retry_${job_run_id}_3', '${job_run_id}', $((confirmed_to + 1)), 3, 'FAILED', 'API timeout');
"
  fi
else
  # Deterministic replay: consume the original event again and repair only rows
  # still marked MISSING. Confirmed pages are not reloaded.
  missing_before="$(pg_json "
SELECT COALESCE(json_agg(page_no ORDER BY page_no)::text, '[]')
FROM pipeline_page_downloads
WHERE job_run_id='${job_run_id}' AND status='MISSING';")"

  pg_exec_sql "
BEGIN;
UPDATE pipeline_page_downloads
SET status='CONFIRMED', records_loaded=100, last_error=NULL, updated_at=now()
WHERE job_run_id='${job_run_id}' AND status='MISSING';
INSERT INTO pipeline_replay_history(replay_id, job_run_id, original_offset, replay_number, pages_recovered)
VALUES ('replay_${job_run_id}_${REPLAY_NUMBER}', '${job_run_id}', ${offset}, ${REPLAY_NUMBER},
  '${missing_before}'::jsonb);
UPDATE pipeline_recovery_runs SET status='REPLAY_RECOVERED', updated_at=now()
WHERE job_run_id='${job_run_id}';
COMMIT;
"
fi

confirmed_pages="$(pg_query_scalar "
SELECT COUNT(*) FROM pipeline_page_downloads
WHERE job_run_id='${job_run_id}' AND status='CONFIRMED';")"
missing_pages="$(pg_json "
SELECT COALESCE(json_agg(page_no ORDER BY page_no)::text, '[]')
FROM pipeline_page_downloads
WHERE job_run_id='${job_run_id}' AND status='MISSING';")"
records_loaded=$((confirmed_pages * 100))
records_missing=$(((expected_pages - confirmed_pages) * 100))

audit_status="COMPLETE"
if (( confirmed_pages < expected_pages )); then audit_status="PARTIAL"; fi

pg_exec_sql "
BEGIN;
INSERT INTO pipeline_processing_audits(audit_id, job_run_id, event_offset, replay_number,
  confirmed_pages, missing_pages, records_loaded, records_missing_estimate, audit_status)
VALUES ('audit_${job_run_id}_${REPLAY_NUMBER}', '${job_run_id}', ${offset}, ${REPLAY_NUMBER},
  ${confirmed_pages}, '${missing_pages}'::jsonb, ${records_loaded}, ${records_missing}, '${audit_status}');
UPDATE pipeline_recovery_runs SET status='${audit_status}', updated_at=now()
WHERE job_run_id='${job_run_id}';
COMMIT;
"

commit_payload="$(jq -cn --arg topic "$TOPIC" --arg group "$GROUP" \
  --argjson partition "$PARTITION" --argjson offset "$offset" \
  '{topic:$topic,group:$group,partition:$partition,offset:$offset}')"
curl -fsS -X POST "${AYDER_BASE}/broker/commit" \
  -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$commit_payload" >/dev/null

pg_exec_sql "
UPDATE pipeline_processing_audits
SET offset_committed_at=now()
WHERE audit_id='audit_${job_run_id}_${REPLAY_NUMBER}';
"

log "worker replay=${REPLAY_NUMBER} confirmed=${confirmed_pages} missing=${missing_pages} audit_then_commit=true"
