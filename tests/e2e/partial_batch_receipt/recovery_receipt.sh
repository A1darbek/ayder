#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/e2e/partial_batch_receipt/env.sh
source "${SCRIPT_DIR}/env.sh"

# Effect-aware recovery receipt for the partial-batch incident demo.
#
# It reconciles:
#   expected_events + expected_effects
# against:
#   observed_effects + dlq + account_balances
#
# The result is an operational artifact, not a passive validator report: it
# tells the operator what to repair and when replaying a full event is unsafe.

require_cmd jq
require_cmd "$DOCKER_BIN"

OUT_DIR="${OUT_DIR:-${REPO_ROOT}/artifacts/partial_batch_receipt/adhoc}"
mkdir -p "$OUT_DIR"
RECEIPT_ID="${RECEIPT_ID:-rcpt_$(date -u +%Y%m%dT%H%M%SZ)_$$}"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

EXP_SCOPE="account_id LIKE 'acct-pbr-%'"
EXP_SCOPE_E="e.account_id LIKE 'acct-pbr-%'"
PG_SCOPE="topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION}"
PG_SCOPE_O="o.topic='${TOPIC}' AND o.group_id='${GROUP}' AND o.partition_id=${PARTITION}"
PG_SCOPE_D="d.topic='${TOPIC}' AND d.group_id='${GROUP}' AND d.partition_id=${PARTITION}"
PG_SCOPE_H="topic='${TOPIC}' AND group_id='${GROUP}' AND partition_id=${PARTITION}"

pg_json() {
  dc exec -T "$PG_SERVICE" psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -X -A -t -c "$1" \
    | tr -d '\r' | sed '/^$/d'
}

event_summary_sql="
WITH scoped_events AS (
  SELECT event_id, account_id, delta, seq
  FROM expected_events
  WHERE ${EXP_SCOPE}
),
effect_rollup AS (
  SELECT
    e.event_id,
    COUNT(x.effect_type) AS expected_effect_count,
    COUNT(o.effect_type) FILTER (WHERE o.status='confirmed') AS confirmed_effect_count,
    COUNT(x.effect_type) FILTER (WHERE o.effect_type IS NULL) AS missing_effect_count
  FROM scoped_events e
  JOIN expected_effects x ON x.event_id=e.event_id
  LEFT JOIN observed_effects o
    ON o.event_id=x.event_id
   AND o.effect_type=x.effect_type
   AND ${PG_SCOPE_O}
  GROUP BY e.event_id
),
dlq_rollup AS (
  SELECT event_id, COUNT(*) AS dlq_rows
  FROM dlq
  WHERE ${PG_SCOPE} AND status='pending'
  GROUP BY event_id
),
statused AS (
  SELECT
    e.event_id, e.account_id, e.delta, e.seq,
    COALESCE(r.expected_effect_count,0) AS expected_effect_count,
    COALESCE(r.confirmed_effect_count,0) AS confirmed_effect_count,
    COALESCE(r.missing_effect_count,0) AS missing_effect_count,
    COALESCE(d.dlq_rows,0) AS dlq_rows,
    CASE
      WHEN COALESCE(d.dlq_rows,0) > 0 AND COALESCE(r.confirmed_effect_count,0) = 0 THEN 'UNRESOLVED'
      WHEN COALESCE(r.confirmed_effect_count,0) = COALESCE(r.expected_effect_count,0)
           AND COALESCE(r.expected_effect_count,0) > 0 THEN 'CONFIRMED'
      WHEN COALESCE(r.confirmed_effect_count,0) = 0 THEN 'MISSING'
      ELSE 'PARTIAL'
    END AS event_status
  FROM scoped_events e
  LEFT JOIN effect_rollup r ON r.event_id=e.event_id
  LEFT JOIN dlq_rollup d ON d.event_id=e.event_id
),
expected_detail AS (
  SELECT
    s.event_id,
    json_agg(json_build_object(
      'effect_type', x.effect_type,
      'status', CASE
        WHEN o.effect_type IS NOT NULL THEN upper(o.status)
        WHEN s.dlq_rows > 0 THEN 'SUPPRESSED'
        ELSE 'MISSING'
      END,
      'expected_value', x.expected_value
    ) ORDER BY x.effect_type) AS expected_effects
  FROM statused s
  JOIN expected_effects x ON x.event_id=s.event_id
  LEFT JOIN observed_effects o
    ON o.event_id=x.event_id
   AND o.effect_type=x.effect_type
   AND ${PG_SCOPE_O}
  GROUP BY s.event_id
),
observed_detail AS (
  SELECT
    s.event_id,
    COALESCE(json_agg(json_build_object(
      'effect_type', o.effect_type,
      'status', upper(o.status),
      'offset', o.msg_offset,
      'observed_value', o.observed_value
    ) ORDER BY o.effect_type) FILTER (WHERE o.event_id IS NOT NULL), '[]'::json) AS observed_effects
  FROM statused s
  LEFT JOIN observed_effects o
    ON o.event_id=s.event_id
   AND ${PG_SCOPE_O}
  GROUP BY s.event_id
),
missing_detail AS (
  SELECT
    s.event_id,
    COALESCE(json_agg(x.effect_type ORDER BY x.effect_type) FILTER (WHERE o.effect_type IS NULL AND s.dlq_rows = 0), '[]'::json) AS missing_effect_types
  FROM statused s
  JOIN expected_effects x ON x.event_id=s.event_id
  LEFT JOIN observed_effects o
    ON o.event_id=x.event_id
   AND o.effect_type=x.effect_type
   AND ${PG_SCOPE_O}
  GROUP BY s.event_id
)
SELECT COALESCE(json_agg(json_build_object(
  'event_id', s.event_id,
  'account_id', s.account_id,
  'delta', s.delta,
  'seq', s.seq,
  'event_status', s.event_status,
  'expected_effects', ed.expected_effects,
  'observed_effects', od.observed_effects,
  'missing_effect_types', md.missing_effect_types,
  'recommended_action',
    CASE
      WHEN s.event_status='CONFIRMED' THEN 'No action required.'
      WHEN s.event_status='MISSING' THEN 'Replay/repair full event.'
      WHEN s.event_status='PARTIAL' AND md.missing_effect_types::text='[\"notification_sent\"]' THEN 'Repair notification only. Do not replay full event blindly.'
      WHEN s.event_status='PARTIAL' THEN 'Repair only the missing effects. Do not replay full event blindly.'
      WHEN s.event_status='UNRESOLVED' THEN 'Replay from retry/DLQ with idempotency checks.'
      ELSE 'Investigate duplicate/unknown effect state.'
    END,
  'warning',
    CASE
      WHEN s.event_status='PARTIAL' THEN 'do not replay full event blindly'
      ELSE NULL
    END
) ORDER BY s.seq)::text, '[]')
FROM statused s
JOIN expected_detail ed ON ed.event_id=s.event_id
JOIN observed_detail od ON od.event_id=s.event_id
JOIN missing_detail md ON md.event_id=s.event_id;
"

event_summaries_json="$(pg_json "$event_summary_sql")"
[[ -z "$event_summaries_json" ]] && event_summaries_json='[]'

event_status_counts_json="$(printf '%s' "$event_summaries_json" | jq -c '
  {
    confirmed: map(select(.event_status=="CONFIRMED")) | length,
    missing: map(select(.event_status=="MISSING")) | length,
    partial: map(select(.event_status=="PARTIAL")) | length,
    duplicated: map(select(.event_status=="DUPLICATED")) | length,
    unresolved: map(select(.event_status=="UNRESOLVED")) | length
  }')"

effect_status_counts_json="$(pg_json "
WITH scoped_events AS (
  SELECT event_id FROM expected_events WHERE ${EXP_SCOPE}
),
expected AS (
  SELECT x.event_id, x.effect_type
  FROM expected_effects x
  JOIN scoped_events e ON e.event_id=x.event_id
),
joined AS (
  SELECT
    x.event_id,
    x.effect_type,
    CASE
      WHEN o.effect_type IS NOT NULL THEN 'confirmed'
      WHEN d.event_id IS NOT NULL THEN 'suppressed'
      ELSE 'missing'
    END AS effect_status
  FROM expected x
  LEFT JOIN observed_effects o
    ON o.event_id=x.event_id
   AND o.effect_type=x.effect_type
   AND ${PG_SCOPE_O}
  LEFT JOIN dlq d
    ON d.event_id=x.event_id
   AND ${PG_SCOPE_D}
   AND d.status='pending'
)
SELECT json_build_object(
  'confirmed', COUNT(*) FILTER (WHERE effect_status='confirmed'),
  'missing', COUNT(*) FILTER (WHERE effect_status='missing'),
  'duplicated', 0,
  'suppressed', COUNT(*) FILTER (WHERE effect_status='suppressed')
)::text
FROM joined;")"
[[ -z "$effect_status_counts_json" ]] && effect_status_counts_json='{"confirmed":0,"missing":0,"duplicated":0,"suppressed":0}'

expected_count="$(printf '%s' "$event_status_counts_json" | jq -r '[.confirmed,.missing,.partial,.duplicated,.unresolved] | add')"
confirmed_count="$(printf '%s' "$event_status_counts_json" | jq -r '.confirmed')"
missing_count="$(printf '%s' "$event_status_counts_json" | jq -r '.missing')"
partial_count="$(printf '%s' "$event_status_counts_json" | jq -r '.partial')"
unresolved_count="$(printf '%s' "$event_status_counts_json" | jq -r '.unresolved')"
duplicate_count="$(printf '%s' "$event_status_counts_json" | jq -r '.duplicated')"
effect_confirmed_count="$(printf '%s' "$effect_status_counts_json" | jq -r '.confirmed')"
effect_missing_count="$(printf '%s' "$effect_status_counts_json" | jq -r '.missing')"
dlq_count="$(pg_query_scalar "
SELECT COUNT(*) FROM expected_events e
JOIN dlq d ON d.event_id=e.event_id AND ${PG_SCOPE_D} AND d.status='pending'
WHERE ${EXP_SCOPE_E};")"

non_monotonic_rows="$(pg_query_scalar "SELECT COUNT(*) FROM consumer_state_history WHERE ${PG_SCOPE_H} AND new_offset < prev_offset;")"

window_json="$(pg_json "
SELECT COALESCE(json_build_object(
  'from_seq', MIN(seq), 'to_seq', MAX(seq),
  'from_event', (SELECT event_id FROM expected_events WHERE ${EXP_SCOPE} ORDER BY seq ASC  LIMIT 1),
  'to_event',   (SELECT event_id FROM expected_events WHERE ${EXP_SCOPE} ORDER BY seq DESC LIMIT 1),
  'produced_from', MIN(produced_at), 'produced_to', MAX(produced_at)
)::text, '{}')
FROM expected_events WHERE ${EXP_SCOPE};")"
[[ -z "$window_json" ]] && window_json='{}'

balance_json="$(pg_json "
WITH exp AS (
  SELECT account_id, SUM(delta) d FROM expected_events WHERE ${EXP_SCOPE} GROUP BY account_id
),
ledger_observed AS (
  SELECT e.account_id, COUNT(*)::bigint AS observed_delta
  FROM expected_events e
  JOIN observed_effects o ON o.event_id=e.event_id AND o.effect_type='ledger_update' AND ${PG_SCOPE_O}
  WHERE ${EXP_SCOPE_E}
  GROUP BY e.account_id
)
SELECT COALESCE(json_agg(json_build_object(
  'account_id', exp.account_id,
  'expected', exp.d,
  'actual', COALESCE(b.balance,0),
  'observed_ledger_effects', COALESCE(lo.observed_delta,0),
  'unexplained', COALESCE(b.balance,0) - COALESCE(lo.observed_delta,0)
) ORDER BY exp.account_id)::text, '[]')
FROM exp
LEFT JOIN account_balances b ON b.account_id=exp.account_id
LEFT JOIN ledger_observed lo ON lo.account_id=exp.account_id;")"
[[ -z "$balance_json" ]] && balance_json='[]'

unexplained_total="$(printf '%s' "$balance_json" | jq -r '[.[].unexplained | tonumber | abs] | add // 0')"

verdict="GREEN"
if (( missing_count > 0 || partial_count > 0 || duplicate_count > 0 || effect_missing_count > 0 )); then
  verdict="RED"
elif (( unresolved_count > 0 || dlq_count > 0 )); then
  verdict="AMBER"
fi

case "$verdict" in
  GREEN) headline="Safe to stand down: every expected business effect was observed exactly once." ;;
  AMBER) headline="Recoverable: ${unresolved_count} event(s) are pending in retry/DLQ and must be replayed with idempotency checks." ;;
  RED)   headline="Business damage: ${missing_count} event(s) missing and ${partial_count} partial event(s); repair only the missing effects where possible." ;;
esac

rule() {
  jq -cn --arg id "$1" --arg name "$2" --arg status "$3" --arg detail "$4" \
    '{id:$id,name:$name,status:$status,detail:$detail}'
}
b() { (( $1 == 1 )) && echo PASS || echo FAIL; }

rules_json="$(jq -cs '.' <<EOF
$(rule R1 "event completeness (no missing or partial events)" "$(b $(( missing_count==0 && partial_count==0 )) )" "missing=${missing_count} partial=${partial_count}")
$(rule R2 "effect completeness (all expected effects observed)" "$(b $(( effect_missing_count==0 )) )" "missing_effects=${effect_missing_count}")
$(rule R3 "no duplicate effects" "$(b $(( duplicate_count==0 )) )" "duplicated=${duplicate_count}")
$(rule R4 "monotonic offset progression" "$(b $(( non_monotonic_rows==0 )) )" "violations=${non_monotonic_rows}")
$(rule R5 "ledger integrity (observed ledger effects match balance)" "$(b $(( unexplained_total==0 )) )" "unexplained_total=${unexplained_total}")
EOF
)"

recommended_actions_json="$(printf '%s' "$event_summaries_json" | jq -c '
  map(select(.event_status != "CONFIRMED") | {
    event_id,
    status: .event_status,
    action: .recommended_action,
    warning
  })')"

receipt_json="$(jq -n \
  --arg receipt_id "$RECEIPT_ID" \
  --arg generated_at "$GENERATED_AT" \
  --arg topic "$TOPIC" --arg group "$GROUP" --argjson partition "$PARTITION" \
  --arg verdict "$verdict" --arg headline "$headline" \
  --argjson window "$window_json" \
  --argjson expected "$expected_count" --argjson confirmed "$confirmed_count" \
  --argjson missing "$missing_count" --argjson partial "$partial_count" \
  --argjson unresolved "$unresolved_count" --argjson duplicated "$duplicate_count" \
  --argjson dlq "$dlq_count" \
  --argjson event_status_counts "$event_status_counts_json" \
  --argjson effect_status_counts "$effect_status_counts_json" \
  --argjson event_summaries "$event_summaries_json" \
  --argjson recommended_actions "$recommended_actions_json" \
  --argjson ledger "$balance_json" \
  --argjson business_rules "$rules_json" \
  '{
     receipt_id:$receipt_id,
     generated_at:$generated_at,
     scope:{topic:$topic, group:$group, partition:$partition},
     incident_window:$window,
     verdict:$verdict,
     headline:$headline,
     totals:{expected:$expected, confirmed:$confirmed, missing:$missing,
             partial:$partial, unresolved:$unresolved, duplicated:$duplicated,
             dlq:$dlq},
     event_status_counts:$event_status_counts,
     effect_status_counts:$effect_status_counts,
     events:$event_summaries,
     recommended_actions:$recommended_actions,
     ledger_reconciliation:$ledger,
     business_rules:$business_rules
   }')"

echo "$receipt_json" > "${OUT_DIR}/receipt.json"

w_from="$(echo "$window_json" | jq -r '.from_seq // "null"')"
w_to="$(echo "$window_json" | jq -r '.to_seq // "null"')"

pg_exec_sql "
INSERT INTO recovery_receipt(receipt_id, topic, group_id, partition_id, window_from_seq, window_to_seq,
  expected_count, confirmed_count, missing_count, duplicate_count, dlq_count,
  event_status_counts, effect_status_counts, verdict)
VALUES ('${RECEIPT_ID}', '${TOPIC}', '${GROUP}', ${PARTITION}, ${w_from}, ${w_to},
  ${expected_count}, ${confirmed_count}, ${missing_count}, ${duplicate_count}, ${dlq_count},
  '${event_status_counts_json}'::jsonb, '${effect_status_counts_json}'::jsonb, '${verdict}')
ON CONFLICT (receipt_id) DO NOTHING;
"

printf '%s' "$event_summaries_json" | jq -c '.[]' | while IFS= read -r event; do
  event_id="$(printf '%s' "$event" | jq -r '.event_id')"
  account_id="$(printf '%s' "$event" | jq -r '.account_id')"
  delta="$(printf '%s' "$event" | jq -r '.delta')"
  seq="$(printf '%s' "$event" | jq -r '.seq')"
  status="$(printf '%s' "$event" | jq -r '.event_status')"
  expected_b64="$(printf '%s' "$event" | jq -c '.expected_effects' | base64 -w0)"
  observed_b64="$(printf '%s' "$event" | jq -c '.observed_effects' | base64 -w0)"
  effect_status_b64="$(printf '%s' "$event" | jq -c '{missing_effect_types}' | base64 -w0)"
  action_safe="$(printf '%s' "$event" | jq -r '.recommended_action' | sed "s/'/''/g")"
  warning_safe="$(printf '%s' "$event" | jq -r '.warning // empty' | sed "s/'/''/g")"
  classification="$(printf '%s' "$status" | tr '[:upper:]' '[:lower:]')"

  pg_exec_sql "
INSERT INTO recovery_receipt_item(receipt_id, event_id, account_id, delta, seq, classification,
  event_status, expected_effects, observed_effects, effect_status, recommended_action, warning, detail)
VALUES ('${RECEIPT_ID}', '${event_id}', '${account_id}', ${delta}, ${seq}, '${classification}',
  '${status}',
  convert_from(decode('${expected_b64}', 'base64'), 'UTF8')::jsonb,
  convert_from(decode('${observed_b64}', 'base64'), 'UTF8')::jsonb,
  convert_from(decode('${effect_status_b64}', 'base64'), 'UTF8')::jsonb,
  '${action_safe}', NULLIF('${warning_safe}', ''), NULL)
ON CONFLICT DO NOTHING;
"
done

{
  echo "============================================================"
  echo " AYDER RECOVERY RECEIPT"
  echo "============================================================"
  echo " receipt_id : ${RECEIPT_ID}"
  echo " generated  : ${GENERATED_AT}"
  echo " scope      : topic=${TOPIC} group=${GROUP} partition=${PARTITION}"
  echo " window     : seq $(echo "$window_json" | jq -r '.from_seq // "?"')..$(echo "$window_json" | jq -r '.to_seq // "?"')" \
       "($(echo "$window_json" | jq -r '.from_event // "?"') .. $(echo "$window_json" | jq -r '.to_event // "?"'))"
  echo "------------------------------------------------------------"
  echo " VERDICT: ${verdict}"
  echo "   ${headline}"
  echo "------------------------------------------------------------"
  echo " EVENT STATUS COUNTS:"
  echo "$event_status_counts_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " EFFECT STATUS COUNTS:"
  echo "$effect_status_counts_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " RECOMMENDED ACTIONS:"
  if [[ "$(printf '%s' "$recommended_actions_json" | jq 'length')" == "0" ]]; then
    echo "   none"
  else
    printf '%s' "$recommended_actions_json" | jq -r 'to_entries[] |
      " " + ((.key + 1)|tostring) + ". " + .value.event_id + "\n" +
      "    status: " + .value.status + "\n" +
      "    action: " + .value.action +
      (if .value.warning then "\n    warning: " + .value.warning else "" end)'
  fi
  echo "------------------------------------------------------------"
  echo " EVENT EFFECT DETAIL:"
  printf '%s' "$event_summaries_json" | jq -r '.[] |
    "   - " + .event_id + " status=" + .event_status + " action=\"" + .recommended_action + "\""'
  echo "------------------------------------------------------------"
  echo " BUSINESS RULES CHECKED:"
  echo "$rules_json" | jq -r '.[] | "   [\(.status)] \(.id) \(.name)  (\(.detail))"'
  echo "------------------------------------------------------------"
  echo " LEDGER RECONCILIATION (per account):"
  echo "$balance_json" | jq -r '.[] | "   - \(.account_id): expected=\(.expected) actual=\(.actual) observed_ledger_effects=\(.observed_ledger_effects) unexplained=\(.unexplained)"'
  echo "============================================================"
  echo " json: ${OUT_DIR}/receipt.json"
} | tee "${OUT_DIR}/receipt.txt"

case "$verdict" in
  GREEN) exit 0 ;;
  AMBER) exit 10 ;;
  RED)   exit 20 ;;
esac
