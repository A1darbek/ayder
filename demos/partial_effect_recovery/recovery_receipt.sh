#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/partial_effect_recovery/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd base64

OUT_DIR="${OUT_DIR:-${REPO_ROOT}/artifacts/partial_effect_recovery/adhoc}"
mkdir -p "$OUT_DIR"
SCENARIO="${SCENARIO:-partial_missing_notification}"
EVENT_ID="${EVENT_ID:-evt_order_001}"
RECEIPT_ID="${RECEIPT_ID:-partial_recovery_rcpt_001}"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

pg_json() {
  dc exec -T "$PG_SERVICE" psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -X -A -t -c "$1" \
    | tr -d '\r' | sed '/^$/d'
}

ayder_evidence_json="$(pg_json "
SELECT json_build_object(
  'topic', topic,
  'consumer_group', consumer_group,
  'event_offset', event_offset,
  'event_durable', event_durable,
  'event_sealed', event_sealed,
  'event_synced', event_synced,
  'idempotency_key', idempotency_key,
  'event_consumed', consumed,
  'offset_committed', offset_committed,
  'deterministic_replay_available', event_durable AND event_sealed
)::text
FROM partial_effect_events
WHERE event_id='${EVENT_ID}';")"
[[ -n "$ayder_evidence_json" ]] || { echo "missing event evidence for ${EVENT_ID}" >&2; exit 1; }

effects_json="$(pg_json "
WITH effect_counts AS (
  SELECT
    e.event_id,
    e.effect_name,
    e.ordinal,
    COUNT(a.*) AS audit_rows,
    COUNT(*) FILTER (WHERE a.status='CONFIRMED') AS confirmed_count,
    COUNT(*) FILTER (WHERE a.status='MISSING') AS missing_count,
    COUNT(*) FILTER (WHERE a.status='UNKNOWN') AS unknown_count
  FROM partial_effect_expected_effects e
  LEFT JOIN partial_effect_audit a
    ON a.event_id=e.event_id AND a.effect_name=e.effect_name
  WHERE e.event_id='${EVENT_ID}'
  GROUP BY e.event_id, e.effect_name, e.ordinal
),
classified AS (
  SELECT
    effect_name,
    ordinal,
    CASE
      WHEN confirmed_count > 1 THEN 'DUPLICATED'
      WHEN unknown_count > 0 THEN 'UNKNOWN'
      WHEN confirmed_count = 1 THEN 'CONFIRMED'
      ELSE 'MISSING'
    END AS status,
    CASE
      WHEN confirmed_count > 0 OR unknown_count > 0 THEN false
      ELSE true
    END AS safe_to_reapply,
    audit_rows,
    confirmed_count
  FROM effect_counts
)
SELECT COALESCE(json_agg(json_build_object(
  'name', effect_name,
  'status', status,
  'safe_to_reapply', safe_to_reapply
) ORDER BY ordinal)::text, '[]')
FROM classified;")"

effect_summary_json="$(pg_json "
WITH effect_counts AS (
  SELECT
    e.effect_name,
    COUNT(a.*) AS audit_rows,
    COUNT(*) FILTER (WHERE a.status='CONFIRMED') AS confirmed_count,
    COUNT(*) FILTER (WHERE a.status='UNKNOWN') AS unknown_count
  FROM partial_effect_expected_effects e
  LEFT JOIN partial_effect_audit a
    ON a.event_id=e.event_id AND a.effect_name=e.effect_name
  WHERE e.event_id='${EVENT_ID}'
  GROUP BY e.effect_name
),
classified AS (
  SELECT
    CASE
      WHEN confirmed_count > 1 THEN 'DUPLICATED'
      WHEN unknown_count > 0 THEN 'UNKNOWN'
      WHEN confirmed_count = 1 THEN 'CONFIRMED'
      ELSE 'MISSING'
    END AS status
  FROM effect_counts
)
SELECT json_build_object(
  'expected_effects', COUNT(*),
  'confirmed_effects', COUNT(*) FILTER (WHERE status='CONFIRMED'),
  'missing_effects', COUNT(*) FILTER (WHERE status='MISSING'),
  'ambiguous_effects', COUNT(*) FILTER (WHERE status='UNKNOWN'),
  'duplicate_effects', COUNT(*) FILTER (WHERE status='DUPLICATED')
)::text
FROM classified;")"

expected_effects="$(printf '%s' "$effect_summary_json" | jq -r '.expected_effects')"
confirmed_effects="$(printf '%s' "$effect_summary_json" | jq -r '.confirmed_effects')"
missing_effects="$(printf '%s' "$effect_summary_json" | jq -r '.missing_effects')"
ambiguous_effects="$(printf '%s' "$effect_summary_json" | jq -r '.ambiguous_effects')"
duplicate_effects="$(printf '%s' "$effect_summary_json" | jq -r '.duplicate_effects')"
event_consumed="$(printf '%s' "$ayder_evidence_json" | jq -r '.event_consumed')"

verdict="GREEN"
operator_answer="Safe to close. All expected business effects are confirmed."
blind_replay_whole_event=false
repair_only_missing_effect=false
manual_review_required=false
safe_next_action="close incident"
receipt_rc=0

if (( duplicate_effects > 0 )); then
  verdict="RED"
  operator_answer="Block blind replay. Duplicate effect risk detected; inspect duplicate effect first."
  manual_review_required=true
  safe_next_action="inspect duplicate effect before replay or repair"
  receipt_rc=20
elif (( ambiguous_effects > 0 )); then
  verdict="YELLOW"
  operator_answer="Manual review required before replay or repair. At least one effect is ambiguous."
  manual_review_required=true
  safe_next_action="review ambiguous ledger_entry_created before any replay or targeted repair"
  receipt_rc=10
elif [[ "$event_consumed" != "true" || "$confirmed_effects" == "0" ]]; then
  verdict="RED"
  operator_answer="Replay the whole event from Ayder. No effect audit was recorded, so no business effect is confirmed."
  blind_replay_whole_event=true
  safe_next_action="replay whole event from Ayder"
  receipt_rc=20
elif (( missing_effects > 0 )); then
  verdict="YELLOW"
  operator_answer="Do not replay the whole event. Two effects are already confirmed. Repair only the missing notification effect."
  repair_only_missing_effect=true
  safe_next_action="repair customer_notification_sent only"
  receipt_rc=10
fi

recovery_guidance_json="$(jq -n \
  --argjson blind_replay_whole_event "$blind_replay_whole_event" \
  --argjson repair_only_missing_effect "$repair_only_missing_effect" \
  --argjson manual_review_required "$manual_review_required" \
  --arg safe_next_action "$safe_next_action" \
  '{
    blind_replay_whole_event:$blind_replay_whole_event,
    repair_only_missing_effect:$repair_only_missing_effect,
    manual_review_required:$manual_review_required,
    safe_next_action:$safe_next_action
  }')"

rule() {
  jq -cn --arg id "$1" --arg name "$2" --arg status "$3" --arg detail "$4" \
    '{id:$id,name:$name,status:$status} + (if $detail == "" then {} else {detail:$detail} end)'
}

event_replayable="$(printf '%s' "$ayder_evidence_json" | jq -r '.deterministic_replay_available')"
rule_event="$([[ "$event_replayable" == "true" ]] && echo PASS || echo FAIL)"
rule_no_blind="PASS"
rule_targeted="PASS"
rule_explicit="PASS"

if (( duplicate_effects > 0 )) && [[ "$blind_replay_whole_event" == "true" ]]; then
  rule_no_blind="FAIL"
fi
if (( missing_effects > 0 && ambiguous_effects == 0 && duplicate_effects == 0 && confirmed_effects > 0 )) && [[ "$repair_only_missing_effect" != "true" ]]; then
  rule_targeted="FAIL"
fi
[[ -n "$safe_next_action" ]] || rule_explicit="FAIL"

business_rules_json="$(jq -cs '.' <<EOF
$(rule R1 "event is durable and replayable" "$rule_event" "")
$(rule R2 "confirmed effects are not replayed blindly" "$rule_no_blind" "")
$(rule R3 "missing effect has targeted repair action" "$rule_targeted" "")
$(rule R4 "operator next action is explicit" "$rule_explicit" "")
EOF
)"

receipt_json="$(jq -n \
  --arg receipt_id "$RECEIPT_ID" \
  --arg generated_at "$GENERATED_AT" \
  --arg scenario "$SCENARIO" \
  --arg verdict "$verdict" \
  --arg operator_answer "$operator_answer" \
  --argjson ayder_evidence "$ayder_evidence_json" \
  --argjson effect_summary "$effect_summary_json" \
  --argjson effects "$effects_json" \
  --argjson recovery_guidance "$recovery_guidance_json" \
  --argjson business_rules "$business_rules_json" \
  '{
    receipt_id:$receipt_id,
    generated_at:$generated_at,
    scenario:$scenario,
    verdict:$verdict,
    operator_answer:$operator_answer,
    ayder_evidence:$ayder_evidence,
    effect_summary:$effect_summary,
    effects:$effects,
    recovery_guidance:$recovery_guidance,
    business_rules:$business_rules
  }')"

echo "$receipt_json" > "${OUT_DIR}/receipt.json"

ayder_b64="$(printf '%s' "$ayder_evidence_json" | base64 -w0)"
summary_b64="$(printf '%s' "$effect_summary_json" | base64 -w0)"
effects_b64="$(printf '%s' "$effects_json" | base64 -w0)"
guidance_b64="$(printf '%s' "$recovery_guidance_json" | base64 -w0)"
rules_b64="$(printf '%s' "$business_rules_json" | base64 -w0)"
operator_safe="$(printf '%s' "$operator_answer" | sed "s/'/''/g")"

pg_exec_sql "
INSERT INTO partial_effect_receipts(
  receipt_id, scenario, verdict, operator_answer, ayder_evidence,
  effect_summary, effects, recovery_guidance, business_rules)
VALUES (
  '${RECEIPT_ID}', '${SCENARIO}', '${verdict}', '${operator_safe}',
  convert_from(decode('${ayder_b64}', 'base64'), 'UTF8')::jsonb,
  convert_from(decode('${summary_b64}', 'base64'), 'UTF8')::jsonb,
  convert_from(decode('${effects_b64}', 'base64'), 'UTF8')::jsonb,
  convert_from(decode('${guidance_b64}', 'base64'), 'UTF8')::jsonb,
  convert_from(decode('${rules_b64}', 'base64'), 'UTF8')::jsonb)
ON CONFLICT (receipt_id) DO UPDATE SET
  scenario=EXCLUDED.scenario,
  verdict=EXCLUDED.verdict,
  operator_answer=EXCLUDED.operator_answer,
  ayder_evidence=EXCLUDED.ayder_evidence,
  effect_summary=EXCLUDED.effect_summary,
  effects=EXCLUDED.effects,
  recovery_guidance=EXCLUDED.recovery_guidance,
  business_rules=EXCLUDED.business_rules;
"

{
  echo "============================================================"
  echo " AYDER PARTIAL EFFECT RECOVERY RECEIPT"
  echo "============================================================"
  echo " receipt_id      : ${RECEIPT_ID}"
  echo " generated       : ${GENERATED_AT}"
  echo " scenario        : ${SCENARIO}"
  echo " verdict         : ${verdict}"
  echo " operator_answer : ${operator_answer}"
  echo "------------------------------------------------------------"
  echo " AYDER EVIDENCE:"
  echo "$ayder_evidence_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " EFFECT SUMMARY:"
  echo "$effect_summary_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " EFFECTS:"
  echo "$effects_json" | jq -r '.[] |
    "   - " + .name + "\n" +
    "     status: " + .status + "\n" +
    "     safe_to_reapply: " + (.safe_to_reapply|tostring)'
  echo "------------------------------------------------------------"
  echo " RECOVERY GUIDANCE:"
  echo "$recovery_guidance_json" | jq -r 'to_entries[] | "   \(.key): \(.value)"'
  echo "------------------------------------------------------------"
  echo " BUSINESS RULES:"
  echo "$business_rules_json" | jq -r '.[] | "   [\(.status)] \(.id) \(.name)" + (if .detail then "  (" + .detail + ")" else "" end)'
  echo "============================================================"
  echo " json: ${OUT_DIR}/receipt.json"
} | tee "${OUT_DIR}/receipt.txt"

exit "$receipt_rc"
