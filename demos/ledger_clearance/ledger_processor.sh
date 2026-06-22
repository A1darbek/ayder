#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/ledger_clearance/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd jq
require_cmd curl
require_cmd base64

consume_url="${AYDER_BASE}/broker/consume/${TOPIC}/${GROUP}/${PARTITION}?offset=0&limit=10&encoding=b64"
raw="$(curl -sS -w $'\n%{http_code}' -H "$AUTH_HEADER" "$consume_url")"
http_code="${raw##*$'\n'}"
body="${raw%$'\n'*}"
[[ "$http_code" == "200" ]] || { echo "consume failed http=${http_code}: ${body}" >&2; exit 1; }

count="$(echo "$body" | jq -r '.count // 0')"
[[ "$count" == "1" ]] || { echo "expected 1 ledger event from Ayder, got ${count}" >&2; exit 1; }

value_b64="$(echo "$body" | jq -r '.messages[0].value_b64')"
payload_json="$(printf '%s' "$value_b64" | base64 -d)"
attempt_id="$(printf '%s' "$payload_json" | jq -r '.attempt_id')"
amount="$(printf '%s' "$payload_json" | jq -r '.amount')"
currency="$(printf '%s' "$payload_json" | jq -r '.currency')"

pg_exec_sql "
BEGIN;
INSERT INTO ledger_journals(journal_id, attempt_id, status)
VALUES ('journal_${attempt_id}', '${attempt_id}', 'COMMITTED')
ON CONFLICT (journal_id) DO UPDATE SET status=EXCLUDED.status;

INSERT INTO ledger_entries(entry_id, journal_id, account_id, direction, amount, currency)
VALUES
  ('entry_${attempt_id}_credit_available', 'journal_${attempt_id}', 'user_available', 'CREDIT', ${amount}, '${currency}'),
  ('entry_${attempt_id}_debit_clearance', 'journal_${attempt_id}', 'clearance_account', 'DEBIT', ${amount}, '${currency}')
ON CONFLICT (entry_id) DO NOTHING;

UPDATE accounts SET balance=80 WHERE account_id='user_available';
UPDATE accounts SET balance=20 WHERE account_id='clearance_account';

INSERT INTO reconciliation_results(reconciliation_id, attempt_id, ledger_amount, bank_amount,
  match_status, discrepancy_reason, recommended_action)
VALUES ('recon_${attempt_id}', '${attempt_id}', ${amount}, NULL, 'MISSING',
  'provider_confirmation_missing',
  'Keep funds in clearance and send to manual reconciliation.')
ON CONFLICT (reconciliation_id) DO UPDATE
SET match_status=EXCLUDED.match_status,
    discrepancy_reason=EXCLUDED.discrepancy_reason,
    recommended_action=EXCLUDED.recommended_action;
COMMIT;
"

payload="$(jq -cn --arg topic "$TOPIC" --arg group "$GROUP" --argjson partition "$PARTITION" --argjson offset 0 \
  '{topic:$topic,group:$group,partition:$partition,offset:$offset}')"
curl -fsS -X POST "${AYDER_BASE}/broker/commit" \
  -H "$AUTH_HEADER" -H 'Content-Type: application/json' --data-binary "$payload" >/dev/null

log "posted atomic ledger journal and recorded unresolved external reconciliation"
