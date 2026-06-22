#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demos/ledger_clearance/env.sh
source "${SCRIPT_DIR}/env.sh"

require_cmd "$DOCKER_BIN"
require_cmd curl

if ! "$DOCKER_BIN" info >/dev/null 2>&1; then
  echo "docker daemon is not reachable from this environment" >&2
  exit 1
fi

log "starting postgres compose stack"
dc up -d "$PG_SERVICE"

log "waiting for postgres readiness"
wait_pg_ready 90 || { echo "postgres failed readiness" >&2; exit 1; }

log "applying ledger-clearance schema"
pg_exec_file "${SCRIPT_DIR}/schema.sql"

log "resetting ledger-clearance demo rows"
pg_exec_sql "
DELETE FROM ledger_clearance_receipts WHERE receipt_id LIKE 'ledger_rcpt_%';
DELETE FROM reconciliation_results WHERE attempt_id='pay_attempt_004';
DELETE FROM bank_statement_entries WHERE bank_entry_id LIKE 'bank_%ledger_clearance%';
DELETE FROM ledger_entries WHERE journal_id='journal_pay_attempt_004';
DELETE FROM ledger_journals WHERE journal_id='journal_pay_attempt_004';
DELETE FROM accounts WHERE account_id IN ('user_available','clearance_account','merchant_settlement','fee_account');

INSERT INTO accounts(account_id, type, balance, currency)
VALUES
  ('user_available', 'customer_available', 100, 'USD'),
  ('clearance_account', 'clearance', 0, 'USD'),
  ('merchant_settlement', 'merchant_settlement', 0, 'USD'),
  ('fee_account', 'fee', 0, 'USD');
"

log "ensuring ayder topic"
ensure_topic

log "init complete"
