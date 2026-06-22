CREATE TABLE IF NOT EXISTS accounts (
  account_id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  balance BIGINT NOT NULL,
  currency TEXT NOT NULL DEFAULT 'USD'
);

CREATE TABLE IF NOT EXISTS ledger_journals (
  journal_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ledger_entries (
  entry_id TEXT PRIMARY KEY,
  journal_id TEXT NOT NULL REFERENCES ledger_journals(journal_id) ON DELETE CASCADE,
  account_id TEXT NOT NULL REFERENCES accounts(account_id),
  direction TEXT NOT NULL,
  amount BIGINT NOT NULL,
  currency TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bank_statement_entries (
  bank_entry_id TEXT PRIMARY KEY,
  provider_transaction_id TEXT,
  amount BIGINT,
  currency TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reconciliation_results (
  reconciliation_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL,
  ledger_amount BIGINT NOT NULL,
  bank_amount BIGINT,
  match_status TEXT NOT NULL,
  discrepancy_reason TEXT,
  recommended_action TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ledger_clearance_receipts (
  receipt_id TEXT PRIMARY KEY,
  verdict TEXT NOT NULL,
  operator_answer TEXT NOT NULL,
  ledger_integrity JSONB NOT NULL,
  clearance JSONB NOT NULL,
  external_reconciliation JSONB NOT NULL,
  business_rules JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ledger_entries_journal
  ON ledger_entries(journal_id);

CREATE INDEX IF NOT EXISTS idx_reconciliation_attempt
  ON reconciliation_results(attempt_id);
