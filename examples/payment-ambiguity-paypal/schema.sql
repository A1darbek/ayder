CREATE TABLE IF NOT EXISTS paypal_payment_attempts (
  attempt_id TEXT PRIMARY KEY,
  idempotency_key TEXT NOT NULL,
  provider TEXT NOT NULL DEFAULT 'paypal',
  provider_order_id TEXT,
  amount BIGINT NOT NULL,
  currency TEXT NOT NULL,
  internal_state TEXT NOT NULL,
  customer_visible_state TEXT NOT NULL,
  provider_commitment TEXT NOT NULL,
  safe_to_close BOOLEAN NOT NULL DEFAULT false,
  manual_reconciliation_required BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS paypal_ayder_evidence (
  id BIGSERIAL PRIMARY KEY,
  attempt_id TEXT NOT NULL,
  topic TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  produce_response JSONB NOT NULL,
  msg_offset BIGINT,
  batch_id BIGINT,
  event_durable BOOLEAN NOT NULL,
  event_sealed BOOLEAN NOT NULL,
  synced BOOLEAN,
  duplicate BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS paypal_provider_calls (
  call_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL REFERENCES paypal_payment_attempts(attempt_id) ON DELETE CASCADE,
  call_type TEXT NOT NULL,
  outcome TEXT NOT NULL,
  provider_order_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS paypal_verification_attempts (
  verification_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL REFERENCES paypal_payment_attempts(attempt_id) ON DELETE CASCADE,
  attempt_no INT NOT NULL,
  provider_result TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS paypal_processing_audit (
  audit_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL REFERENCES paypal_payment_attempts(attempt_id) ON DELETE CASCADE,
  event_offset BIGINT NOT NULL,
  audit_written_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  offset_committed_at TIMESTAMPTZ,
  detail JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS paypal_wallet_effects (
  effect_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL REFERENCES paypal_payment_attempts(attempt_id) ON DELETE CASCADE,
  effect_type TEXT NOT NULL,
  amount BIGINT NOT NULL,
  currency TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (attempt_id, effect_type)
);

CREATE TABLE IF NOT EXISTS paypal_cache_reconciliation (
  reconciliation_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL REFERENCES paypal_payment_attempts(attempt_id) ON DELETE CASCADE,
  database_state TEXT NOT NULL,
  redis_state_before TEXT,
  redis_state_after TEXT NOT NULL,
  resolution TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS paypal_receipts (
  receipt_id TEXT PRIMARY KEY,
  scenario TEXT NOT NULL,
  verdict TEXT NOT NULL,
  operator_answer TEXT NOT NULL,
  receipt JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_paypal_evidence_attempt
  ON paypal_ayder_evidence(attempt_id, duplicate);

CREATE INDEX IF NOT EXISTS idx_paypal_verification_attempt
  ON paypal_verification_attempts(attempt_id, attempt_no);
