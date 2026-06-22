CREATE TABLE IF NOT EXISTS payment_attempts (
  attempt_id TEXT PRIMARY KEY,
  merchant_attempt_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  amount BIGINT NOT NULL,
  currency TEXT NOT NULL,
  internal_state TEXT NOT NULL,
  customer_state TEXT NOT NULL,
  ambiguity_reason TEXT,
  provider_transaction_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS provider_calls (
  call_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL REFERENCES payment_attempts(attempt_id) ON DELETE CASCADE,
  request_type TEXT NOT NULL,
  outcome TEXT NOT NULL,
  reason TEXT,
  provider_transaction_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS verification_attempts (
  verification_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL REFERENCES payment_attempts(attempt_id) ON DELETE CASCADE,
  provider_result TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS suppressed_attempts (
  attempt_id TEXT PRIMARY KEY REFERENCES payment_attempts(attempt_id) ON DELETE CASCADE,
  duplicate_of TEXT NOT NULL REFERENCES payment_attempts(attempt_id) ON DELETE CASCADE,
  idempotency_key TEXT NOT NULL,
  reason TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS payment_expected_effects (
  attempt_id TEXT NOT NULL REFERENCES payment_attempts(attempt_id) ON DELETE CASCADE,
  effect_type TEXT NOT NULL,
  PRIMARY KEY (attempt_id, effect_type)
);

CREATE TABLE IF NOT EXISTS payment_observed_effects (
  attempt_id TEXT NOT NULL REFERENCES payment_attempts(attempt_id) ON DELETE CASCADE,
  effect_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'CONFIRMED',
  detail JSONB NOT NULL DEFAULT '{}'::jsonb,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (attempt_id, effect_type)
);

CREATE TABLE IF NOT EXISTS provider_timeout_receipts (
  receipt_id TEXT PRIMARY KEY,
  verdict TEXT NOT NULL,
  financial_determinism TEXT NOT NULL,
  operator_answer TEXT NOT NULL,
  summary JSONB NOT NULL,
  indeterminate_attempts JSONB NOT NULL,
  business_rules JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_payment_attempts_idem
  ON payment_attempts(idempotency_key);

CREATE INDEX IF NOT EXISTS idx_provider_calls_attempt
  ON provider_calls(attempt_id, request_type, outcome);
