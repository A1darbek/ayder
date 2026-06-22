CREATE TABLE IF NOT EXISTS webhook_payments (
  payment_id TEXT PRIMARY KEY,
  state TEXT NOT NULL,
  state_rank INT NOT NULL,
  success_effect_count INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS webhook_events (
  event_id TEXT PRIMARY KEY,
  payment_id TEXT NOT NULL,
  incoming_state TEXT NOT NULL,
  state_rank INT NOT NULL,
  received_order INT NOT NULL,
  action TEXT NOT NULL,
  reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS webhook_suppressed_attempts (
  event_id TEXT PRIMARY KEY,
  payment_id TEXT NOT NULL,
  reason TEXT NOT NULL,
  incoming_state TEXT NOT NULL,
  current_state TEXT NOT NULL,
  action TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS webhook_receipts (
  receipt_id TEXT PRIMARY KEY,
  verdict TEXT NOT NULL,
  operator_answer TEXT NOT NULL,
  payment_id TEXT NOT NULL,
  final_state TEXT NOT NULL,
  suppressed_attempts JSONB NOT NULL,
  business_rules JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_webhook_events_payment
  ON webhook_events(payment_id, received_order);
