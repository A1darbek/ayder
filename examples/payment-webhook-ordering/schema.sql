CREATE TABLE IF NOT EXISTS ordering_payments (
  payment_id TEXT PRIMARY KEY,
  state TEXT NOT NULL,
  terminal BOOLEAN NOT NULL,
  success_effect_count INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ordering_ayder_evidence (
  id BIGSERIAL PRIMARY KEY,
  ingestion_event_id TEXT NOT NULL,
  provider_event_id TEXT NOT NULL,
  topic TEXT NOT NULL,
  produce_response JSONB NOT NULL,
  msg_offset BIGINT,
  batch_id BIGINT,
  event_durable BOOLEAN NOT NULL,
  event_sealed BOOLEAN NOT NULL,
  synced BOOLEAN,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ordering_webhook_handling (
  ingestion_event_id TEXT PRIMARY KEY,
  provider_event_id TEXT NOT NULL,
  payment_id TEXT NOT NULL,
  incoming_state TEXT NOT NULL,
  current_state_before TEXT,
  decision TEXT NOT NULL,
  business_effect_applied BOOLEAN NOT NULL DEFAULT false,
  event_offset BIGINT NOT NULL,
  audit_written_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  offset_committed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ordering_provider_event_dedupe (
  provider_event_id TEXT PRIMARY KEY,
  first_ingestion_event_id TEXT NOT NULL,
  payment_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ordering_business_effects (
  effect_id TEXT PRIMARY KEY,
  payment_id TEXT NOT NULL,
  effect_type TEXT NOT NULL,
  provider_event_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (payment_id, effect_type)
);

CREATE TABLE IF NOT EXISTS ordering_receipts (
  receipt_id TEXT PRIMARY KEY,
  verdict TEXT NOT NULL,
  operator_answer TEXT NOT NULL,
  receipt JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ordering_handling_payment
  ON ordering_webhook_handling(payment_id, event_offset);
