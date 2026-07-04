CREATE TABLE IF NOT EXISTS partial_effect_events (
  event_id TEXT PRIMARY KEY,
  event_type TEXT NOT NULL,
  order_id TEXT NOT NULL,
  customer_id TEXT NOT NULL,
  amount NUMERIC(12,2) NOT NULL,
  currency TEXT NOT NULL,
  topic TEXT NOT NULL,
  consumer_group TEXT NOT NULL,
  partition INT NOT NULL,
  event_offset BIGINT NOT NULL,
  idempotency_key TEXT NOT NULL,
  event_durable BOOLEAN NOT NULL,
  event_sealed BOOLEAN NOT NULL,
  event_synced BOOLEAN NOT NULL,
  consumed BOOLEAN NOT NULL DEFAULT false,
  offset_committed BOOLEAN NOT NULL DEFAULT false,
  raw_event JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS partial_effect_expected_effects (
  event_id TEXT NOT NULL,
  effect_name TEXT NOT NULL,
  ordinal INT NOT NULL,
  PRIMARY KEY (event_id, effect_name)
);

CREATE TABLE IF NOT EXISTS partial_effect_audit (
  audit_id BIGSERIAL PRIMARY KEY,
  event_id TEXT NOT NULL,
  effect_name TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('CONFIRMED','MISSING','UNKNOWN')),
  attempt_no INT NOT NULL DEFAULT 1,
  detail TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS partial_effect_receipts (
  receipt_id TEXT PRIMARY KEY,
  scenario TEXT NOT NULL,
  verdict TEXT NOT NULL,
  operator_answer TEXT NOT NULL,
  ayder_evidence JSONB NOT NULL,
  effect_summary JSONB NOT NULL,
  effects JSONB NOT NULL,
  recovery_guidance JSONB NOT NULL,
  business_rules JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_partial_effect_audit_event
  ON partial_effect_audit(event_id, effect_name);
