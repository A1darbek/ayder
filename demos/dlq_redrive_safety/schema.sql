CREATE TABLE IF NOT EXISTS sqs_messages (
  message_id TEXT PRIMARY KEY,
  business_operation TEXT NOT NULL,
  payload JSONB NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sqs_processing_attempts (
  attempt_id TEXT PRIMARY KEY,
  message_id TEXT NOT NULL REFERENCES sqs_messages(message_id) ON DELETE CASCADE,
  attempt_no INT NOT NULL,
  outcome TEXT NOT NULL,
  failure_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sqs_business_effects (
  effect_id TEXT PRIMARY KEY,
  message_id TEXT NOT NULL REFERENCES sqs_messages(message_id) ON DELETE CASCADE,
  effect_type TEXT NOT NULL,
  status TEXT NOT NULL,
  detail JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sqs_dlq_items (
  message_id TEXT PRIMARY KEY REFERENCES sqs_messages(message_id) ON DELETE CASCADE,
  business_operation TEXT NOT NULL,
  failure_reason TEXT NOT NULL,
  last_known_effect TEXT NOT NULL,
  replay_safety TEXT NOT NULL,
  disposition TEXT NOT NULL,
  recommended_action TEXT NOT NULL,
  moved_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dlq_redrive_receipts (
  receipt_id TEXT PRIMARY KEY,
  verdict TEXT NOT NULL,
  operator_answer TEXT NOT NULL,
  dlq_summary JSONB NOT NULL,
  dlq_items JSONB NOT NULL,
  business_rules JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sqs_attempts_message
  ON sqs_processing_attempts(message_id, attempt_no);
