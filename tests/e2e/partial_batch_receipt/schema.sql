-- Schema for the partial-batch / offset-commit recovery-receipt demo.
--
-- It deliberately reuses the same business-effect model as the exactly-once
-- harness (expected_events / processed_events / account_balances /
-- consumer_state{,_history}) so the failure mode is apples-to-apples, and adds:
--   * dlq                    : events a consumer parked instead of applying
--   * expected_effects       : business effects each event should produce
--   * observed_effects       : business effects actually observed downstream
--   * recovery_receipt       : persisted receipts (audit trail of reconciliations)
--   * recovery_receipt_item  : per-event detail behind each receipt
--
-- Every table is CREATE ... IF NOT EXISTS so the file is idempotent and can be
-- applied on top of an existing exactly-once database without conflict.

CREATE TABLE IF NOT EXISTS expected_events (
  event_id      TEXT PRIMARY KEY,
  account_id    TEXT NOT NULL,
  delta         BIGINT NOT NULL,
  seq           BIGINT,
  produced_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- seq may not exist if an older exactly-once schema created the table first.
ALTER TABLE expected_events ADD COLUMN IF NOT EXISTS seq BIGINT;

CREATE TABLE IF NOT EXISTS processed_events (
  event_id      TEXT PRIMARY KEY,
  topic         TEXT NOT NULL,
  group_id      TEXT NOT NULL,
  partition_id  INT NOT NULL,
  msg_offset    BIGINT NOT NULL,
  account_id    TEXT NOT NULL,
  delta         BIGINT NOT NULL,
  payload       JSONB NOT NULL,
  applied_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS account_balances (
  account_id    TEXT PRIMARY KEY,
  balance       BIGINT NOT NULL DEFAULT 0,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS expected_effects (
  event_id      TEXT NOT NULL REFERENCES expected_events(event_id) ON DELETE CASCADE,
  effect_type   TEXT NOT NULL,
  account_id    TEXT,
  expected_value JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (event_id, effect_type)
);

CREATE TABLE IF NOT EXISTS observed_effects (
  event_id      TEXT NOT NULL,
  effect_type   TEXT NOT NULL,
  topic         TEXT NOT NULL,
  group_id      TEXT NOT NULL,
  partition_id  INT NOT NULL,
  msg_offset    BIGINT,
  status        TEXT NOT NULL DEFAULT 'confirmed',
  observed_value JSONB NOT NULL DEFAULT '{}'::jsonb,
  observed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (event_id, effect_type, topic, group_id, partition_id)
);

CREATE TABLE IF NOT EXISTS consumer_state (
  topic         TEXT NOT NULL,
  group_id      TEXT NOT NULL,
  partition_id  INT NOT NULL,
  last_offset   BIGINT NOT NULL,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (topic, group_id, partition_id)
);

CREATE TABLE IF NOT EXISTS consumer_state_history (
  id            BIGSERIAL PRIMARY KEY,
  topic         TEXT NOT NULL,
  group_id      TEXT NOT NULL,
  partition_id  INT NOT NULL,
  prev_offset   BIGINT NOT NULL,
  new_offset    BIGINT NOT NULL,
  changed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS commit_log (
  id            BIGSERIAL PRIMARY KEY,
  topic         TEXT NOT NULL,
  group_id      TEXT NOT NULL,
  partition_id  INT NOT NULL,
  msg_offset    BIGINT NOT NULL,
  commit_ok     BOOLEAN NOT NULL,
  detail        TEXT,
  at_utc        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dead-letter / retry backlog: events a consumer explicitly could not apply.
-- An event sitting here is recoverable (you know where to look); an event that
-- is neither processed NOR here is silently lost business work.
CREATE TABLE IF NOT EXISTS dlq (
  id            BIGSERIAL PRIMARY KEY,
  event_id      TEXT NOT NULL,
  topic         TEXT NOT NULL,
  group_id      TEXT NOT NULL,
  partition_id  INT NOT NULL,
  msg_offset    BIGINT NOT NULL,
  account_id    TEXT,
  delta         BIGINT,
  reason        TEXT NOT NULL,
  attempts      INT NOT NULL DEFAULT 1,
  status        TEXT NOT NULL DEFAULT 'pending',  -- pending | retried | resolved
  parked_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (event_id, topic, group_id, partition_id)
);

-- Persisted receipts: one row per reconciliation, plus per-event detail.
CREATE TABLE IF NOT EXISTS recovery_receipt (
  receipt_id        TEXT PRIMARY KEY,
  topic             TEXT NOT NULL,
  group_id          TEXT NOT NULL,
  partition_id      INT NOT NULL,
  window_from_seq   BIGINT,
  window_to_seq     BIGINT,
  expected_count    BIGINT NOT NULL,
  confirmed_count   BIGINT NOT NULL,
  missing_count     BIGINT NOT NULL,
  duplicate_count   BIGINT NOT NULL,
  dlq_count         BIGINT NOT NULL,
  event_status_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
  effect_status_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
  verdict           TEXT NOT NULL,                -- GREEN | AMBER | RED
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE recovery_receipt ADD COLUMN IF NOT EXISTS event_status_counts JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE recovery_receipt ADD COLUMN IF NOT EXISTS effect_status_counts JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS recovery_receipt_item (
  receipt_id    TEXT NOT NULL REFERENCES recovery_receipt(receipt_id) ON DELETE CASCADE,
  event_id      TEXT NOT NULL,
  account_id    TEXT,
  delta         BIGINT,
  seq           BIGINT,
  classification TEXT NOT NULL,                   -- confirmed | missing | duplicate | dlq
  event_status  TEXT,
  expected_effects JSONB NOT NULL DEFAULT '[]'::jsonb,
  observed_effects JSONB NOT NULL DEFAULT '[]'::jsonb,
  effect_status JSONB NOT NULL DEFAULT '{}'::jsonb,
  recommended_action TEXT,
  warning       TEXT,
  detail        TEXT,
  PRIMARY KEY (receipt_id, event_id, classification)
);

ALTER TABLE recovery_receipt_item ADD COLUMN IF NOT EXISTS event_status TEXT;
ALTER TABLE recovery_receipt_item ADD COLUMN IF NOT EXISTS expected_effects JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE recovery_receipt_item ADD COLUMN IF NOT EXISTS observed_effects JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE recovery_receipt_item ADD COLUMN IF NOT EXISTS effect_status JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE recovery_receipt_item ADD COLUMN IF NOT EXISTS recommended_action TEXT;
ALTER TABLE recovery_receipt_item ADD COLUMN IF NOT EXISTS warning TEXT;

CREATE INDEX IF NOT EXISTS idx_pbr_processed_offsets
  ON processed_events(topic, group_id, partition_id, msg_offset);

CREATE INDEX IF NOT EXISTS idx_pbr_state_history
  ON consumer_state_history(topic, group_id, partition_id, id);

CREATE INDEX IF NOT EXISTS idx_pbr_dlq_scope
  ON dlq(topic, group_id, partition_id, status);

CREATE INDEX IF NOT EXISTS idx_pbr_expected_effects_event
  ON expected_effects(event_id);

CREATE INDEX IF NOT EXISTS idx_pbr_observed_effects_scope
  ON observed_effects(topic, group_id, partition_id, status);
