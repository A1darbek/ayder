CREATE TABLE IF NOT EXISTS expected_events (
  event_id      TEXT PRIMARY KEY,
  account_id    TEXT NOT NULL,
  delta         BIGINT NOT NULL,
  produced_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

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

CREATE INDEX IF NOT EXISTS idx_processed_offsets
  ON processed_events(topic, group_id, partition_id, msg_offset);

CREATE INDEX IF NOT EXISTS idx_state_history
  ON consumer_state_history(topic, group_id, partition_id, id);
