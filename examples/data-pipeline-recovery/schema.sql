CREATE TABLE IF NOT EXISTS pipeline_recovery_runs (
  job_run_id TEXT PRIMARY KEY,
  scenario TEXT NOT NULL,
  job_name TEXT NOT NULL,
  stage TEXT NOT NULL,
  external_source TEXT NOT NULL,
  expected_pages INT NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_ayder_evidence (
  id BIGSERIAL PRIMARY KEY,
  job_run_id TEXT NOT NULL,
  topic TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  produce_response JSONB NOT NULL,
  msg_offset BIGINT,
  batch_id BIGINT,
  event_durable BOOLEAN NOT NULL,
  event_sealed BOOLEAN NOT NULL,
  synced BOOLEAN,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_page_downloads (
  job_run_id TEXT NOT NULL REFERENCES pipeline_recovery_runs(job_run_id) ON DELETE CASCADE,
  page_no INT NOT NULL,
  critical BOOLEAN NOT NULL DEFAULT false,
  status TEXT NOT NULL,
  records_loaded INT NOT NULL DEFAULT 0,
  last_error TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (job_run_id, page_no)
);

CREATE TABLE IF NOT EXISTS pipeline_retry_attempts (
  retry_id TEXT PRIMARY KEY,
  job_run_id TEXT NOT NULL REFERENCES pipeline_recovery_runs(job_run_id) ON DELETE CASCADE,
  page_no INT NOT NULL,
  attempt_no INT NOT NULL,
  outcome TEXT NOT NULL,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_processing_audits (
  audit_id TEXT PRIMARY KEY,
  job_run_id TEXT NOT NULL REFERENCES pipeline_recovery_runs(job_run_id) ON DELETE CASCADE,
  event_offset BIGINT NOT NULL,
  replay_number INT NOT NULL DEFAULT 0,
  confirmed_pages INT NOT NULL,
  missing_pages JSONB NOT NULL,
  records_loaded INT NOT NULL,
  records_missing_estimate INT NOT NULL,
  audit_status TEXT NOT NULL,
  audit_written_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  offset_committed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS pipeline_replay_history (
  replay_id TEXT PRIMARY KEY,
  job_run_id TEXT NOT NULL REFERENCES pipeline_recovery_runs(job_run_id) ON DELETE CASCADE,
  original_offset BIGINT NOT NULL,
  replay_number INT NOT NULL,
  pages_recovered JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_recovery_receipts (
  receipt_id TEXT PRIMARY KEY,
  scenario TEXT NOT NULL,
  verdict TEXT NOT NULL,
  operator_answer TEXT NOT NULL,
  receipt JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_pages_run
  ON pipeline_page_downloads(job_run_id, status, page_no);

CREATE INDEX IF NOT EXISTS idx_pipeline_audits_run
  ON pipeline_processing_audits(job_run_id, replay_number);
