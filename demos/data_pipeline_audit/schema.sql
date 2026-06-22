CREATE TABLE IF NOT EXISTS pipeline_jobs (
  job_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  stage TEXT NOT NULL,
  status TEXT NOT NULL,
  source_status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS api_retry_attempts (
  attempt_id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES pipeline_jobs(job_id) ON DELETE CASCADE,
  attempt_no INT NOT NULL,
  outcome TEXT NOT NULL,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_data_quality (
  job_id TEXT PRIMARY KEY REFERENCES pipeline_jobs(job_id) ON DELETE CASCADE,
  input_rows BIGINT NOT NULL,
  output_rows BIGINT NOT NULL,
  valid_rows BIGINT NOT NULL,
  invalid_rows BIGINT NOT NULL,
  missing_rows BIGINT NOT NULL,
  invalid_fields JSONB NOT NULL DEFAULT '[]'::jsonb,
  failed_rules JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_audit_receipts (
  receipt_id TEXT PRIMARY KEY,
  verdict TEXT NOT NULL,
  operator_answer TEXT NOT NULL,
  job JSONB NOT NULL,
  retry_summary JSONB NOT NULL,
  data_quality JSONB NOT NULL,
  business_rules JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
