-- Schemas
CREATE SCHEMA IF NOT EXISTS metadata;
CREATE SCHEMA IF NOT EXISTS staging;

-- Track incremental watermarks per source
CREATE TABLE IF NOT EXISTS metadata.ingestion_state (
  source_name TEXT PRIMARY KEY,
  last_ingested_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One row per pipeline run (simple observability)
CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
  run_id TEXT PRIMARY KEY,
  pipeline_name TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ,
  status TEXT NOT NULL,
  row_count INTEGER,
  error_message TEXT
);

-- Staging table for API records (upsert target)
CREATE TABLE IF NOT EXISTS staging.loan_applications (
  application_id UUID PRIMARY KEY,
  applicant_id UUID NOT NULL,
  submitted_at TIMESTAMPTZ NOT NULL,
  loan_amount NUMERIC NOT NULL,
  purpose TEXT NOT NULL,
  state TEXT NOT NULL,
  annual_income NUMERIC NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_staging_loan_applications_submitted_at
ON staging.loan_applications (submitted_at);
