ALTER TABLE jobs
  ADD COLUMN attempts INT NOT NULL DEFAULT 0,
  ADD COLUMN max_attempts INT NOT NULL DEFAULT 3,
  ADD COLUMN next_run_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX idx_jobs_due ON jobs (next_run_at) WHERE status = 'pending';
