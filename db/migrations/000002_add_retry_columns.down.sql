DROP INDEX IF EXISTS idx_jobs_due;

ALTER TABLE jobs
  DROP COLUMN next_run_at,
  DROP COLUMN max_attempts,
  DROP COLUMN attempts;
