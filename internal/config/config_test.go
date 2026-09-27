package config

import (
	"testing"
	"time"
)

func setValidEnv(t *testing.T) {
	t.Helper()
	t.Setenv("PORT", "8080")
	t.Setenv("WORKERS", "8")
	t.Setenv("JOB_TIMEOUT", "30")
	t.Setenv("MAX_ATTEMPTS", "3")
	t.Setenv("QUEUE_MAX", "1000")
	t.Setenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/workerpool?sslmode=disable")
}

func TestLoadValid(t *testing.T) {
	setValidEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if cfg.Port != "8080" || cfg.Workers != 8 || cfg.JobTimeout != 30*time.Second || cfg.MaxAttempts != 3 || cfg.QueueMax != 1000 {
		t.Fatalf("mismatch: %+v", cfg)
	}
}

func TestLoadMissing(t *testing.T) {
	t.Setenv("PORT", "")
	t.Setenv("WORKERS", "")
	t.Setenv("JOB_TIMEOUT", "")
	t.Setenv("MAX_ATTEMPTS", "")
	t.Setenv("QUEUE_MAX", "")
	t.Setenv("DATABASE_URL", "")
	if _, err := Load(); err == nil {
		t.Fatal("expected error for missing env")
	}
}

func TestLoadBadWorkers(t *testing.T) {
	for _, v := range []string{"abc", "0", "-1", "101"} {
		setValidEnv(t)
		t.Setenv("WORKERS", v)
		if _, err := Load(); err == nil {
			t.Fatalf("expected error for WORKERS=%s", v)
		}
	}
}

func TestLoadBadTimeout(t *testing.T) {
	for _, v := range []string{"abc", "0", "-5"} {
		setValidEnv(t)
		t.Setenv("JOB_TIMEOUT", v)
		if _, err := Load(); err == nil {
			t.Fatalf("expected error for JOB_TIMEOUT=%s", v)
		}
	}
}

func TestLoadBadMaxAttempts(t *testing.T) {
	for _, v := range []string{"abc", "0", "-1", "11"} {
		setValidEnv(t)
		t.Setenv("MAX_ATTEMPTS", v)
		if _, err := Load(); err == nil {
			t.Fatalf("expected error for MAX_ATTEMPTS=%s", v)
		}
	}
}

func TestLoadBadQueueMax(t *testing.T) {
	for _, v := range []string{"abc", "0", "-1", "100001"} {
		setValidEnv(t)
		t.Setenv("QUEUE_MAX", v)
		if _, err := Load(); err == nil {
			t.Fatalf("expected error for QUEUE_MAX=%s", v)
		}
	}
}
