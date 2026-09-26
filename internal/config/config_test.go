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
}

func TestLoadValid(t *testing.T) {
	setValidEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if cfg.Port != "8080" || cfg.Workers != 8 || cfg.JobTimeout != 30*time.Second {
		t.Fatalf("mismatch: %+v", cfg)
	}
}

func TestLoadMissing(t *testing.T) {
	t.Setenv("PORT", "")
	t.Setenv("WORKERS", "")
	t.Setenv("JOB_TIMEOUT", "")
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
