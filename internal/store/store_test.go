package store

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/AbePlays/go-worker-pool-system-design/internal/job"
)

func testURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:postgres@localhost:5432/workerpool?sslmode=disable"
}

func TestRoundtrip(t *testing.T) {
	ctx := context.Background()
	s, err := New(ctx, testURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()
	if err := s.Truncate(ctx); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	j := job.Job{ID: uuid.NewString(), Type: "sleep", Payload: job.Payload{DurationMs: 50}, Status: job.StatusPending}
	if err := s.Save(ctx, j); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, ok, err := s.GetByID(ctx, j.ID)
	if err != nil || !ok {
		t.Fatalf("get: %v found=%v", err, ok)
	}
	if got.Payload.DurationMs != 50 || got.Status != job.StatusPending {
		t.Fatalf("mismatch: %+v", got)
	}

	got.Status = job.StatusDone
	got.Result = "Slept for 50ms"
	ok, err = s.Update(ctx, got)
	if err != nil || !ok {
		t.Fatalf("update: %v ok=%v", err, ok)
	}
	got2, _, _ := s.GetByID(ctx, j.ID)
	if got2.Status != job.StatusDone {
		t.Fatalf("expected done, got %s", got2.Status)
	}

	if _, ok, _ := s.GetByID(ctx, uuid.NewString()); ok {
		t.Fatal("expected not found for random id")
	}
}

func TestClaimBatch(t *testing.T) {
	ctx := context.Background()
	s, err := New(ctx, testURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()
	if err := s.Truncate(ctx); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	for range 5 {
		j := job.Job{ID: uuid.NewString(), Type: "sleep", Payload: job.Payload{DurationMs: 10}, Status: job.StatusPending}
		if err := s.Save(ctx, j); err != nil {
			t.Fatalf("save: %v", err)
		}
	}

	got, err := s.Claim(ctx, 3)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 claimed, got %d", len(got))
	}
	for _, j := range got {
		if j.Status != job.StatusRunning {
			t.Fatalf("expected running, got %s", j.Status)
		}
	}

	rest, err := s.Claim(ctx, 8)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(rest) != 2 {
		t.Fatalf("expected 2 remaining, got %d", len(rest))
	}

	empty, err := s.Claim(ctx, 8)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("expected 0, got %d", len(empty))
	}
}

func TestRequeueRunning(t *testing.T) {
	ctx := context.Background()
	s, err := New(ctx, testURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()
	if err := s.Truncate(ctx); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	j := job.Job{ID: uuid.NewString(), Type: "sleep", Payload: job.Payload{DurationMs: 10}, Status: job.StatusPending}
	if err := s.Save(ctx, j); err != nil {
		t.Fatalf("save: %v", err)
	}
	claimed, err := s.Claim(ctx, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v n=%d", err, len(claimed))
	}

	n, err := s.RequeueRunning(ctx)
	if err != nil || n != 1 {
		t.Fatalf("requeue: %v n=%d", err, n)
	}
	got, ok, err := s.GetByID(ctx, j.ID)
	if err != nil || !ok || got.Status != job.StatusPending {
		t.Fatalf("expected pending, got %+v err=%v", got, err)
	}

	n, err = s.RequeueRunning(ctx)
	if err != nil || n != 0 {
		t.Fatalf("expected 0, got %d err=%v", n, err)
	}
}
