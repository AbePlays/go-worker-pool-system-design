package dispatcher

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AbePlays/go-worker-pool-system-design/internal/job"
	"github.com/AbePlays/go-worker-pool-system-design/internal/pool"
	"github.com/AbePlays/go-worker-pool-system-design/internal/store"
)

func testDBURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:postgres@localhost:5432/workerpool?sslmode=disable"
}

func TestRunsPendingJobs(t *testing.T) {
	ctx := context.Background()
	s, err := store.New(ctx, testDBURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()
	if err := s.Truncate(ctx); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	p := pool.New(s, 30*time.Second, 2)
	p.Start()
	defer p.Stop()

	id := uuid.NewString()
	if err := s.Save(ctx, job.Job{ID: id, Type: "sleep", Payload: job.Payload{DurationMs: 20}, Status: job.StatusPending}); err != nil {
		t.Fatalf("save: %v", err)
	}

	dctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go New(p, s, 2).Run(dctx)

	deadline := time.Now().Add(10 * time.Second)
	for {
		got, ok, err := s.GetByID(ctx, id)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if ok && got.Status == job.StatusDone {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout, last=%+v", got)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
