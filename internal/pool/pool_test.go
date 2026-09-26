package pool

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AbePlays/go-worker-pool-system-design/internal/job"
	"github.com/AbePlays/go-worker-pool-system-design/internal/store"
)

func testDBURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:postgres@localhost:5432/workerpool?sslmode=disable"
}

func newTestStore(t *testing.T) *store.Postgres {
	t.Helper()
	ctx := context.Background()
	s, err := store.New(ctx, testDBURL())
	if err != nil {
		t.Fatalf("postgres connect: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.Truncate(ctx); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return s
}

func mustSave(t *testing.T, s *store.Postgres, j job.Job) {
	t.Helper()
	if err := s.Save(context.Background(), j); err != nil {
		t.Fatalf("save %s: %v", j.ID, err)
	}
}

func waitFor(t *testing.T, s *store.Postgres, id string, want job.Status, timeout time.Duration) job.Job {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for {
		got, ok, err := s.GetByID(ctx, id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if ok && got.Status == want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s=%s, got %+v", id, want, got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func newJob(dur int) job.Job {
	return job.Job{ID: uuid.NewString(), Type: "sleep", Payload: job.Payload{DurationMs: dur}, Status: job.StatusPending}
}

func TestSingleJobDone(t *testing.T) {
	s := newTestStore(t)
	p := New(s, 30*time.Second, 2)
	p.Start()
	defer p.Stop()

	j := newJob(20)
	mustSave(t, s, j)
	if !p.Submit(j.ID) {
		t.Fatal("submit should succeed")
	}

	got := waitFor(t, s, j.ID, job.StatusDone, 5*time.Second)
	if got.Result == nil {
		t.Fatal("expected result to be set on done")
	}
}

func TestNegativeDurationFailed(t *testing.T) {
	s := newTestStore(t)
	p := New(s, 30*time.Second, 2)
	p.Start()
	defer p.Stop()

	j := newJob(-5)
	mustSave(t, s, j)
	p.Submit(j.ID)

	got := waitFor(t, s, j.ID, job.StatusFailed, 5*time.Second)
	if got.LastError == "" {
		t.Fatal("expected LastError on failed")
	}
}

func TestMissingIDDoesNotCrashWorker(t *testing.T) {
	s := newTestStore(t)
	p := New(s, 30*time.Second, 1)
	p.Start()
	defer p.Stop()

	p.Submit("00000000-0000-0000-0000-000000000000") // no such job, worker should skip

	j := newJob(10)
	mustSave(t, s, j)
	p.Submit(j.ID)
	waitFor(t, s, j.ID, job.StatusDone, 5*time.Second)
}

func TestBoundedConcurrency(t *testing.T) {
	s := newTestStore(t)
	workers := 4
	p := New(s, 30*time.Second, workers)
	p.Start()
	defer p.Stop()

	n := 8
	dur := 200
	ids := make([]string, n)
	for i := range n {
		j := newJob(dur)
		ids[i] = j.ID
		mustSave(t, s, j)
	}
	start := time.Now()
	for _, id := range ids {
		if !p.Submit(id) {
			t.Fatal("submit should succeed")
		}
	}
	for _, id := range ids {
		waitFor(t, s, id, job.StatusDone, 10*time.Second)
	}
	elapsed := time.Since(start)

	// 8x200ms with 4 workers = 2 waves ≈ 400ms. Sequential would be 1600ms.
	if elapsed < 350*time.Millisecond {
		t.Fatalf("too fast (%v), workers may be unbounded", elapsed)
	}
	if elapsed > 3000*time.Millisecond {
		t.Fatalf("too slow (%v), expected ~400ms for 2 waves", elapsed)
	}
}

func TestJobTimeout(t *testing.T) {
	s := newTestStore(t)
	p := New(s, 50*time.Millisecond, 1)
	p.Start()
	defer p.Stop()

	j := newJob(5000)
	mustSave(t, s, j)
	start := time.Now()
	p.Submit(j.ID)

	got := waitFor(t, s, j.ID, job.StatusFailed, 5*time.Second)
	elapsed := time.Since(start)
	if elapsed > 2000*time.Millisecond {
		t.Fatalf("timeout did not abort early, took %v", elapsed)
	}
	if got.LastError == "" {
		t.Fatal("expected LastError on timeout")
	}
}

func TestSubmitAfterShutdownFalse(t *testing.T) {
	s := newTestStore(t)
	p := New(s, 30*time.Second, 1)
	p.Start()
	p.Shutdown()
	if p.Submit("late") {
		t.Fatal("expected Submit false after Shutdown")
	}
}

func TestDoubleShutdownSafe(t *testing.T) {
	s := newTestStore(t)
	p := New(s, 30*time.Second, 1)
	p.Start()
	p.Shutdown()
	p.Shutdown() // must not panic
	p.Stop()     // alias must not panic
}

func TestShutdownDrains(t *testing.T) {
	s := newTestStore(t)
	p := New(s, 30*time.Second, 2)
	p.Start()

	j := newJob(100)
	mustSave(t, s, j)
	if !p.Submit(j.ID) {
		t.Fatal("submit should succeed before shutdown")
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		p.Shutdown()
	}()
	p.wg.Wait() // ensure drain path exercised via Shutdown internal wait
	got, ok, err := s.GetByID(context.Background(), j.ID)
	if err != nil || !ok {
		t.Fatalf("get: %v found=%v", err, ok)
	}
	if got.Status != job.StatusDone {
		t.Fatalf("expected drained done, got %s", got.Status)
	}
}
