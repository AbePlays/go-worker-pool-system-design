package pool

import (
	"testing"
	"time"

	"github.com/AbePlays/go-worker-pool-system-design/internal/job"
	"github.com/AbePlays/go-worker-pool-system-design/internal/store"
)

func waitFor(t *testing.T, s *store.Store, id string, want job.Status, timeout time.Duration) job.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got, ok := s.GetByID(id)
		if ok && got.Status == want {
			return got
		}
		if time.Now().After(deadline) {
			got, _ := s.GetByID(id)
			t.Fatalf("timeout waiting for %s=%s, got %+v", id, want, got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSingleJobDone(t *testing.T) {
	s := store.New()
	p := New(s, 30*time.Second, 2)
	p.Start()
	defer p.Stop()

	j := job.Job{ID: "1", Type: "sleep", Payload: job.Payload{DurationMs: 20}, Status: job.StatusPending}
	s.Save(j)
	p.Submit("1")

	got := waitFor(t, s, "1", job.StatusDone, 2*time.Second)
	if got.Result == nil {
		t.Fatal("expected result to be set on done")
	}
}

func TestNegativeDurationFailed(t *testing.T) {
	s := store.New()
	p := New(s, 30*time.Second, 2)
	p.Start()
	defer p.Stop()

	j := job.Job{ID: "bad", Type: "sleep", Payload: job.Payload{DurationMs: -5}, Status: job.StatusPending}
	s.Save(j)
	p.Submit("bad")

	got := waitFor(t, s, "bad", job.StatusFailed, 2*time.Second)
	if got.LastError == "" {
		t.Fatal("expected LastError on failed")
	}
}

func TestMissingIDDoesNotCrashWorker(t *testing.T) {
	s := store.New()
	p := New(s, 30*time.Second, 1)
	p.Start()
	defer p.Stop()

	p.Submit("nope") // no such job, worker should skip

	// follow with a real job to prove worker still alive
	j := job.Job{ID: "after", Type: "sleep", Payload: job.Payload{DurationMs: 10}, Status: job.StatusPending}
	s.Save(j)
	p.Submit("after")
	waitFor(t, s, "after", job.StatusDone, 2*time.Second)
}

func TestBoundedConcurrency(t *testing.T) {
	s := store.New()
	workers := 4
	p := New(s, 30*time.Second, workers)
	p.Start()
	defer p.Stop()

	n := 8
	dur := 200
	for i := range n {
		id := string(rune('a' + i))
		s.Save(job.Job{ID: id, Type: "sleep", Payload: job.Payload{DurationMs: dur}, Status: job.StatusPending})
	}
	start := time.Now()
	for i := range n {
		p.Submit(string(rune('a' + i)))
	}
	for i := range n {
		waitFor(t, s, string(rune('a'+i)), job.StatusDone, 5*time.Second)
	}
	elapsed := time.Since(start)

	// 8x200ms with 4 workers = 2 waves ≈ 400ms. Sequential would be 1600ms.
	if elapsed < 350*time.Millisecond {
		t.Fatalf("too fast (%v), workers may be unbounded", elapsed)
	}
	if elapsed > 1200*time.Millisecond {
		t.Fatalf("too slow (%v), expected ~400ms for 2 waves", elapsed)
	}
}

func TestJobTimeout(t *testing.T) {
	s := store.New()
	p := New(s, 50*time.Millisecond, 1)
	p.Start()
	defer p.Stop()

	s.Save(job.Job{ID: "slow", Type: "sleep", Payload: job.Payload{DurationMs: 5000}, Status: job.StatusPending})
	start := time.Now()
	p.Submit("slow")

	got := waitFor(t, s, "slow", job.StatusFailed, 2*time.Second)
	elapsed := time.Since(start)
	if elapsed > 1000*time.Millisecond {
		t.Fatalf("timeout did not abort early, took %v", elapsed)
	}
	if got.LastError == "" {
		t.Fatal("expected LastError on timeout")
	}
}
