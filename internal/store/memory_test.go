package store

import (
	"fmt"
	"sync"
	"testing"

	"github.com/AbePlays/go-worker-pool-system-design/internal/job"
)

func newTestJob(id string) job.Job {
	return job.Job{
		ID:      id,
		Type:    "sleep",
		Payload: job.Payload{DurationMs: 10},
		Status:  job.StatusPending,
	}
}

func TestSaveThenGet(t *testing.T) {
	s := New()
	j := newTestJob("1")
	s.Save(j)

	got, ok := s.GetByID("1")
	if !ok {
		t.Fatal("expected job to be found")
	}
	if got.ID != j.ID || got.Status != job.StatusPending {
		t.Fatalf("mismatch: got %+v want %+v", got, j)
	}
	if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
		t.Fatal("expected timestamps to be stamped on Save")
	}
}

func TestGetMissing(t *testing.T) {
	s := New()
	_, ok := s.GetByID("nope")
	if ok {
		t.Fatal("expected not found for missing id")
	}
}

func TestUpdate(t *testing.T) {
	s := New()
	s.Save(newTestJob("1"))

	got, _ := s.GetByID("1")
	got.Status = job.StatusRunning
	if !s.Update(got) {
		t.Fatal("expected Update to return true")
	}

	updated, _ := s.GetByID("1")
	if updated.Status != job.StatusRunning {
		t.Fatalf("expected running, got %s", updated.Status)
	}
	if updated.CreatedAt != got.CreatedAt {
		t.Fatal("CreatedAt must be preserved on Update")
	}

	if s.Update(newTestJob("missing")) {
		t.Fatal("expected Update false for missing id")
	}
}

func TestMutationIsolation(t *testing.T) {
	s := New()
	s.Save(newTestJob("1"))

	got, _ := s.GetByID("1")
	got.Status = job.StatusDone // mutate returned copy

	again, _ := s.GetByID("1")
	if again.Status == job.StatusDone {
		t.Fatal("mutating returned struct must not affect store")
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("job-%d", i)
			s.Save(newTestJob(id))
			j, ok := s.GetByID(id)
			if !ok {
				t.Errorf("missing %s", id)
				return
			}
			j.Status = job.StatusDone
			s.Update(j)
		}(i)
	}
	wg.Wait()

	for i := range 50 {
		id := fmt.Sprintf("job-%d", i)
		got, ok := s.GetByID(id)
		if !ok || got.Status != job.StatusDone {
			t.Fatalf("expected %s done, got %+v found=%v", id, got, ok)
		}
	}
}
