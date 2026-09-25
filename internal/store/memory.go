package store

import (
	"sync"
	"time"

	"github.com/AbePlays/go-worker-pool-system-design/internal/job"
)

type Store struct {
	mutex sync.RWMutex
	store map[string]job.Job
}

func New() *Store {
	return &Store{
		mutex: sync.RWMutex{},
		store: make(map[string]job.Job),
	}
}

func (s *Store) Save(j job.Job) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	now := time.Now()
	if j.CreatedAt.IsZero() {
		j.CreatedAt = now
	}
	j.UpdatedAt = now
	s.store[j.ID] = j
}

func (s *Store) Update(j job.Job) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	existing, ok := s.store[j.ID]
	if !ok {
		return false
	}

	j.CreatedAt = existing.CreatedAt
	j.UpdatedAt = time.Now()
	s.store[j.ID] = j

	return true
}

func (s *Store) GetByID(id string) (job.Job, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	j, ok := s.store[id]
	return j, ok
}
