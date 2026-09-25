package pool

import (
	"fmt"
	"sync"
	"time"

	"github.com/AbePlays/go-worker-pool-system-design/internal/job"
	"github.com/AbePlays/go-worker-pool-system-design/internal/store"
)

type Pool struct {
	queue   chan string
	store   *store.Store
	wg      sync.WaitGroup
	workers int
}

func New(store *store.Store, workers int) *Pool {
	return &Pool{
		queue:   make(chan string, workers),
		store:   store,
		workers: workers,
	}
}

func (p *Pool) Start() {
	p.wg.Add(p.workers)
	for range p.workers {
		go p.worker()
	}
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for id := range p.queue {
		j, ok := p.store.GetByID(id)
		if !ok {
			continue
		}

		j.Status = job.StatusRunning
		p.store.Update(j)

		time.Sleep(time.Duration(j.Payload.DurationMs) * time.Millisecond)

		if j.Payload.DurationMs < 0 {
			j.Status = job.StatusFailed
			j.LastError = fmt.Sprintf("invalid duration_ms %d", j.Payload.DurationMs)
		} else {
			j.Status = job.StatusDone
		}

		result := fmt.Sprintf("Slept for %dms", j.Payload.DurationMs)
		j.Result = result
		p.store.Update(j)
	}
}

func (p *Pool) Stop() {
	close(p.queue)
	p.wg.Wait()
}

func (p *Pool) Submit(id string) {
	p.queue <- id
}
