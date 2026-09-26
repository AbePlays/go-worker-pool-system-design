package pool

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/AbePlays/go-worker-pool-system-design/internal/job"
	"github.com/AbePlays/go-worker-pool-system-design/internal/store"
)

type Pool struct {
	queue   chan string
	store   *store.Store
	timeout time.Duration
	wg      sync.WaitGroup
	workers int
}

func New(store *store.Store, timeout time.Duration, workers int) *Pool {
	return &Pool{
		queue:   make(chan string, workers),
		store:   store,
		timeout: timeout,
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

		if j.Payload.DurationMs < 0 {
			j.Status = job.StatusFailed
			j.LastError = fmt.Sprintf("invalid duration_ms %d", j.Payload.DurationMs)
			p.store.Update(j)
			continue
		}

		j.Status = job.StatusRunning
		p.store.Update(j)

		ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
		select {
		case <-time.After(time.Duration(j.Payload.DurationMs) * time.Millisecond):
			cancel()
			j.Status = job.StatusDone
			j.Result = fmt.Sprintf("Slept for %dms", j.Payload.DurationMs)
			p.store.Update(j)
		case <-ctx.Done():
			cancel()
			j.Status = job.StatusFailed
			j.LastError = "timeout: job exceeded deadline"
			j.Result = nil
			p.store.Update(j)
		}
	}
}

func (p *Pool) Stop() {
	close(p.queue)
	p.wg.Wait()
}

func (p *Pool) Submit(id string) {
	p.queue <- id
}
