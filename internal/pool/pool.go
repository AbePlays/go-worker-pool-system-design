package pool

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/AbePlays/go-worker-pool-system-design/internal/job"
	"github.com/AbePlays/go-worker-pool-system-design/internal/store"
)

type Pool struct {
	queue     chan string
	store     *store.Store
	timeout   time.Duration
	wg        sync.WaitGroup
	mu        sync.RWMutex
	closeOnce sync.Once
	closed    bool
	workers   int
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
		dbCtx := context.Background()
		j, ok, err := p.store.GetByID(dbCtx, id)
		if err != nil {
			slog.Error("job fetch failed", "job_id", id, "error", err.Error())
			continue
		}
		if !ok {
			continue
		}

		if j.Payload.DurationMs < 0 {
			j.Status = job.StatusFailed
			j.LastError = fmt.Sprintf("invalid duration_ms %d", j.Payload.DurationMs)
			if _, err := p.store.Update(dbCtx, j); err != nil {
				slog.Error("job update failed", "job_id", j.ID, "error", err.Error())
			} else {
				slog.Error("job failed", "job_id", j.ID, "type", j.Type, "error", j.LastError)
			}
			continue
		}

		j.Status = job.StatusRunning
		if _, err := p.store.Update(dbCtx, j); err != nil {
			slog.Error("job update failed", "job_id", j.ID, "error", err.Error())
			continue
		}
		slog.Info("job started", "job_id", j.ID, "type", j.Type, "duration_ms", j.Payload.DurationMs)

		ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
		select {
		case <-time.After(time.Duration(j.Payload.DurationMs) * time.Millisecond):
			cancel()
			j.Status = job.StatusDone
			j.Result = fmt.Sprintf("Slept for %dms", j.Payload.DurationMs)
			if _, err := p.store.Update(dbCtx, j); err != nil {
				slog.Error("job update failed", "job_id", j.ID, "error", err.Error())
			} else {
				slog.Info("job done", "job_id", j.ID, "type", j.Type, "duration_ms", j.Payload.DurationMs)
			}
		case <-ctx.Done():
			cancel()
			j.Status = job.StatusFailed
			j.LastError = "timeout: job exceeded deadline"
			j.Result = nil
			if _, err := p.store.Update(dbCtx, j); err != nil {
				slog.Error("job update failed", "job_id", j.ID, "error", err.Error())
			} else {
				slog.Error("job failed", "job_id", j.ID, "type", j.Type, "error", j.LastError)
			}
		}
	}
}

func (p *Pool) Stop() {
	p.Shutdown()
}

func (p *Pool) Shutdown() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.closeOnce.Do(func() { close(p.queue) })
	p.wg.Wait()
}

func (p *Pool) Submit(id string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return false
	}
	p.queue <- id
	return true
}
