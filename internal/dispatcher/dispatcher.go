package dispatcher

import (
	"context"
	"log/slog"
	"time"

	"github.com/AbePlays/go-worker-pool-system-design/internal/pool"
	"github.com/AbePlays/go-worker-pool-system-design/internal/store"
)

type Dispatcher struct {
	pollEmpty time.Duration
	pollFull  time.Duration
	pool      *pool.Pool
	store     *store.Store
	workers   int
}

func New(pool *pool.Pool, store *store.Store, workers int) *Dispatcher {
	return &Dispatcher{
		pollEmpty: 2 * time.Second,
		pollFull:  500 * time.Millisecond,
		pool:      pool,
		store:     store,
		workers:   workers,
	}
}

func (d *Dispatcher) Run(ctx context.Context) {
	slog.Info("dispatcher started", "workers", d.workers)
	for {
		select {
		case <-ctx.Done():
			slog.Info("dispatcher stopped")
			return
		default:
		}

		jobs, err := d.store.Claim(ctx, d.workers)
		if ctx.Err() != nil {
			slog.Info("dispatcher stopped")
			return
		}

		if err != nil {
			slog.Error("dispatcher claim failed", "error", err.Error())
			select {
			case <-ctx.Done():
				slog.Info("dispatcher stopped")
				return
			case <-time.After(d.pollEmpty):
			}
			continue
		}

		if len(jobs) > 0 {
			slog.Info("dispatcher claimed", "count", len(jobs))
		}

		for _, job := range jobs {
			if !d.pool.Submit(job.ID) {
				slog.Info("dispatcher stopped")
				return
			}
		}

		wait := d.pollEmpty
		if len(jobs) == d.workers {
			wait = d.pollFull
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}
