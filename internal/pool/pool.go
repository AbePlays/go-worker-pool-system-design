package pool

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	_ "image/png"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"golang.org/x/image/draw"

	"github.com/AbePlays/go-worker-pool-system-design/internal/job"
	"github.com/AbePlays/go-worker-pool-system-design/internal/store"
)

var imageLadder = []struct {
	width   int
	quality int
}{
	{800, 75},
	{400, 50},
	{200, 25},
}

const (
	maxImageBytes     = 2 << 20
	maxImagePixels    = 8000
	maxRenditionBytes = 512 << 10
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

func backoffForAttempt(attempt int) time.Duration {
	d := 2 * time.Second
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= time.Minute {
			return time.Minute
		}
	}

	return d
}

func (p *Pool) fail(j job.Job, reason string) {
	j.Attempts++
	j.LastError = reason
	j.Result = nil

	if j.MaxAttempts <= 0 || j.Attempts >= j.MaxAttempts {
		j.Status = job.StatusFailed
		if _, err := p.store.Update(context.Background(), j); err != nil {
			slog.Error("job update failed", "job_id", j.ID, "error", err.Error())
		} else {
			slog.Error("job failed", "job_id", j.ID, "type", j.Type, "error", j.LastError, "attempts", j.Attempts)
		}
		return
	}

	j.Status = job.StatusPending
	j.NextRunAt = time.Now().Add(backoffForAttempt(j.Attempts))
	if _, err := p.store.Update(context.Background(), j); err != nil {
		slog.Error("job update failed", "job_id", j.ID, "error", err.Error())
	} else {
		slog.Info("job retry scheduled", "job_id", j.ID, "type", j.Type, "attempt", j.Attempts, "next_run_at", j.NextRunAt)
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

		switch j.Type {
		case "webhook":
			p.runWebhook(j)
			continue
		case "image":
			p.runImage(j)
			continue
		case "sleep":
		default:
			p.fail(j, fmt.Sprintf("unknown job type %q", j.Type))
			continue
		}

		if j.Payload.DurationMs < 0 {
			p.fail(j, fmt.Sprintf("invalid duration_ms %d", j.Payload.DurationMs))
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
			p.fail(j, "timeout: job exceeded deadline")
		}
	}
}

func (p *Pool) runWebhook(j job.Job) {
	dbCtx := context.Background()

	j.Status = job.StatusRunning
	if _, err := p.store.Update(dbCtx, j); err != nil {
		slog.Error("job update failed", "job_id", j.ID, "error", err.Error())
		return
	}
	slog.Info("job started", "job_id", j.ID, "type", j.Type, "url", j.Payload.Url)

	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	body, err := json.Marshal(map[string]string{"id": j.ID, "body": j.Payload.Body})
	if err != nil {
		p.fail(j, fmt.Sprintf("webhook encode failed: %v", err))
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, j.Payload.Url, bytes.NewReader(body))
	if err != nil {
		p.fail(j, fmt.Sprintf("webhook request failed: %v", err))
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		p.fail(j, fmt.Sprintf("webhook delivery failed: %v", err))
		return
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		p.fail(j, fmt.Sprintf("webhook bad status: %d", resp.StatusCode))
		return
	}

	j.Status = job.StatusDone
	j.Result = fmt.Sprintf("POST %d: %s", resp.StatusCode, string(respBody))
	j.LastError = ""
	if _, err := p.store.Update(dbCtx, j); err != nil {
		slog.Error("job update failed", "job_id", j.ID, "error", err.Error())
	} else {
		slog.Info("job done", "job_id", j.ID, "type", j.Type, "status_code", resp.StatusCode)
	}
}

func (p *Pool) failPermanent(j job.Job, reason string) {
	j.Attempts++
	j.Status = job.StatusFailed
	j.LastError = reason
	j.Result = nil
	if _, err := p.store.Update(context.Background(), j); err != nil {
		slog.Error("job update failed", "job_id", j.ID, "error", err.Error())
	} else {
		slog.Error("job failed", "job_id", j.ID, "type", j.Type, "error", j.LastError, "attempts", j.Attempts)
	}
}

func (p *Pool) runImage(j job.Job) {
	dbCtx := context.Background()

	j.Status = job.StatusRunning
	if _, err := p.store.Update(dbCtx, j); err != nil {
		slog.Error("job update failed", "job_id", j.ID, "error", err.Error())
		return
	}
	slog.Info("job started", "job_id", j.ID, "type", j.Type, "image_url", j.Payload.ImageUrl)

	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, j.Payload.ImageUrl, nil)
	if err != nil {
		p.failPermanent(j, fmt.Sprintf("image request failed: %v", err))
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		p.failPermanent(j, fmt.Sprintf("image fetch failed: %v", err))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		p.failPermanent(j, fmt.Sprintf("image bad status: %d", resp.StatusCode))
		return
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxImageBytes+1))
	if err != nil {
		p.failPermanent(j, fmt.Sprintf("image read failed: %v", err))
		return
	}
	if len(raw) > maxImageBytes {
		p.failPermanent(j, "image too large")
		return
	}

	cfg, _, err := image.DecodeConfig(bytes.NewReader(raw))
	if err != nil {
		p.failPermanent(j, fmt.Sprintf("image decode failed: %v", err))
		return
	}
	if cfg.Width <= 0 || cfg.Height <= 0 || cfg.Width > maxImagePixels || cfg.Height > maxImagePixels {
		p.failPermanent(j, fmt.Sprintf("image dimensions rejected: %dx%d", cfg.Width, cfg.Height))
		return
	}
	src, _, err := image.Decode(bytes.NewReader(raw))
	if err != nil {
		p.failPermanent(j, fmt.Sprintf("image decode failed: %v", err))
		return
	}

	renditions := make([]job.Rendition, 0, len(imageLadder))
	for _, rung := range imageLadder {
		w := min(rung.width, cfg.Width)
		h := max(cfg.Height*w/cfg.Width, 1)
		dst := image.NewRGBA(image.Rect(0, 0, w, h))
		draw.ApproxBiLinear.Scale(dst, dst.Bounds(), src, src.Bounds(), draw.Over, nil)
		var buf bytes.Buffer
		if err := jpeg.Encode(&buf, dst, &jpeg.Options{Quality: rung.quality}); err != nil {
			p.failPermanent(j, fmt.Sprintf("image encode failed: %v", err))
			return
		}
		if buf.Len() > maxRenditionBytes {
			p.failPermanent(j, "image rendition too large")
			return
		}
		renditions = append(renditions, job.Rendition{
			Quality: rung.quality,
			Width:   w,
			Height:  h,
			Data:    base64.StdEncoding.EncodeToString(buf.Bytes()),
		})
	}

	j.Status = job.StatusDone
	j.Result = job.ImageResult{OriginalWidth: cfg.Width, OriginalHeight: cfg.Height, Renditions: renditions}
	j.LastError = ""
	if _, err := p.store.Update(dbCtx, j); err != nil {
		slog.Error("job update failed", "job_id", j.ID, "error", err.Error())
	} else {
		slog.Info("job done", "job_id", j.ID, "type", j.Type, "renditions", len(renditions))
	}
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
