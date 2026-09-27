package pool

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
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

func newTestStore(t *testing.T) *store.Store {
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

func mustSave(t *testing.T, s *store.Store, j job.Job) {
	t.Helper()
	if err := s.Save(context.Background(), j); err != nil {
		t.Fatalf("save %s: %v", j.ID, err)
	}
}

func waitForCond(t *testing.T, s *store.Store, id, desc string, timeout time.Duration, match func(job.Job) bool) job.Job {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for {
		got, ok, err := s.GetByID(ctx, id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if ok && match(got) {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s %s, got %+v", id, desc, got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitFor(t *testing.T, s *store.Store, id string, want job.Status, timeout time.Duration) job.Job {
	return waitForCond(t, s, id, "status="+string(want), timeout, func(got job.Job) bool {
		return got.Status == want
	})
}

func waitForAttempts(t *testing.T, s *store.Store, id string, want int, timeout time.Duration) job.Job {
	return waitForCond(t, s, id, fmt.Sprintf("attempts=%d", want), timeout, func(got job.Job) bool {
		return got.Attempts == want
	})
}

func newJob(dur int) job.Job {
	return job.Job{ID: uuid.NewString(), Type: "sleep", Payload: job.Payload{DurationMs: dur}, Status: job.StatusPending}
}

func TestSingleJobDone(t *testing.T) {
	s := newTestStore(t)
	p := New(s, 30*time.Second, 2)
	p.Start()
	defer p.Shutdown()

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
	defer p.Shutdown()

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
	defer p.Shutdown()

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
	defer p.Shutdown()

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
	defer p.Shutdown()

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
	p.Shutdown() // must not panic
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

func TestBackoffForAttempt(t *testing.T) {
	cases := map[int]time.Duration{
		1:  2 * time.Second,
		2:  4 * time.Second,
		3:  8 * time.Second,
		4:  16 * time.Second,
		5:  32 * time.Second,
		6:  60 * time.Second,
		10: 60 * time.Second,
	}
	for attempt, want := range cases {
		if got := backoffForAttempt(attempt); got != want {
			t.Fatalf("attempt %d: expected %v, got %v", attempt, want, got)
		}
	}
}

func TestRetryScheduled(t *testing.T) {
	s := newTestStore(t)
	p := New(s, 30*time.Second, 1)
	p.Start()
	defer p.Shutdown()

	j := newJob(-5)
	j.MaxAttempts = 3
	mustSave(t, s, j)
	p.Submit(j.ID)

	got := waitForAttempts(t, s, j.ID, 1, 5*time.Second)
	if got.Status != job.StatusPending {
		t.Fatalf("expected pending retry, got %s", got.Status)
	}
	if !got.NextRunAt.After(time.Now()) {
		t.Fatalf("expected future next_run_at, got %v", got.NextRunAt)
	}
	if got.LastError == "" {
		t.Fatal("expected LastError on retry")
	}
}

func TestRetryExhausted(t *testing.T) {
	s := newTestStore(t)
	p := New(s, 30*time.Second, 1)
	p.Start()
	defer p.Shutdown()

	j := newJob(-5)
	j.MaxAttempts = 1
	mustSave(t, s, j)
	p.Submit(j.ID)

	got := waitFor(t, s, j.ID, job.StatusFailed, 5*time.Second)
	if got.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", got.Attempts)
	}
}

func newWebhookJob(url string, maxAttempts int) job.Job {
	return job.Job{
		ID:          uuid.NewString(),
		Type:        "webhook",
		Payload:     job.Payload{Url: url, Body: `{"hello":"world"}`},
		Status:      job.StatusPending,
		MaxAttempts: maxAttempts,
	}
}

func TestWebhookSuccess(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	s := newTestStore(t)
	p := New(s, 30*time.Second, 1)
	p.Start()
	defer p.Shutdown()

	j := newWebhookJob(srv.URL, 3)
	mustSave(t, s, j)
	p.Submit(j.ID)

	got := waitFor(t, s, j.ID, job.StatusDone, 5*time.Second)
	if got.Attempts != 0 {
		t.Fatalf("expected 0 attempts, got %d", got.Attempts)
	}
	if !strings.Contains(string(gotBody), j.ID) {
		t.Fatalf("expected delivery to include job id, got %s", string(gotBody))
	}
}

func TestWebhookExhausted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	s := newTestStore(t)
	p := New(s, 30*time.Second, 1)
	p.Start()
	defer p.Shutdown()

	j := newWebhookJob(srv.URL, 1)
	mustSave(t, s, j)
	p.Submit(j.ID)

	got := waitFor(t, s, j.ID, job.StatusFailed, 5*time.Second)
	if got.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", got.Attempts)
	}
	if !strings.Contains(got.LastError, "500") {
		t.Fatalf("expected status in error, got %q", got.LastError)
	}
}

func TestWebhookRetryThenSucceed(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "flaky", http.StatusBadGateway)
			return
		}
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	s := newTestStore(t)
	p := New(s, 30*time.Second, 1)
	p.Start()
	defer p.Shutdown()

	j := newWebhookJob(srv.URL, 3)
	mustSave(t, s, j)
	p.Submit(j.ID)

	retry := waitForAttempts(t, s, j.ID, 1, 5*time.Second)
	if retry.Status != job.StatusPending {
		t.Fatalf("expected pending retry, got %s", retry.Status)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		got, _, _ := s.GetByID(context.Background(), j.ID)
		if time.Now().After(got.NextRunAt) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retry never became due")
		}
		time.Sleep(50 * time.Millisecond)
	}
	claimed, err := s.Claim(context.Background(), 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("re-claim due retry: %v n=%d", err, len(claimed))
	}
	p.Submit(j.ID)

	got := waitFor(t, s, j.ID, job.StatusDone, 5*time.Second)
	if got.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", got.Attempts)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected 2 deliveries, got %d", calls.Load())
	}
}

func testPNG(t *testing.T, w, h int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.Set(x, y, color.RGBA{uint8(x % 256), uint8(y % 256), 128, 255})
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encode test png: %v", err)
	}
	return buf.Bytes()
}

func TestImageSuccess(t *testing.T) {
	raw := testPNG(t, 1000, 500)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Write(raw)
	}))
	defer srv.Close()

	s := newTestStore(t)
	p := New(s, 30*time.Second, 1)
	p.Start()
	defer p.Shutdown()

	j := job.Job{ID: uuid.NewString(), Type: "image", Payload: job.Payload{ImageUrl: srv.URL}, Status: job.StatusPending, MaxAttempts: 3}
	mustSave(t, s, j)
	p.Submit(j.ID)

	got := waitFor(t, s, j.ID, job.StatusDone, 10*time.Second)
	if got.Attempts != 0 {
		t.Fatalf("expected 0 attempts, got %d", got.Attempts)
	}

	stored, ok, err := s.GetByID(context.Background(), j.ID)
	if err != nil || !ok {
		t.Fatalf("get: %v found=%v", err, ok)
	}
	resJSON, _ := json.Marshal(stored.Result)
	var res job.ImageResult
	if err := json.Unmarshal(resJSON, &res); err != nil {
		t.Fatalf("result shape: %v (%s)", err, string(resJSON))
	}
	if res.OriginalWidth != 1000 || res.OriginalHeight != 500 {
		t.Fatalf("bad original dims: %+v", res)
	}
	want := []struct {
		w, q int
	}{{800, 75}, {400, 50}, {200, 25}}
	if len(res.Renditions) != 3 {
		t.Fatalf("expected 3 renditions, got %d", len(res.Renditions))
	}
	for i, r := range res.Renditions {
		if r.Width != want[i].w || r.Quality != want[i].q || r.Height != 500*r.Width/1000 {
			t.Fatalf("bad rendition %d: %+v", i, r)
		}
		data, err := base64.StdEncoding.DecodeString(r.Data)
		if err != nil || len(data) < 2 || data[0] != 0xFF || data[1] != 0xD8 {
			t.Fatalf("rendition %d not a jpeg", i)
		}
	}
}

func TestImageCorruptPermanent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("this is not an image"))
	}))
	defer srv.Close()

	s := newTestStore(t)
	p := New(s, 30*time.Second, 1)
	p.Start()
	defer p.Shutdown()

	j := job.Job{ID: uuid.NewString(), Type: "image", Payload: job.Payload{ImageUrl: srv.URL}, Status: job.StatusPending, MaxAttempts: 3}
	mustSave(t, s, j)
	p.Submit(j.ID)

	got := waitFor(t, s, j.ID, job.StatusFailed, 5*time.Second)
	if got.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", got.Attempts)
	}
	if got.LastError == "" {
		t.Fatal("expected LastError")
	}
}
