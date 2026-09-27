package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AbePlays/go-worker-pool-system-design/internal/dispatcher"
	"github.com/AbePlays/go-worker-pool-system-design/internal/job"
	"github.com/AbePlays/go-worker-pool-system-design/internal/pool"
	"github.com/AbePlays/go-worker-pool-system-design/internal/store"
)

func testDBURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:postgres@localhost:5432/workerpool?sslmode=disable"
}

func newTestSetup(t *testing.T, workers int) (*store.Store, *pool.Pool, *http.ServeMux) {
	return newTestSetupWithLimits(t, workers, 3, 1000)
}

func newTestSetupWithLimits(t *testing.T, workers, maxAttempts, queueMax int) (*store.Store, *pool.Pool, *http.ServeMux) {
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
	p := pool.New(s, 30*time.Second, workers)
	p.Start()
	t.Cleanup(func() { p.Shutdown() })
	dispCtx, dispCancel := context.WithCancel(context.Background())
	t.Cleanup(dispCancel)
	go dispatcher.New(p, s, workers).Run(dispCtx)
	h := New(s, maxAttempts, queueMax)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/jobs", h.CreateJob)
	mux.HandleFunc("GET /api/jobs/{id}", h.GetJob)
	return s, p, mux
}

func TestCreateBadJSON(t *testing.T) {
	_, _, mux := newTestSetup(t, 2)

	req := httptest.NewRequest("POST", "/api/jobs", strings.NewReader("{bad"))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestCreateGoodReturnsID(t *testing.T) {
	_, _, mux := newTestSetup(t, 2)

	body := `{"type":"sleep","payload":{"duration_ms":10}}`
	req := httptest.NewRequest("POST", "/api/jobs", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("bad json response: %v", err)
	}
	if resp["id"] == "" {
		t.Fatalf("expected id field, got %v", resp)
	}
}

func TestGetMissing404(t *testing.T) {
	_, _, mux := newTestSetup(t, 1)

	req := httptest.NewRequest("GET", "/api/jobs/00000000-0000-0000-0000-000000000000", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestFullFlowPendingToDone(t *testing.T) {
	_, _, mux := newTestSetup(t, 2)

	body := `{"type":"sleep","payload":{"duration_ms":30}}`
	req := httptest.NewRequest("POST", "/api/jobs", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}
	var created map[string]string
	_ = json.NewDecoder(rec.Body).Decode(&created)
	id := created["id"]

	deadline := time.Now().Add(10 * time.Second)
	for {
		req := httptest.NewRequest("GET", "/api/jobs/"+id, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		var got job.Job
		_ = json.NewDecoder(rec.Body).Decode(&got)
		if got.Status == job.StatusDone {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for done, last=%+v", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCreateOversize413(t *testing.T) {
	_, _, mux := newTestSetup(t, 2)

	big := strings.Repeat("a", (64<<10)+1000)
	body := `{"type":"sleep","payload":{"duration_ms":10},"pad":"` + big + `"}`
	req := httptest.NewRequest("POST", "/api/jobs", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rec.Code)
	}
}

func TestCreateUnknownType400(t *testing.T) {
	_, _, mux := newTestSetup(t, 2)

	for _, body := range []string{
		`{"type":"webhook","payload":{"duration_ms":10}}`,
		`{"type":"","payload":{"duration_ms":10}}`,
		`{}`,
	} {
		req := httptest.NewRequest("POST", "/api/jobs", strings.NewReader(body))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for %s, got %d", body, rec.Code)
		}
	}
}

func TestCreateBadDuration400(t *testing.T) {
	_, _, mux := newTestSetup(t, 2)

	for _, body := range []string{
		`{"type":"sleep","payload":{"duration_ms":-5}}`,
		`{"type":"sleep","payload":{"duration_ms":30001}}`,
	} {
		req := httptest.NewRequest("POST", "/api/jobs", strings.NewReader(body))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for %s, got %d", body, rec.Code)
		}
	}
}

func TestCreateAfterShutdownAccepted(t *testing.T) {
	_, p, mux := newTestSetup(t, 2)
	p.Shutdown()

	body := `{"type":"sleep","payload":{"duration_ms":10}}`
	req := httptest.NewRequest("POST", "/api/jobs", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 persisted after shutdown, got %d", rec.Code)
	}
}

func TestCreateWebhookGood202(t *testing.T) {
	_, _, mux := newTestSetup(t, 2)

	body := `{"type":"webhook","payload":{"url":"https://example.com/hook","body":"{\"k\":\"v\"}"}}`
	req := httptest.NewRequest("POST", "/api/jobs", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestCreateWebhookBadURL400(t *testing.T) {
	_, _, mux := newTestSetup(t, 2)

	for _, body := range []string{
		`{"type":"webhook","payload":{}}`,
		`{"type":"webhook","payload":{"url":""}}`,
		`{"type":"webhook","payload":{"url":"ftp://example.com/f"}}`,
		`{"type":"webhook","payload":{"url":"http://127.0.0.1/hook"}}`,
		`{"type":"webhook","payload":{"url":"http://localhost/hook"}}`,
		`{"type":"webhook","payload":{"url":"https://example.com/hook","body":"{broken"}}`,
	} {
		req := httptest.NewRequest("POST", "/api/jobs", strings.NewReader(body))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for %s, got %d", body, rec.Code)
		}
	}
}

func postJob(t *testing.T, mux *http.ServeMux, key, body string) (int, string) {
	t.Helper()
	req := httptest.NewRequest("POST", "/api/jobs", strings.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	var resp map[string]string
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	return rec.Code, resp["id"]
}

func TestIdempotentSameKey(t *testing.T) {
	_, _, mux := newTestSetup(t, 2)
	body := `{"type":"sleep","payload":{"duration_ms":10}}`

	code, first := postJob(t, mux, "key-abc", body)
	if code != http.StatusAccepted || first == "" {
		t.Fatalf("expected 202 + id, got %d %q", code, first)
	}
	code, second := postJob(t, mux, "key-abc", body)
	if code != http.StatusOK || second != first {
		t.Fatalf("expected 200 same id, got %d %q vs %q", code, second, first)
	}
}

func TestIdempotentDifferentKeys(t *testing.T) {
	_, _, mux := newTestSetup(t, 2)
	body := `{"type":"sleep","payload":{"duration_ms":10}}`

	_, first := postJob(t, mux, "key-one", body)
	_, second := postJob(t, mux, "key-two", body)
	if first == second {
		t.Fatalf("expected distinct ids, got %q twice", first)
	}
}

func TestIdempotentNoKeyDuplicates(t *testing.T) {
	_, _, mux := newTestSetup(t, 2)
	body := `{"type":"sleep","payload":{"duration_ms":10}}`

	_, first := postJob(t, mux, "", body)
	_, second := postJob(t, mux, "", body)
	if first == second {
		t.Fatalf("expected distinct ids without key, got %q twice", first)
	}
}

func TestIdempotentKeyTooLong(t *testing.T) {
	_, _, mux := newTestSetup(t, 2)
	code, _ := postJob(t, mux, strings.Repeat("k", 65), `{"type":"sleep","payload":{"duration_ms":10}}`)
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", code)
	}
}

func TestIdempotentConcurrentSameKey(t *testing.T) {
	_, _, mux := newTestSetup(t, 2)
	body := `{"type":"sleep","payload":{"duration_ms":10}}`

	const n = 10
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ids[i] = postJob(t, mux, "key-race", body)
		}()
	}
	wg.Wait()
	for _, id := range ids[1:] {
		if id != ids[0] || ids[0] == "" {
			t.Fatalf("expected single id, got %q vs %q", ids[0], id)
		}
	}
}

func TestQueueFull429(t *testing.T) {
	_, _, mux := newTestSetupWithLimits(t, 1, 3, 1)
	body := `{"type":"sleep","payload":{"duration_ms":10}}`

	code, _ := postJob(t, mux, "", body)
	if code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", code)
	}
	req := httptest.NewRequest("POST", "/api/jobs", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") != "5" {
		t.Fatalf("expected Retry-After 5, got %q", rec.Header().Get("Retry-After"))
	}
}

func TestQueueFullDupeBypass(t *testing.T) {
	_, _, mux := newTestSetupWithLimits(t, 1, 3, 1)
	body := `{"type":"sleep","payload":{"duration_ms":10}}`

	code, first := postJob(t, mux, "key-cap", body)
	if code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", code)
	}
	code, second := postJob(t, mux, "key-cap", body)
	if code != http.StatusOK || second != first {
		t.Fatalf("expected 200 same id under full queue, got %d %q", code, second)
	}
}

func TestQueueFullInvalidStill400(t *testing.T) {
	_, _, mux := newTestSetupWithLimits(t, 1, 3, 1)
	code, _ := postJob(t, mux, "", `{"type":"sleep","payload":{"duration_ms":10}}`)
	if code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", code)
	}
	code, _ = postJob(t, mux, "", `{"type":"nope","payload":{}}`)
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid under full queue, got %d", code)
	}
}
