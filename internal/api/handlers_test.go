package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

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

func newTestSetup(t *testing.T, workers int) (*store.Postgres, *pool.Pool, *http.ServeMux) {
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
	t.Cleanup(func() { p.Stop() })
	h := New(p, s)
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

func TestCreateAfterShutdown503(t *testing.T) {
	s, p, mux := newTestSetup(t, 2)
	p.Shutdown()

	body := `{"type":"sleep","payload":{"duration_ms":10}}`
	req := httptest.NewRequest("POST", "/api/jobs", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") != "5" {
		t.Fatalf("expected Retry-After 5, got %q", rec.Header().Get("Retry-After"))
	}
	_ = s
}
