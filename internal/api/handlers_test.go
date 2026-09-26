package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AbePlays/go-worker-pool-system-design/internal/job"
	"github.com/AbePlays/go-worker-pool-system-design/internal/pool"
	"github.com/AbePlays/go-worker-pool-system-design/internal/store"
)

func newTestMux(s *store.Store, p *pool.Pool) *http.ServeMux {
	h := New(p, s)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/jobs", h.CreateJob)
	mux.HandleFunc("GET /api/jobs/{id}", h.GetJob)
	return mux
}

func TestCreateBadJSON(t *testing.T) {
	s := store.New()
	p := pool.New(s, 30*time.Second, 2)
	p.Start()
	defer p.Stop()
	mux := newTestMux(s, p)

	req := httptest.NewRequest("POST", "/api/jobs", strings.NewReader("{bad"))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestCreateGoodReturnsID(t *testing.T) {
	s := store.New()
	p := pool.New(s, 30*time.Second, 2)
	p.Start()
	defer p.Stop()
	mux := newTestMux(s, p)

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
	s := store.New()
	p := pool.New(s, 30*time.Second, 1)
	p.Start()
	defer p.Stop()
	mux := newTestMux(s, p)

	req := httptest.NewRequest("GET", "/api/jobs/nope", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestFullFlowPendingToDone(t *testing.T) {
	s := store.New()
	p := pool.New(s, 30*time.Second, 2)
	p.Start()
	defer p.Stop()
	mux := newTestMux(s, p)

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

	deadline := time.Now().Add(3 * time.Second)
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
	s := store.New()
	p := pool.New(s, 30*time.Second, 2)
	p.Start()
	defer p.Stop()
	mux := newTestMux(s, p)

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
	s := store.New()
	p := pool.New(s, 30*time.Second, 2)
	p.Start()
	defer p.Stop()
	mux := newTestMux(s, p)

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
	s := store.New()
	p := pool.New(s, 30*time.Second, 2)
	p.Start()
	defer p.Stop()
	mux := newTestMux(s, p)

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
