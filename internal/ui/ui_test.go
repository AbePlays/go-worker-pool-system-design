package ui

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/AbePlays/go-worker-pool-system-design/internal/job"
	"github.com/AbePlays/go-worker-pool-system-design/internal/store"
	"github.com/google/uuid"
)

func testURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://postgres:postgres@localhost:5432/workerpool?sslmode=disable"
}

func newTestHandler(t *testing.T) (*Handler, *store.Store) {
	t.Helper()
	ctx := context.Background()
	s, err := store.New(ctx, testURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.Truncate(ctx); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return New(s, 3, 1000), s
}

func TestDashboard200(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	h.Dashboard(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Queue depth") {
		t.Fatal("expected dashboard content")
	}
}

func TestNewForm200(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/new", nil)
	rec := httptest.NewRecorder()
	h.NewForm(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	for _, want := range []string{"Sleep", "Webhook", "Image", "idempotency_key"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("expected %q in form", want)
		}
	}
}

func TestAbout200(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/about", nil)
	rec := httptest.NewRecorder()
	h.About(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	for _, want := range []string{"Sleep", "Webhook", "Image", "1000"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("expected %q in about", want)
		}
	}
}

func TestDetail404(t *testing.T) {
	h, _ := newTestHandler(t)
	req := httptest.NewRequest("GET", "/jobs/00000000-0000-0000-0000-000000000000", nil)
	req.SetPathValue("id", "00000000-0000-0000-0000-000000000000")
	rec := httptest.NewRecorder()
	h.Detail(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestDetail200(t *testing.T) {
	h, s := newTestHandler(t)
	j := job.Job{ID: uuid.NewString(), Type: "sleep", Payload: job.Payload{DurationMs: 10}, Status: job.StatusDone, MaxAttempts: 3}
	if err := s.Save(context.Background(), j); err != nil {
		t.Fatalf("save: %v", err)
	}
	req := httptest.NewRequest("GET", "/jobs/"+j.ID, nil)
	req.SetPathValue("id", j.ID)
	rec := httptest.NewRecorder()
	h.Detail(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "done") {
		t.Fatal("expected status in detail")
	}
}

func TestCreateFromFormRedirect(t *testing.T) {
	h, _ := newTestHandler(t)
	form := url.Values{"type": {"sleep"}, "duration": {"1"}, "idempotency_key": {"ui-test-1"}}
	req := httptest.NewRequest("POST", "/new", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.CreateFromForm(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("expected 303, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.HasPrefix(rec.Header().Get("Location"), "/jobs/") {
		t.Fatalf("expected redirect to detail, got %q", rec.Header().Get("Location"))
	}
}

func TestCreateFromFormBadDuration(t *testing.T) {
	h, _ := newTestHandler(t)
	form := url.Values{"type": {"sleep"}, "duration": {"99999"}}
	req := httptest.NewRequest("POST", "/new", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.CreateFromForm(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with error, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "invalid payload duration") {
		t.Fatalf("expected validation error, got %s", rec.Body.String())
	}
}

func TestDetailImageRendersDataURLs(t *testing.T) {
	h, s := newTestHandler(t)
	res := job.ImageResult{
		OriginalWidth: 1000, OriginalHeight: 500,
		Renditions: []job.Rendition{
			{Quality: 75, Width: 800, Height: 400, Data: base64.StdEncoding.EncodeToString([]byte{0xFF, 0xD8, 0xFF, 0x00})},
		},
	}
	j := job.Job{ID: uuid.NewString(), Type: "image", Payload: job.Payload{ImageUrl: "https://example.com/p.jpg"}, Status: job.StatusPending, MaxAttempts: 3}
	if err := s.Save(context.Background(), j); err != nil {
		t.Fatalf("save: %v", err)
	}
	j.Status = job.StatusDone
	j.Result = res
	if _, err := s.Update(context.Background(), j); err != nil {
		t.Fatalf("update: %v", err)
	}
	req := httptest.NewRequest("GET", "/jobs/"+j.ID, nil)
	req.SetPathValue("id", j.ID)
	rec := httptest.NewRecorder()
	h.Detail(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "ZgotmplZ") {
		t.Fatal("template neutered the image URL")
	}
	if !strings.Contains(body, `src="data:image/jpeg;base64,`) {
		t.Fatal("expected data URL image in detail")
	}
}
