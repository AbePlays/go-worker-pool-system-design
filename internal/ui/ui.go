package ui

import (
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/AbePlays/go-worker-pool-system-design/internal/api"
	"github.com/AbePlays/go-worker-pool-system-design/internal/job"
	"github.com/AbePlays/go-worker-pool-system-design/internal/store"
	"github.com/google/uuid"
)

//go:embed *.html
var files embed.FS

func page(name string) *template.Template {
	return template.Must(template.ParseFS(files, "layout.html", name))
}

var (
	aboutTmpl  = page("about.html")
	dashTmpl   = page("dashboard.html")
	newTmpl    = page("new.html")
	detailTmpl = page("details.html")
)

type Handler struct {
	store       *store.Store
	maxAttempts int
	queueMax    int
}

func New(store *store.Store, maxAttempts, queueMax int) *Handler {
	return &Handler{store: store, maxAttempts: maxAttempts, queueMax: queueMax}
}

func (h *Handler) Dashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	counts, err := h.store.StatusCounts(ctx)
	if err != nil {
		http.Error(w, "store unavailable", http.StatusInternalServerError)
		return
	}
	jobs, err := h.store.Recent(ctx, 50)
	if err != nil {
		http.Error(w, "store unavailable", http.StatusInternalServerError)
		return
	}

	rows := make([]jobRow, 0, len(jobs))
	for _, j := range jobs {
		rows = append(rows, jobRow{
			ID:          j.ID,
			ShortID:     shortID(j.ID),
			Type:        j.Type,
			Status:      string(j.Status),
			Attempts:    j.Attempts,
			MaxAttempts: j.MaxAttempts,
			Age:         roundDur(time.Since(j.CreatedAt)),
		})
	}
	pending := counts["pending"] + counts["running"]
	data := map[string]any{
		"Title":   "Dashboard",
		"Refresh": true,
		"Counts": map[string]int{
			"Pending": counts["pending"],
			"Running": counts["running"],
			"Done":    counts["done"],
			"Failed":  counts["failed"],
		},
		"QueueLen": pending,
		"QueueMax": h.queueMax,
		"QueuePct": min(pending*100/h.queueMax, 100),
		"Jobs":     rows,
	}
	render(dashTmpl, w, data)
}

func (h *Handler) About(w http.ResponseWriter, r *http.Request) {
	render(aboutTmpl, w, map[string]any{"Title": "About"})
}

func (h *Handler) NewForm(w http.ResponseWriter, r *http.Request) {
	render(newTmpl, w, map[string]any{
		"Title":          "New job",
		"IdempotencyKey": uuid.NewString(),
	})
}

func (h *Handler) CreateFromForm(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		formError(w, "could not read form")
		return
	}
	key := r.FormValue("idempotency_key")
	if len(key) > 64 {
		formError(w, "idempotency key too long")
		return
	}

	var req api.CreateJobRequest
	switch r.FormValue("type") {
	case "sleep":
		secs, err := strconv.Atoi(r.FormValue("duration"))
		if err != nil {
			formError(w, "duration must be a number")
			return
		}
		req = api.CreateJobRequest{Type: "sleep", Payload: job.Payload{DurationMs: secs * 1000}}
	case "webhook":
		req = api.CreateJobRequest{Type: "webhook", Payload: job.Payload{Url: r.FormValue("url"), Body: r.FormValue("body")}}
	case "image":
		req = api.CreateJobRequest{Type: "image", Payload: job.Payload{ImageUrl: r.FormValue("image_url")}}
	default:
		formError(w, "unknown job type")
		return
	}

	if err := api.ValidateRequest(req); err != nil {
		formError(w, err.Error())
		return
	}

	queued, err := h.store.PendingCount(r.Context())
	if err != nil {
		http.Error(w, "store unavailable", http.StatusInternalServerError)
		return
	}
	if queued >= h.queueMax {
		if key != "" {
			if existing, found, err := h.store.GetByIdempotencyKey(r.Context(), key); err == nil && found {
				http.Redirect(w, r, "/jobs/"+existing.ID, http.StatusSeeOther)
				return
			}
		}
		formError(w, "queue is full, try again shortly")
		return
	}

	j := job.Job{
		ID:             uuid.NewString(),
		IdempotencyKey: key,
		Type:           req.Type,
		Payload:        req.Payload,
		Status:         job.StatusPending,
		MaxAttempts:    h.maxAttempts,
		NextRunAt:      time.Now(),
	}
	if err := h.store.Save(r.Context(), j); err != nil {
		existing, found, ferr := h.store.GetByIdempotencyKey(r.Context(), key)
		if key != "" && err == store.ErrDuplicate && ferr == nil && found {
			http.Redirect(w, r, "/jobs/"+existing.ID, http.StatusSeeOther)
			return
		}
		slog.Error("job save failed", "job_id", j.ID, "error", err.Error())
		http.Error(w, "store unavailable", http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/jobs/"+j.ID, http.StatusSeeOther)
}

func (h *Handler) Detail(w http.ResponseWriter, r *http.Request) {
	j, ok, err := h.store.GetByID(r.Context(), r.PathValue("id"))
	if err != nil {
		http.Error(w, "store unavailable", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}

	view := detailJob{
		ShortID:     shortID(j.ID),
		Status:      string(j.Status),
		Type:        j.Type,
		Attempts:    j.Attempts,
		MaxAttempts: j.MaxAttempts,
		Error:       j.LastError,
		CreatedAt:   j.CreatedAt.Format("15:04:05 Jan 2"),
		UpdatedAt:   j.UpdatedAt.Format("15:04:05 Jan 2"),
	}
	if j.Status == job.StatusPending && j.NextRunAt.After(time.Now()) {
		view.NextRunText = "in " + roundDur(time.Until(j.NextRunAt))
	}
	switch j.Type {
	case "sleep":
		view.Payload.DurationText = fmt.Sprintf("%dms", j.Payload.DurationMs)
	case "webhook":
		view.Payload.URL = j.Payload.Url
		view.Payload.Body = j.Payload.Body
		if s, ok := j.Result.(string); ok {
			view.ResultText = s
		}
	case "image":
		view.Payload.URL = j.Payload.ImageUrl
		raw, _ := json.Marshal(j.Result)
		var res job.ImageResult
		if err := json.Unmarshal(raw, &res); err == nil && len(res.Renditions) > 0 {
			view.Result.Width = res.OriginalWidth
			view.Result.Height = res.OriginalHeight
			for _, rn := range res.Renditions {
				view.Result.Thumbnails = append(view.Result.Thumbnails, thumbnail{
					DataURL: template.URL("data:image/jpeg;base64," + rn.Data),
					Size:    rn.Width,
				})
			}
		}
	}

	render(detailTmpl, w, map[string]any{
		"Title":   "Job " + view.ShortID,
		"Refresh": j.Status == job.StatusPending || j.Status == job.StatusRunning,
		"Job":     view,
	})
}

type jobRow struct {
	ID, ShortID, Type, Status string
	Attempts, MaxAttempts     int
	Age                       string
}

type detailJob struct {
	ShortID, Status, Type, Error string
	Attempts, MaxAttempts        int
	NextRunText                  string
	CreatedAt, UpdatedAt         string
	Payload                      payloadView
	ResultText                   string
	Result                       resultView
}

type payloadView struct {
	DurationText string
	URL, Body    string
}

type resultView struct {
	Width, Height int
	Thumbnails    []thumbnail
}

type thumbnail struct {
	DataURL template.URL
	Size    int
}

func render(t *template.Template, w http.ResponseWriter, data map[string]any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := t.ExecuteTemplate(w, "layout", data); err != nil {
		slog.Error("template render failed", "error", err.Error())
	}
}

func formError(w http.ResponseWriter, msg string) {
	render(newTmpl, w, map[string]any{
		"Title":          "New job",
		"IdempotencyKey": uuid.NewString(),
		"Error":          msg,
	})
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func roundDur(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
}
