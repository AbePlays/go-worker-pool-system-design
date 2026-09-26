package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/AbePlays/go-worker-pool-system-design/internal/job"
	"github.com/AbePlays/go-worker-pool-system-design/internal/pool"
	"github.com/AbePlays/go-worker-pool-system-design/internal/store"
	"github.com/google/uuid"
)

type Handler struct {
	pool  *pool.Pool
	store *store.Store
}

type CreateJobRequest struct {
	Type    string      `json:"type"`
	Payload job.Payload `json:"payload"`
}

const maxBodySize = 64 << 10

func New(pool *pool.Pool, store *store.Store) *Handler {
	return &Handler{
		pool:  pool,
		store: store,
	}
}

func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)

	var req CreateJobRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		if strings.Contains(err.Error(), "request body too large") {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}

	if req.Type != "sleep" {
		http.Error(w, "invalid job type", http.StatusBadRequest)
		return
	}

	if req.Payload.DurationMs < 0 || req.Payload.DurationMs > 30000 {
		http.Error(w, "invalid payload duration", http.StatusBadRequest)
		return
	}

	j := job.Job{
		ID:      uuid.NewString(),
		Type:    req.Type,
		Payload: req.Payload,
		Status:  job.StatusPending,
	}
	if err := h.store.Save(r.Context(), j); err != nil {
		slog.Error("job save failed", "job_id", j.ID, "error", err.Error())
		http.Error(w, "store unavailable", http.StatusInternalServerError)
		return
	}

	slog.Info("job enqueued", "job_id", j.ID, "type", j.Type, "duration_ms", j.Payload.DurationMs)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"id": j.ID})
}

func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	j, ok, err := h.store.GetByID(r.Context(), id)
	if err != nil {
		slog.Error("job fetch failed", "job_id", id, "error", err.Error())
		http.Error(w, "store unavailable", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(j)
}
