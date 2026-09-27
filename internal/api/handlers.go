package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/AbePlays/go-worker-pool-system-design/internal/job"
	"github.com/AbePlays/go-worker-pool-system-design/internal/pool"
	"github.com/AbePlays/go-worker-pool-system-design/internal/store"
	"github.com/AbePlays/go-worker-pool-system-design/utils"
	"github.com/google/uuid"
)

type Handler struct {
	pool        *pool.Pool
	store       *store.Store
	maxAttempts int
	queueMax    int
}

type CreateJobRequest struct {
	Type    string      `json:"type"`
	Payload job.Payload `json:"payload"`
}

const maxBodySize = 64 << 10

func New(pool *pool.Pool, store *store.Store, maxAttempts, queueMax int) *Handler {
	return &Handler{
		pool:        pool,
		store:       store,
		maxAttempts: maxAttempts,
		queueMax:    queueMax,
	}
}

func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)

	idempotencyKey := r.Header.Get("Idempotency-Key")
	if len(idempotencyKey) > 64 {
		http.Error(w, "idempotency key too long", http.StatusBadRequest)
		return
	}

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

	switch req.Type {
	case "sleep":
		if req.Payload.DurationMs < 0 || req.Payload.DurationMs > 30000 {
			http.Error(w, "invalid payload duration", http.StatusBadRequest)
			return
		}
	case "webhook":
		if !utils.ValidateUrl(req.Payload.Url) {
			http.Error(w, "invalid payload url", http.StatusBadRequest)
			return
		}
		if req.Payload.Body != "" && !json.Valid([]byte(req.Payload.Body)) {
			http.Error(w, "invalid payload body: must be valid JSON", http.StatusBadRequest)
			return
		}
	default:
		http.Error(w, "invalid job type", http.StatusBadRequest)
		return
	}

	if full, err := h.queueFull(r); err != nil {
		slog.Error("queue count failed", "error", err.Error())
		http.Error(w, "store unavailable", http.StatusInternalServerError)
		return
	} else if full {
		if idempotencyKey != "" {
			if existing, found, err := h.store.GetByIdempotencyKey(r.Context(), idempotencyKey); err != nil {
				slog.Error("job fetch failed", "error", err.Error())
				http.Error(w, "store unavailable", http.StatusInternalServerError)
				return
			} else if found {
				slog.Info("job deduplicated", "job_id", existing.ID)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]string{"id": existing.ID})
				return
			}
		}
		w.Header().Set("Retry-After", "5")
		http.Error(w, "queue full", http.StatusTooManyRequests)
		return
	}

	j := job.Job{
		ID:             uuid.NewString(),
		IdempotencyKey: idempotencyKey,
		Type:           req.Type,
		Payload:        req.Payload,
		Status:         job.StatusPending,
		MaxAttempts:    h.maxAttempts,
	}

	if err := h.store.Save(r.Context(), j); err != nil {
		if errors.Is(err, store.ErrDuplicate) {
			existing, found, ferr := h.store.GetByIdempotencyKey(r.Context(), idempotencyKey)
			if ferr != nil {
				slog.Error("job fetch failed", "error", ferr.Error())
				http.Error(w, "store unavailable", http.StatusInternalServerError)
				return
			}

			if !found {
				slog.Error("duplicate key without row", "job_id", j.ID)
				http.Error(w, "store unavailable", http.StatusInternalServerError)
				return
			}

			slog.Info("job deduplicated", "job_id", existing.ID)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"id": existing.ID})
			return
		}

		slog.Error("job save failed", "job_id", j.ID, "error", err.Error())
		http.Error(w, "store unavailable", http.StatusInternalServerError)
		return
	}

	slog.Info("job enqueued", "job_id", j.ID, "type", j.Type, "duration_ms", j.Payload.DurationMs)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"id": j.ID})
}

func (h *Handler) queueFull(r *http.Request) (bool, error) {
	n, err := h.store.PendingCount(r.Context())
	if err != nil {
		return false, err
	}
	return n >= h.queueMax, nil
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
