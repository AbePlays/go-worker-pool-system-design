package api

import (
	"encoding/json"
	"net/http"

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

func New(pool *pool.Pool, store *store.Store) *Handler {
	return &Handler{
		pool:  pool,
		store: store,
	}
}

func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	var req CreateJobRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	j := job.Job{
		ID:      uuid.NewString(),
		Type:    req.Type,
		Payload: req.Payload,
		Status:  job.StatusPending,
	}
	h.store.Save(j)
	h.pool.Submit(j.ID)

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

	j, ok := h.store.GetByID(id)
	if !ok {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(j)
}
