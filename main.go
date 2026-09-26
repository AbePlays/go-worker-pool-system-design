package main

import (
	"log"
	"net/http"

	"github.com/AbePlays/go-worker-pool-system-design/internal/api"
	"github.com/AbePlays/go-worker-pool-system-design/internal/pool"
	"github.com/AbePlays/go-worker-pool-system-design/internal/store"
)

func main() {
	s := store.New()
	p := pool.New(s, 8)
	p.Start()
	h := api.New(p, s)

	mux := http.NewServeMux()

	// Routes
	mux.HandleFunc("POST /api/jobs", h.CreateJob)
	mux.HandleFunc("GET /api/jobs/{id}", h.GetJob)

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}
	log.Fatal(server.ListenAndServe())
}
