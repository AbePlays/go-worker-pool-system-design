package main

import (
	"log"
	"net/http"

	"github.com/AbePlays/go-worker-pool-system-design/internal/api"
	"github.com/AbePlays/go-worker-pool-system-design/internal/config"
	"github.com/AbePlays/go-worker-pool-system-design/internal/pool"
	"github.com/AbePlays/go-worker-pool-system-design/internal/store"
)

func main() {
	c, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	s := store.New()
	p := pool.New(s, c.Workers)
	p.Start()
	h := api.New(p, s)

	mux := http.NewServeMux()

	// Routes
	mux.HandleFunc("POST /api/jobs", h.CreateJob)
	mux.HandleFunc("GET /api/jobs/{id}", h.GetJob)

	server := &http.Server{
		Addr:    ":" + c.Port,
		Handler: mux,
	}
	log.Fatal(server.ListenAndServe())
}
