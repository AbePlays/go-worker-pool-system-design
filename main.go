package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AbePlays/go-worker-pool-system-design/internal/api"
	"github.com/AbePlays/go-worker-pool-system-design/internal/config"
	"github.com/AbePlays/go-worker-pool-system-design/internal/pool"
	"github.com/AbePlays/go-worker-pool-system-design/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	c, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	s, err := store.New(context.Background(), c.DatabaseUrl)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	p := pool.New(s, c.JobTimeout, c.Workers)
	p.Start()
	slog.Info("server starting", "port", c.Port, "workers", c.Workers, "job_timeout_s", int(c.JobTimeout.Seconds()))
	h := api.New(p, s)

	mux := http.NewServeMux()

	// Routes
	mux.HandleFunc("POST /api/jobs", h.CreateJob)
	mux.HandleFunc("GET /api/jobs/{id}", h.GetJob)

	server := &http.Server{
		Addr:    ":" + c.Port,
		Handler: mux,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}

	p.Stop()

	slog.Info("server shut down gracefully")
}
