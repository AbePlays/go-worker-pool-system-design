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
	"github.com/AbePlays/go-worker-pool-system-design/internal/dispatcher"
	"github.com/AbePlays/go-worker-pool-system-design/internal/pool"
	"github.com/AbePlays/go-worker-pool-system-design/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	c := loadConfig()

	s := connectStore(c.DatabaseUrl)
	defer s.Close()
	requeueRunning(s)

	p := pool.New(s, c.JobTimeout, c.Workers)
	p.Start()
	slog.Info("server starting", "port", c.Port, "workers", c.Workers, "job_timeout_s", int(c.JobTimeout.Seconds()))

	server := newServer(c.Port, api.New(p, s))
	d := dispatcher.New(p, s, c.Workers)

	serveUntilSignal(server, d, p)
}

func loadConfig() config.Config {
	c, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	return c
}

func connectStore(url string) *store.Store {
	s, err := store.New(context.Background(), url)
	if err != nil {
		log.Fatal(err)
	}

	return s
}

func requeueRunning(s *store.Store) {
	n, err := s.RequeueRunning(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	if n > 0 {
		slog.Info("requeued running jobs", "count", n)
	}
}

func newServer(port string, h *api.Handler) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/jobs", h.CreateJob)
	mux.HandleFunc("GET /api/jobs/{id}", h.GetJob)

	return &http.Server{Addr: ":" + port, Handler: mux}
}

func serveUntilSignal(server *http.Server, d *dispatcher.Dispatcher, p *pool.Pool) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	dispCtx, dispCancel := context.WithCancel(context.Background())
	go d.Run(dispCtx)

	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}

	dispCancel()
	p.Stop()

	slog.Info("server shut down gracefully")
}
