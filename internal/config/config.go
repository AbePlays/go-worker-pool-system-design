package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseUrl string
	JobTimeout  time.Duration
	MaxAttempts int
	Port        string
	QueueMax    int
	Workers     int
}

func Load() (Config, error) {
	port := os.Getenv("PORT")
	if port == "" {
		return Config{}, fmt.Errorf("PORT environment variable not set")
	}

	timeoutStr := os.Getenv("JOB_TIMEOUT")
	if timeoutStr == "" {
		return Config{}, fmt.Errorf("JOB_TIMEOUT environment variable not set")
	}
	timeout, err := strconv.Atoi(timeoutStr)
	if err != nil {
		return Config{}, fmt.Errorf("JOB_TIMEOUT must be integer seconds: %w", err)
	}
	if timeout <= 0 {
		return Config{}, fmt.Errorf("JOB_TIMEOUT must be > 0, got %d", timeout)
	}

	workersStr := os.Getenv("WORKERS")
	if workersStr == "" {
		return Config{}, fmt.Errorf("WORKERS environment variable not set")
	}
	workers, err := strconv.Atoi(workersStr)
	if err != nil {
		return Config{}, fmt.Errorf("WORKERS must be integer: %w", err)
	}
	if workers <= 0 || workers > 100 {
		return Config{}, fmt.Errorf("WORKERS must be 1..100, got %d", workers)
	}

	databaseUrl := os.Getenv("DATABASE_URL")
	if databaseUrl == "" {
		return Config{}, fmt.Errorf("DATABASE_URL environment variable not set")
	}

	maxAttemptsStr := os.Getenv("MAX_ATTEMPTS")
	if maxAttemptsStr == "" {
		return Config{}, fmt.Errorf("MAX_ATTEMPTS environment variable not set")
	}
	maxAttempts, err := strconv.Atoi(maxAttemptsStr)
	if err != nil {
		return Config{}, fmt.Errorf("MAX_ATTEMPTS must be integer: %w", err)
	}
	if maxAttempts <= 0 || maxAttempts > 10 {
		return Config{}, fmt.Errorf("MAX_ATTEMPTS must be 1..10, got %d", maxAttempts)
	}

	queueMaxStr := os.Getenv("QUEUE_MAX")
	if queueMaxStr == "" {
		return Config{}, fmt.Errorf("QUEUE_MAX environment variable not set")
	}
	queueMax, err := strconv.Atoi(queueMaxStr)
	if err != nil {
		return Config{}, fmt.Errorf("QUEUE_MAX must be integer: %w", err)
	}
	if queueMax <= 0 || queueMax > 100000 {
		return Config{}, fmt.Errorf("QUEUE_MAX must be 1..100000, got %d", queueMax)
	}

	return Config{
		DatabaseUrl: databaseUrl,
		JobTimeout:  time.Duration(timeout) * time.Second,
		MaxAttempts: maxAttempts,
		Port:        port,
		QueueMax:    queueMax,
		Workers:     workers,
	}, nil
}
