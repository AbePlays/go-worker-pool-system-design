package job

import "time"

type Status string

const (
	StatusPending Status = "pending"
	StatusRunning Status = "running"
	StatusDone    Status = "done"
	StatusFailed  Status = "failed"
)

type Payload struct {
	DurationMs int `json:"duration_ms"`
}

type Job struct {
	Attempts    int       `json:"attempts"`
	CreatedAt   time.Time `json:"created_at"`
	ID          string    `json:"id"`
	LastError   string    `json:"last_error,omitempty"`
	MaxAttempts int       `json:"max_attempts"`
	NextRunAt   time.Time `json:"next_run_at"`
	Payload     Payload   `json:"payload"`
	Result      any       `json:"result,omitempty"`
	Status      Status    `json:"status"`
	Type        string    `json:"type"`
	UpdatedAt   time.Time `json:"updated_at"`
}
