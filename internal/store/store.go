package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AbePlays/go-worker-pool-system-design/internal/job"
)

var ErrDuplicate = errors.New("duplicate idempotency key")

type Store struct {
	db *pgxpool.Pool
}

func New(ctx context.Context, url string) (*Store, error) {
	p, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}

	if err := p.Ping(ctx); err != nil {
		p.Close()
		return nil, err
	}
	return &Store{db: p}, nil
}

func (p *Store) Close() {
	p.db.Close()
}

func (p *Store) Truncate(ctx context.Context) error {
	_, err := p.db.Exec(ctx, `TRUNCATE jobs`)
	return err
}

func (p *Store) Save(ctx context.Context, j job.Job) error {
	payload, err := json.Marshal(j.Payload)
	if err != nil {
		return err
	}

	var key any
	if j.IdempotencyKey != "" {
		key = j.IdempotencyKey
	}

	_, err = p.db.Exec(ctx,
		`INSERT INTO jobs (id, type, payload, status, attempts, max_attempts, next_run_at, idempotency_key) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		j.ID, j.Type, string(payload), string(j.Status), j.Attempts, j.MaxAttempts, j.NextRunAt, key,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrDuplicate
		}
		return err
	}
	return nil
}

func (p *Store) GetByID(ctx context.Context, id string) (job.Job, bool, error) {
	var j job.Job
	var payload []byte
	var result []byte
	var lastErr *string
	var idemKey *string

	err := p.db.QueryRow(ctx,
		`SELECT id, type, payload, status, result, last_error, created_at, updated_at, attempts, max_attempts, next_run_at, idempotency_key FROM jobs WHERE id = $1`,
		id,
	).Scan(&j.ID, &j.Type, &payload, &j.Status, &result, &lastErr, &j.CreatedAt, &j.UpdatedAt, &j.Attempts, &j.MaxAttempts, &j.NextRunAt, &idemKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return job.Job{}, false, nil
		}
		return job.Job{}, false, err
	}

	if idemKey != nil {
		j.IdempotencyKey = *idemKey
	}

	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &j.Payload); err != nil {
			return job.Job{}, false, err
		}
	}

	if len(result) > 0 {
		var r any
		if err := json.Unmarshal(result, &r); err != nil {
			return job.Job{}, false, err
		}
		j.Result = r
	}

	if lastErr != nil {
		j.LastError = *lastErr
	}

	return j, true, nil
}

func (p *Store) Update(ctx context.Context, j job.Job) (bool, error) {
	var resultJSON *string
	if j.Result != nil {
		b, err := json.Marshal(j.Result)
		if err != nil {
			return false, err
		}
		s := string(b)
		resultJSON = &s
	}

	var lastErr *string
	if j.LastError != "" {
		lastErr = &j.LastError
	}

	tag, err := p.db.Exec(ctx,
		`UPDATE jobs SET status = $2, result = $3, last_error = $4, updated_at = NOW(), attempts = $5, next_run_at = $6 WHERE id = $1`,
		j.ID, string(j.Status), resultJSON, lastErr, j.Attempts, j.NextRunAt,
	)
	if err != nil {
		return false, err
	}

	return tag.RowsAffected() > 0, nil
}

func (p *Store) Claim(ctx context.Context, limit int) ([]job.Job, error) {
	now := time.Now()

	rows, err := p.db.Query(ctx,
		`UPDATE jobs SET status = 'running', updated_at = NOW()
		WHERE id IN (
			SELECT id FROM jobs
			WHERE status = 'pending'
			AND next_run_at <= $2
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		RETURNING id, type, payload, status, result, last_error, created_at, updated_at, attempts, max_attempts, next_run_at, idempotency_key`,
		limit, now,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []job.Job
	for rows.Next() {
		var j job.Job
		var payload []byte
		var result []byte
		var lastErr *string
		var idemKey *string

		if err := rows.Scan(&j.ID, &j.Type, &payload, &j.Status, &result, &lastErr, &j.CreatedAt, &j.UpdatedAt, &j.Attempts, &j.MaxAttempts, &j.NextRunAt, &idemKey); err != nil {
			return nil, err
		}
		if len(payload) > 0 {
			if err := json.Unmarshal(payload, &j.Payload); err != nil {
				return nil, err
			}
		}

		if len(result) > 0 {
			var r any
			if err := json.Unmarshal(result, &r); err != nil {
				return nil, err
			}
			j.Result = r
		}

		if lastErr != nil {
			j.LastError = *lastErr
		}
		if idemKey != nil {
			j.IdempotencyKey = *idemKey
		}
		out = append(out, j)
	}

	return out, rows.Err()
}

func (p *Store) GetByIdempotencyKey(ctx context.Context, key string) (job.Job, bool, error) {
	var j job.Job
	var payload []byte
	var result []byte
	var lastErr *string
	var idemKey *string

	err := p.db.QueryRow(ctx,
		`SELECT id, type, payload, status, result, last_error, created_at, updated_at, attempts, max_attempts, next_run_at, idempotency_key FROM jobs WHERE idempotency_key = $1`,
		key,
	).Scan(&j.ID, &j.Type, &payload, &j.Status, &result, &lastErr, &j.CreatedAt, &j.UpdatedAt, &j.Attempts, &j.MaxAttempts, &j.NextRunAt, &idemKey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return job.Job{}, false, nil
		}
		return job.Job{}, false, err
	}

	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &j.Payload); err != nil {
			return job.Job{}, false, err
		}
	}

	if len(result) > 0 {
		var r any
		if err := json.Unmarshal(result, &r); err != nil {
			return job.Job{}, false, err
		}
		j.Result = r
	}

	if lastErr != nil {
		j.LastError = *lastErr
	}
	if idemKey != nil {
		j.IdempotencyKey = *idemKey
	}

	return j, true, nil
}

func (p *Store) PendingCount(ctx context.Context) (int, error) {
	var n int
	err := p.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM jobs WHERE status IN ('pending', 'running')`,
	).Scan(&n)
	return n, err
}

func (p *Store) RequeueRunning(ctx context.Context) (int64, error) {
	tag, err := p.db.Exec(ctx,
		`UPDATE jobs SET status = 'pending', updated_at = NOW() WHERE status = 'running'`,
	)
	if err != nil {
		return 0, err
	}

	return tag.RowsAffected(), nil
}
