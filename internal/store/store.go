package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/ZubairQazi/rungrid/internal/scheduler"
	"github.com/ZubairQazi/rungrid/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrConflict = errors.New("conflict or stale lease")
var ErrNotFound = errors.New("not found")
var ErrInvalid = errors.New("invalid request")

type Store struct {
	Pool         *pgxpool.Pool
	LeaseSeconds int
}

func Open(ctx context.Context, url string, lease int) (*Store, error) {
	if lease < 2 {
		return nil, fmt.Errorf("lease seconds must be >=2")
	}
	cfg, e := pgxpool.ParseConfig(url)
	if e != nil {
		return nil, e
	}
	cfg.MaxConns = 32
	p, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		return nil, e
	}
	s := &Store{p, lease}
	tx, e := p.Begin(ctx)
	if e != nil {
		p.Close()
		return nil, e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(78234901)"); e == nil {
		_, e = tx.Exec(ctx, migrations.Initial)
	}
	if e == nil {
		e = tx.Commit(ctx)
	}
	if e != nil {
		p.Close()
		return nil, e
	}
	return s, nil
}

type JobRow struct {
	ID          string        `json:"id"`
	Definition  scheduler.Job `json:"definition"`
	State       string        `json:"state"`
	MaxAttempts int           `json:"max_attempts"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
}

func scanJob(row pgx.Row) (j JobRow, e error) {
	e = row.Scan(&j.ID, &j.Definition, &j.State, &j.MaxAttempts, &j.CreatedAt, &j.UpdatedAt)
	if errors.Is(e, pgx.ErrNoRows) {
		e = ErrNotFound
	}
	return
}

const jobCols = "id,definition,state,max_attempts,created_at,updated_at"

func (s *Store) Get(ctx context.Context, id string) (JobRow, error) {
	return scanJob(s.Pool.QueryRow(ctx, "SELECT "+jobCols+" FROM jobs WHERE id=$1", id))
}
func (s *Store) List(ctx context.Context, limit, offset int) ([]JobRow, error) {
	rows, e := s.Pool.Query(ctx, "SELECT "+jobCols+" FROM jobs ORDER BY created_at,id LIMIT $1 OFFSET $2", limit, offset)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []JobRow{}
	for rows.Next() {
		j, e := scanJob(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, j)
	}
	return out, rows.Err()
}
func event(ctx context.Context, tx pgx.Tx, job string, attempt any, kind string, payload any) error {
	b, e := json.Marshal(payload)
	if e != nil {
		return e
	}
	_, e = tx.Exec(ctx, "INSERT INTO events(job_id,attempt_id,event_type,payload) VALUES($1,$2,$3,$4)", job, attempt, kind, b)
	return e
}
func (s *Store) Submit(ctx context.Context, jobs []scheduler.Job) ([]JobRow, error) {
	if len(jobs) < 1 || len(jobs) > 1000 {
		return nil, ErrInvalid
	}
	for _, j := range jobs {
		if e := j.Validate(); e != nil {
			return nil, fmt.Errorf("%w: %s", ErrInvalid, e)
		}
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(ctx)
	out := []JobRow{}
	// Serialize batch keys in a deterministic order to avoid crossed-key deadlocks.
	keys := make([]string, len(jobs))
	for i, j := range jobs {
		keys[i] = j.IdempotencyKey
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", key); e != nil {
			return nil, e
		}
	}
	for _, j := range jobs {
		b, _ := json.Marshal(j)
		id := scheduler.ID()
		tag, e := tx.Exec(ctx, `INSERT INTO jobs(id,name,idempotency_key,definition,cpu,memory_mb,gpu,max_attempts,timeout_seconds) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(idempotency_key) DO NOTHING`, id, j.Name, j.IdempotencyKey, b, j.Resources.CPU, j.Resources.MemoryMB, j.Resources.GPU, j.MaxAttempts, j.TimeoutSeconds)
		if e != nil {
			return nil, e
		}
		row, e := scanJob(tx.QueryRow(ctx, "SELECT "+jobCols+" FROM jobs WHERE idempotency_key=$1", j.IdempotencyKey))
		if e != nil {
			return nil, e
		}
		orig, _ := json.Marshal(row.Definition)
		if string(orig) != string(b) {
			return nil, fmt.Errorf("%w: idempotency key reused with different definition", ErrConflict)
		}
		if tag.RowsAffected() > 0 {
			if e = event(ctx, tx, id, nil, "SUBMITTED", map[string]any{}); e != nil {
				return nil, e
			}
		}
		out = append(out, row)
	}
	return out, tx.Commit(ctx)
}
func (s *Store) ReadRows(ctx context.Context, kind, id string, limit, offset int) ([]json.RawMessage, error) {
	queries := map[string]string{
		"attempts":  "SELECT to_jsonb(a)-'lease_token' FROM attempts a WHERE job_id=$1 ORDER BY attempt_number LIMIT $2 OFFSET $3",
		"events":    "SELECT to_jsonb(e) FROM events e WHERE job_id=$1 ORDER BY id LIMIT $2 OFFSET $3",
		"artifacts": "SELECT to_jsonb(a) FROM artifacts a WHERE job_id=$1 ORDER BY created_at LIMIT $2 OFFSET $3",
	}
	q, ok := queries[kind]
	if !ok {
		return nil, ErrInvalid
	}
	rows, e := s.Pool.Query(ctx, q, id, limit, offset)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []json.RawMessage{}
	for rows.Next() {
		var b json.RawMessage
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		out = append(out, b)
	}
	return out, rows.Err()
}
func (s *Store) Mutate(ctx context.Context, scope, id string, payload any, fn func(pgx.Tx) (any, error)) (json.RawMessage, error) {
	if id == "" || len(id) > 256 {
		return nil, fmt.Errorf("%w: request_id required", ErrInvalid)
	}
	b, e := json.Marshal(payload)
	if e != nil {
		return nil, e
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(b))
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(ctx)
	_, e = tx.Exec(ctx, "INSERT INTO requests(scope,request_id,payload_hash) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", scope, id, hash)
	if e != nil {
		return nil, e
	}
	var old string
	var response json.RawMessage
	e = tx.QueryRow(ctx, "SELECT payload_hash,response FROM requests WHERE scope=$1 AND request_id=$2 FOR UPDATE", scope, id).Scan(&old, &response)
	if e != nil {
		return nil, e
	}
	if old != hash {
		return nil, ErrConflict
	}
	if response != nil {
		return response, tx.Commit(ctx)
	}
	result, e := fn(tx)
	if e != nil {
		return nil, e
	}
	response, e = json.Marshal(result)
	if e != nil {
		return nil, e
	}
	_, e = tx.Exec(ctx, "UPDATE requests SET response=$3 WHERE scope=$1 AND request_id=$2", scope, id, response)
	if e != nil {
		return nil, e
	}
	return response, tx.Commit(ctx)
}
func (s *Store) Action(ctx context.Context, id, action, requestID string) (json.RawMessage, error) {
	return s.Mutate(ctx, "job:"+action, requestID, id, func(tx pgx.Tx) (any, error) {
		j, e := scanJob(tx.QueryRow(ctx, "SELECT "+jobCols+" FROM jobs WHERE id=$1 FOR UPDATE", id))
		if e != nil {
			return nil, e
		}
		switch action {
		case "cancel":
			if j.State == "SUCCEEDED" || j.State == "FAILED" {
				return nil, ErrConflict
			}
			if j.State != "CANCELLED" {
				_, e = tx.Exec(ctx, "UPDATE attempts SET state='CANCELLED',finished_at=clock_timestamp(),failure_reason='cancelled' WHERE job_id=$1 AND state IN ('LEASED','RUNNING')", id)
				if e != nil {
					return nil, e
				}
				j.State = "CANCELLED"
			}
		case "retry":
			if j.State != "FAILED" {
				return nil, ErrConflict
			}
			j.State = "QUEUED"
			j.MaxAttempts++
		default:
			return nil, ErrInvalid
		}
		_, e = tx.Exec(ctx, "UPDATE jobs SET state=$2,max_attempts=$3,updated_at=clock_timestamp() WHERE id=$1", id, j.State, j.MaxAttempts)
		if e != nil {
			return nil, e
		}
		if e = event(ctx, tx, id, nil, action, map[string]string{"request_id": requestID}); e != nil {
			return nil, e
		}
		return scanJob(tx.QueryRow(ctx, "SELECT "+jobCols+" FROM jobs WHERE id=$1", id))
	})
}
