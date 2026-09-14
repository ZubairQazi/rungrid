package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ZubairQazi/rungrid/internal/scheduler"
	"github.com/jackc/pgx/v5"
	"regexp"
	"strings"
	"time"
)

type Request struct {
	RequestID  string              `json:"request_id"`
	WorkerID   string              `json:"worker_id"`
	SessionID  string              `json:"session_id"`
	AttemptID  string              `json:"attempt_id"`
	LeaseToken string              `json:"lease_token"`
	Hostname   string              `json:"hostname"`
	Resources  scheduler.Resources `json:"resources"`
	ExitCode   int                 `json:"exit_code"`
	Reason     string              `json:"reason"`
	Logs       []string            `json:"logs"`
	Artifacts  []Artifact          `json:"artifacts"`
}
type Artifact struct {
	ObjectKey   string `json:"object_key"`
	ContentType string `json:"content_type"`
	SizeBytes   int64  `json:"size_bytes"`
	Checksum    string `json:"checksum"`
	Kind        string `json:"kind"`
}
type Lease struct {
	Job            JobRow    `json:"job"`
	AttemptID      string    `json:"attempt_id"`
	AttemptNumber  int       `json:"attempt_number"`
	LeaseToken     string    `json:"lease_token"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
	ServerTime     time.Time `json:"server_time"`
	Checkpoint     *Artifact `json:"checkpoint,omitempty"`
}

func (s *Store) Workers(ctx context.Context) ([]json.RawMessage, error) {
	rows, e := s.Pool.Query(ctx, `SELECT jsonb_build_object('id',w.id,'hostname',w.hostname,'state',w.state,'registered_at',w.registered_at,'last_heartbeat_at',w.last_heartbeat_at,
 'total_resources',jsonb_build_object('cpu',w.cpu,'memory_mb',w.memory_mb,'gpu',w.gpu),
 'available_resources',jsonb_build_object('cpu',w.cpu-coalesce(u.cpu,0),'memory_mb',w.memory_mb-coalesce(u.memory_mb,0),'gpu',w.gpu-coalesce(u.gpu,0)))
 FROM workers w LEFT JOIN LATERAL (SELECT sum(j.cpu) cpu,sum(j.memory_mb) memory_mb,sum(j.gpu) gpu FROM attempts a JOIN jobs j ON j.id=a.job_id WHERE a.worker_id=w.id AND a.state IN ('LEASED','RUNNING')) u ON true ORDER BY w.id LIMIT 1000`)
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
func (s *Store) RPC(ctx context.Context, method string, r Request) (json.RawMessage, error) {
	if r.WorkerID == "" || r.SessionID == "" || len(r.WorkerID) > 256 || len(r.SessionID) > 256 {
		return nil, ErrInvalid
	}
	return s.Mutate(ctx, "rpc:"+method+":"+r.WorkerID, r.RequestID, r, func(tx pgx.Tx) (any, error) {
		if method == "RegisterWorker" {
			if !r.Resources.Valid() || r.Resources.CPU < 1 || r.Resources.MemoryMB < 1 {
				return nil, ErrInvalid
			}
			_, e := tx.Exec(ctx, `INSERT INTO workers(id,session_id,hostname,cpu,memory_mb,gpu,state) VALUES($1,$2,$3,$4,$5,$6,'ACTIVE') ON CONFLICT DO NOTHING`, r.WorkerID, r.SessionID, r.Hostname, r.Resources.CPU, r.Resources.MemoryMB, r.Resources.GPU)
			if e != nil {
				return nil, e
			}
			var session string
			var cap scheduler.Resources
			e = tx.QueryRow(ctx, "SELECT session_id,cpu,memory_mb,gpu FROM workers WHERE id=$1 FOR UPDATE", r.WorkerID).Scan(&session, &cap.CPU, &cap.MemoryMB, &cap.GPU)
			if e != nil {
				return nil, e
			}
			if session != r.SessionID || cap != r.Resources {
				return nil, ErrConflict
			}
			return map[string]bool{"registered": true}, nil
		}
		// A single lock order for every worker mutation: worker -> job -> attempt.
		var session, state string
		var cap scheduler.Resources
		e := tx.QueryRow(ctx, "SELECT session_id,state,cpu,memory_mb,gpu FROM workers WHERE id=$1 FOR UPDATE", r.WorkerID).Scan(&session, &state, &cap.CPU, &cap.MemoryMB, &cap.GPU)
		if errors.Is(e, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		if e != nil {
			return nil, e
		}
		if session != r.SessionID {
			return nil, ErrConflict
		}
		_, e = tx.Exec(ctx, "UPDATE workers SET last_heartbeat_at=clock_timestamp() WHERE id=$1", r.WorkerID)
		if e != nil {
			return nil, e
		}
		switch method {
		case "LeaseJob":
			if state != "ACTIVE" {
				return nil, ErrConflict
			}
			return s.lease(ctx, tx, r, cap)
		case "ReleaseWorker":
			_, e = tx.Exec(ctx, "UPDATE workers SET state='DRAINING' WHERE id=$1", r.WorkerID)
			return map[string]bool{"released": true}, e
		case "Heartbeat":
			if r.AttemptID == "" {
				return map[string]bool{"alive": true}, nil
			}
		case "StartAttempt", "ReportProgress", "CompleteAttempt", "FailAttempt":
		default:
			return nil, ErrInvalid
		}
		return s.attempt(ctx, tx, method, r)
	})
}
func (s *Store) lease(ctx context.Context, tx pgx.Tx, r Request, cap scheduler.Resources) (any, error) {
	var used scheduler.Resources
	e := tx.QueryRow(ctx, `SELECT coalesce(sum(j.cpu),0),coalesce(sum(j.memory_mb),0),coalesce(sum(j.gpu),0) FROM attempts a JOIN jobs j ON j.id=a.job_id WHERE a.worker_id=$1 AND a.state IN ('LEASED','RUNNING')`, r.WorkerID).Scan(&used.CPU, &used.MemoryMB, &used.GPU)
	if e != nil {
		return nil, e
	}
	j, e := scanJob(tx.QueryRow(ctx, "SELECT "+jobCols+` FROM jobs WHERE state='QUEUED' AND cpu<=$1 AND memory_mb<=$2 AND gpu<=$3 ORDER BY created_at,id LIMIT 1 FOR UPDATE SKIP LOCKED`, cap.CPU-used.CPU, cap.MemoryMB-used.MemoryMB, cap.GPU-used.GPU))
	if errors.Is(e, ErrNotFound) {
		return map[string]any{"idle": true}, nil
	}
	if e != nil {
		return nil, e
	}
	l := Lease{Job: j, AttemptID: scheduler.ID(), LeaseToken: scheduler.ID()}
	e = tx.QueryRow(ctx, `INSERT INTO attempts(id,job_id,attempt_number,worker_id,state,lease_token,lease_expires_at,deadline_at)
 VALUES($1,$2,(SELECT coalesce(max(attempt_number),0)+1 FROM attempts WHERE job_id=$2),$3,'LEASED',$4,
 clock_timestamp()+make_interval(secs=>least($5,$6)),clock_timestamp()+make_interval(secs=>$6)) RETURNING attempt_number,lease_expires_at,clock_timestamp()`, l.AttemptID, j.ID, r.WorkerID, l.LeaseToken, s.LeaseSeconds, j.Definition.TimeoutSeconds).Scan(&l.AttemptNumber, &l.LeaseExpiresAt, &l.ServerTime)
	if e != nil {
		return nil, e
	}
	if l.AttemptNumber > j.MaxAttempts {
		return nil, fmt.Errorf("attempt budget invariant violated")
	}
	_, e = tx.Exec(ctx, "UPDATE jobs SET state='LEASED',updated_at=clock_timestamp() WHERE id=$1", j.ID)
	if e != nil {
		return nil, e
	}
	l.Job.State = "LEASED"
	var checkpoint Artifact
	e = tx.QueryRow(ctx, "SELECT object_key,content_type,size_bytes,checksum,kind FROM artifacts WHERE job_id=$1 AND kind='checkpoint' ORDER BY created_at DESC,id DESC LIMIT 1", j.ID).Scan(&checkpoint.ObjectKey, &checkpoint.ContentType, &checkpoint.SizeBytes, &checkpoint.Checksum, &checkpoint.Kind)
	if e == nil {
		l.Checkpoint = &checkpoint
	} else if !errors.Is(e, pgx.ErrNoRows) {
		return nil, e
	}
	e = event(ctx, tx, j.ID, l.AttemptID, "LEASED", map[string]any{"request_id": r.RequestID, "worker_id": r.WorkerID, "attempt_number": l.AttemptNumber})
	return l, e
}

var checksumPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var idPattern = regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`)

func (s *Store) attempt(ctx context.Context, tx pgx.Tx, method string, r Request) (any, error) {
	if !idPattern.MatchString(r.AttemptID) || !idPattern.MatchString(r.LeaseToken) {
		return nil, ErrInvalid
	}
	var jobID string
	e := tx.QueryRow(ctx, "SELECT job_id FROM attempts WHERE id=$1", r.AttemptID).Scan(&jobID)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if e != nil {
		return nil, e
	}
	j, e := scanJob(tx.QueryRow(ctx, "SELECT "+jobCols+" FROM jobs WHERE id=$1 FOR UPDATE", jobID))
	if e != nil {
		return nil, e
	}
	var state, token, worker string
	var number int
	var valid bool
	e = tx.QueryRow(ctx, "SELECT state,lease_token,worker_id,attempt_number,lease_expires_at>clock_timestamp() AND deadline_at>clock_timestamp() FROM attempts WHERE id=$1 FOR UPDATE", r.AttemptID).Scan(&state, &token, &worker, &number, &valid)
	if e != nil {
		return nil, e
	}
	if worker != r.WorkerID || token != r.LeaseToken || !valid || (j.State != "LEASED" && j.State != "RUNNING") || (state != "LEASED" && state != "RUNNING") {
		return nil, ErrConflict
	}
	switch method {
	case "Heartbeat":
		var deadline, now time.Time
		e = tx.QueryRow(ctx, "UPDATE attempts SET lease_expires_at=least(deadline_at,clock_timestamp()+make_interval(secs=>$2)) WHERE id=$1 RETURNING lease_expires_at,clock_timestamp()", r.AttemptID, s.LeaseSeconds).Scan(&deadline, &now)
		return map[string]any{"lease_expires_at": deadline, "server_time": now}, e
	case "StartAttempt":
		if state == "LEASED" {
			_, e = tx.Exec(ctx, "UPDATE attempts SET state='RUNNING',started_at=clock_timestamp() WHERE id=$1", r.AttemptID)
			if e != nil {
				return nil, e
			}
			_, e = tx.Exec(ctx, "UPDATE jobs SET state='RUNNING',updated_at=clock_timestamp() WHERE id=$1", jobID)
			if e != nil {
				return nil, e
			}
		}
	case "ReportProgress", "CompleteAttempt", "FailAttempt":
		if state != "RUNNING" {
			return nil, ErrConflict
		}
		if len(r.Logs) > 100 || len(r.Artifacts) > 100 || len(r.Reason) > 8192 {
			return nil, ErrInvalid
		}
		for _, line := range r.Logs {
			if len(line) > 16384 {
				return nil, ErrInvalid
			}
		}
		for _, a := range r.Artifacts {
			prefix := strings.Trim(j.Definition.ArtifactPrefix, "/")
			if prefix != "" {
				prefix += "/"
			}
			prefix += jobID + "/" + r.AttemptID + "/"
			if !strings.HasPrefix(a.ObjectKey, prefix) || strings.Contains(a.ObjectKey, "..") || !checksumPattern.MatchString(a.Checksum) || a.SizeBytes < 0 || (a.Kind != "checkpoint" && a.Kind != "output" && a.Kind != "log") {
				return nil, ErrInvalid
			}
			var existing Artifact
			err := tx.QueryRow(ctx, "SELECT object_key,content_type,size_bytes,checksum,kind FROM artifacts WHERE object_key=$1", a.ObjectKey).Scan(&existing.ObjectKey, &existing.ContentType, &existing.SizeBytes, &existing.Checksum, &existing.Kind)
			if err == nil {
				if existing != a {
					return nil, ErrConflict
				}
				continue
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return nil, err
			}
			_, e = tx.Exec(ctx, "INSERT INTO artifacts(id,job_id,attempt_id,object_key,content_type,size_bytes,checksum,kind) VALUES($1,$2,$3,$4,$5,$6,$7,$8)", scheduler.ID(), jobID, r.AttemptID, a.ObjectKey, a.ContentType, a.SizeBytes, a.Checksum, a.Kind)
			if e != nil {
				return nil, e
			}
		}
		if method != "ReportProgress" {
			next, attemptState := "SUCCEEDED", "SUCCEEDED"
			if method == "CompleteAttempt" && r.ExitCode != 0 {
				return nil, ErrInvalid
			}
			if method == "FailAttempt" {
				next = scheduler.RetryState(number, j.MaxAttempts)
				attemptState = "FAILED"
			}
			_, e = tx.Exec(ctx, "UPDATE attempts SET state=$2,finished_at=clock_timestamp(),exit_code=$3,failure_reason=$4 WHERE id=$1", r.AttemptID, attemptState, r.ExitCode, r.Reason)
			if e != nil {
				return nil, e
			}
			_, e = tx.Exec(ctx, "UPDATE jobs SET state=$2,updated_at=clock_timestamp() WHERE id=$1", jobID, next)
			if e != nil {
				return nil, e
			}
		}
	}
	e = event(ctx, tx, jobID, r.AttemptID, method, map[string]any{"request_id": r.RequestID, "worker_id": r.WorkerID, "logs": r.Logs, "reason": r.Reason, "exit_code": r.ExitCode})
	return map[string]bool{"accepted": true}, e
}
