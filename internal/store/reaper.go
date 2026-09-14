package store

import (
	"context"
	"errors"
	"github.com/ZubairQazi/rungrid/internal/scheduler"
	"github.com/jackc/pgx/v5"
)

// Reap locks parents in bounded batches. All durable changes share one transaction.
func (s *Store) Reap(ctx context.Context) (int, error) {
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return 0, e
	}
	defer tx.Rollback(ctx)
	rows, e := tx.Query(ctx, `SELECT j.id FROM jobs j WHERE j.state IN ('LEASED','RUNNING') AND EXISTS(SELECT 1 FROM attempts a WHERE a.job_id=j.id AND a.state IN ('LEASED','RUNNING') AND a.lease_expires_at<=clock_timestamp()) ORDER BY j.id LIMIT 100 FOR UPDATE OF j SKIP LOCKED`)
	if e != nil {
		return 0, e
	}
	ids, e := pgx.CollectRows(rows, pgx.RowTo[string])
	if e != nil {
		return 0, e
	}
	reaped := 0
	for _, id := range ids {
		var aid string
		var number, max int
		e = tx.QueryRow(ctx, "SELECT a.id,a.attempt_number,j.max_attempts FROM attempts a JOIN jobs j ON j.id=a.job_id WHERE a.job_id=$1 AND a.state IN ('LEASED','RUNNING') AND a.lease_expires_at<=clock_timestamp()", id).Scan(&aid, &number, &max)
		if errors.Is(e, pgx.ErrNoRows) {
			continue
		}
		if e != nil {
			return 0, e
		}
		_, e = tx.Exec(ctx, "UPDATE attempts SET state='EXPIRED',finished_at=clock_timestamp(),failure_reason='lease or attempt deadline expired' WHERE id=$1", aid)
		if e != nil {
			return 0, e
		}
		_, e = tx.Exec(ctx, "UPDATE jobs SET state=$2,updated_at=clock_timestamp() WHERE id=$1", id, scheduler.RetryState(number, max))
		if e != nil {
			return 0, e
		}
		if e = event(ctx, tx, id, aid, "LEASE_EXPIRED", map[string]string{"reason": "lease or attempt deadline expired"}); e != nil {
			return 0, e
		}
		reaped++
	}
	return reaped, tx.Commit(ctx)
}
