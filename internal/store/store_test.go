package store

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/ZubairQazi/rungrid/internal/scheduler"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("RUNGRID_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set RUNGRID_TEST_DATABASE_URL for real PostgreSQL tests")
	}
	ctx := context.Background()
	admin, e := pgxpool.New(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	schema := "test_" + strings.ReplaceAll(scheduler.ID(), "-", "")
	if _, e = admin.Exec(ctx, "CREATE SCHEMA "+schema); e != nil {
		t.Fatal(e)
	}
	u, e := url.Parse(dsn)
	if e != nil {
		t.Fatal(e)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	s, e := Open(ctx, u.String(), 5)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Pool.Close(); admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); admin.Close() })
	return s
}
func job() scheduler.Job {
	return scheduler.Job{Name: "test", Command: []string{"true"}, Resources: scheduler.Resources{CPU: 1, MemoryMB: 64}, MaxAttempts: 3, TimeoutSeconds: 30, IdempotencyKey: scheduler.ID()}
}
func submit(t *testing.T, s *Store) JobRow {
	t.Helper()
	rows, e := s.Submit(context.Background(), []scheduler.Job{job()})
	if e != nil {
		t.Fatal(e)
	}
	return rows[0]
}
func worker(t *testing.T, s *Store, cpu int) Request {
	t.Helper()
	r := Request{RequestID: scheduler.ID(), WorkerID: scheduler.ID(), SessionID: scheduler.ID(), Resources: scheduler.Resources{CPU: cpu, MemoryMB: 4096}}
	if _, e := s.RPC(context.Background(), "RegisterWorker", r); e != nil {
		t.Fatal(e)
	}
	return r
}
func lease(t *testing.T, s *Store, r Request) Lease {
	t.Helper()
	r.RequestID = scheduler.ID()
	b, e := s.RPC(context.Background(), "LeaseJob", r)
	if e != nil {
		t.Fatal(e)
	}
	var l Lease
	if e = json.Unmarshal(b, &l); e != nil || l.AttemptID == "" {
		t.Fatalf("no lease: %s %v", b, e)
	}
	return l
}
func rpc(t *testing.T, s *Store, r Request, l Lease, method string) Request {
	t.Helper()
	r.RequestID = scheduler.ID()
	r.AttemptID = l.AttemptID
	r.LeaseToken = l.LeaseToken
	if _, e := s.RPC(context.Background(), method, r); e != nil {
		t.Fatal(e)
	}
	return r
}
func assertState(t *testing.T, s *Store, id, state string) {
	t.Helper()
	j, e := s.Get(context.Background(), id)
	if e != nil || j.State != state {
		t.Fatalf("want %s got %+v %v", state, j, e)
	}
}
func expire(t *testing.T, s *Store, l Lease) {
	t.Helper()
	if _, e := s.Pool.Exec(context.Background(), "UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", l.AttemptID); e != nil {
		t.Fatal(e)
	}
}
func TestSubmissionAndBatchAtomicity(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	j := job()
	first, e := s.Submit(ctx, []scheduler.Job{j, j})
	if e != nil {
		t.Fatal(e)
	}
	if first[0].ID != first[1].ID {
		t.Fatal("duplicate submission")
	}
	other := job()
	j.Name = "conflict"
	if _, e = s.Submit(ctx, []scheduler.Job{other, j}); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	rows, e := s.List(ctx, 100, 0)
	if e != nil || len(rows) != 1 {
		t.Fatal("batch did not roll back", rows, e)
	}
}
func TestCompletionIdempotencyAndReceiptConflict(t *testing.T) {
	s := testStore(t)
	j := submit(t, s)
	w := worker(t, s, 1)
	l := lease(t, s, w)
	rpc(t, s, w, l, "StartAttempt")
	r := rpc(t, s, w, l, "CompleteAttempt")
	if _, e := s.RPC(context.Background(), "CompleteAttempt", r); e != nil {
		t.Fatal(e)
	}
	r.ExitCode = 1
	if _, e := s.RPC(context.Background(), "CompleteAttempt", r); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	r.RequestID = scheduler.ID()
	r.ExitCode = 0
	if _, e := s.RPC(context.Background(), "CompleteAttempt", r); !errors.Is(e, ErrConflict) {
		t.Fatal("new stale completion accepted", e)
	}
	assertState(t, s, j.ID, "SUCCEEDED")
}
func TestExpiryFencesHeartbeatAndNewAttempt(t *testing.T) {
	s := testStore(t)
	j := submit(t, s)
	w := worker(t, s, 1)
	l := lease(t, s, w)
	r := rpc(t, s, w, l, "StartAttempt")
	expire(t, s, l)
	r.RequestID = scheduler.ID()
	if _, e := s.RPC(context.Background(), "Heartbeat", r); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	n, e := s.Reap(context.Background())
	if e != nil || n != 1 {
		t.Fatal(n, e)
	}
	l2 := lease(t, s, w)
	if l2.AttemptNumber != 2 {
		t.Fatal("attempt numbers")
	}
	rpc(t, s, w, l2, "StartAttempt")
	rpc(t, s, w, l2, "CompleteAttempt")
	r.RequestID = scheduler.ID()
	if _, e = s.RPC(context.Background(), "CompleteAttempt", r); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	assertState(t, s, j.ID, "SUCCEEDED")
}
func TestCancellationAndManualRetry(t *testing.T) {
	s := testStore(t)
	j := submit(t, s)
	w := worker(t, s, 1)
	l := lease(t, s, w)
	r := rpc(t, s, w, l, "StartAttempt")
	ctx := context.Background()
	id := scheduler.ID()
	if _, e := s.Action(ctx, j.ID, "cancel", id); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Action(ctx, j.ID, "cancel", id); e != nil {
		t.Fatal(e)
	}
	r.RequestID = scheduler.ID()
	if _, e := s.RPC(ctx, "CompleteAttempt", r); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	assertState(t, s, j.ID, "CANCELLED")
	if _, e := s.Action(ctx, j.ID, "retry", scheduler.ID()); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	j = submit(t, s)
	for i := 0; i < 3; i++ {
		l = lease(t, s, w)
		rpc(t, s, w, l, "StartAttempt")
		rpc(t, s, w, l, "FailAttempt")
	}
	assertState(t, s, j.ID, "FAILED")
	id = scheduler.ID()
	if _, e := s.Action(ctx, j.ID, "retry", id); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Action(ctx, j.ID, "retry", id); e != nil {
		t.Fatal(e)
	}
	l = lease(t, s, w)
	if l.AttemptNumber != 4 {
		t.Fatal(l)
	}
}
func TestConcurrentPlacementAndResourceConservation(t *testing.T) {
	s := testStore(t)
	for i := 0; i < 30; i++ {
		submit(t, s)
	}
	w := worker(t, s, 4)
	var wg sync.WaitGroup
	errs := make(chan error, 30)
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := w
			r.RequestID = scheduler.ID()
			_, e := s.RPC(context.Background(), "LeaseJob", r)
			errs <- e
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	var n int
	if e := s.Pool.QueryRow(context.Background(), "SELECT count(*) FROM attempts WHERE state='LEASED'").Scan(&n); e != nil || n != 4 {
		t.Fatal("oversubscription", n, e)
	}
}
func TestOldestCompatibleAndDrain(t *testing.T) {
	s := testStore(t)
	j := job()
	j.Resources.GPU = 1
	if _, e := s.Submit(context.Background(), []scheduler.Job{j}); e != nil {
		t.Fatal(e)
	}
	oldest := submit(t, s)
	submit(t, s)
	w := worker(t, s, 1)
	l := lease(t, s, w)
	if l.Job.ID != oldest.ID {
		t.Fatal("not oldest compatible")
	}
	r := w
	r.RequestID = scheduler.ID()
	if _, e := s.RPC(context.Background(), "ReleaseWorker", r); e != nil {
		t.Fatal(e)
	}
	r.RequestID = scheduler.ID()
	if _, e := s.RPC(context.Background(), "LeaseJob", r); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
}
func TestConcurrentCancelCompletion(t *testing.T) {
	s := testStore(t)
	w := worker(t, s, 1)
	for i := 0; i < 20; i++ {
		j := submit(t, s)
		l := lease(t, s, w)
		r := rpc(t, s, w, l, "StartAttempt")
		r.RequestID = scheduler.ID()
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		wg.Add(2)
		go func() { defer wg.Done(); _, e := s.RPC(context.Background(), "CompleteAttempt", r); errs <- e }()
		go func() {
			defer wg.Done()
			_, e := s.Action(context.Background(), j.ID, "cancel", scheduler.ID())
			errs <- e
		}()
		wg.Wait()
		close(errs)
		for e := range errs {
			if e != nil && !errors.Is(e, ErrConflict) {
				t.Fatal(e)
			}
		}
		got, e := s.Get(context.Background(), j.ID)
		if e != nil || (got.State != "CANCELLED" && got.State != "SUCCEEDED") {
			t.Fatal(got, e)
		}
	}
}
func TestTimeoutAndWorkerSessionFence(t *testing.T) {
	s := testStore(t)
	j := job()
	j.TimeoutSeconds = 1
	j.MaxAttempts = 1
	rows, e := s.Submit(context.Background(), []scheduler.Job{j})
	if e != nil {
		t.Fatal(e)
	}
	w := worker(t, s, 1)
	l := lease(t, s, w)
	r := rpc(t, s, w, l, "StartAttempt")
	w.SessionID = scheduler.ID()
	w.RequestID = scheduler.ID()
	if _, e = s.RPC(context.Background(), "RegisterWorker", w); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	time.Sleep(1100 * time.Millisecond)
	r.RequestID = scheduler.ID()
	if _, e = s.RPC(context.Background(), "Heartbeat", r); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	if _, e = s.Reap(context.Background()); e != nil {
		t.Fatal(e)
	}
	assertState(t, s, rows[0].ID, "FAILED")
}
func TestCheckpointMetadataFence(t *testing.T) {
	s := testStore(t)
	j := submit(t, s)
	w := worker(t, s, 1)
	l := lease(t, s, w)
	r := rpc(t, s, w, l, "StartAttempt")
	r.RequestID = scheduler.ID()
	r.Artifacts = []Artifact{{ObjectKey: j.ID + "/" + l.AttemptID + "/checkpoint/x", ContentType: "application/json", SizeBytes: 2, Checksum: strings.Repeat("a", 64), Kind: "checkpoint"}}
	if _, e := s.RPC(context.Background(), "ReportProgress", r); e != nil {
		t.Fatal(e)
	}
	expire(t, s, l)
	s.Reap(context.Background())
	next := lease(t, s, w)
	if next.Checkpoint == nil || next.Checkpoint.ObjectKey != r.Artifacts[0].ObjectKey {
		t.Fatal("checkpoint not recovered")
	}
	r.RequestID = scheduler.ID()
	if _, e := s.RPC(context.Background(), "ReportProgress", r); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
}
