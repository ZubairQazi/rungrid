// benchqueue measures database-layer placement behind incompatible queued work.
// It owns a unique temporary schema, never the demo cluster's public schema.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/ZubairQazi/rungrid/internal/scheduler"
	"github.com/ZubairQazi/rungrid/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

func run() error {
	count := flag.Int("jobs", 10000, "incompatible queued jobs, maximum 100000")
	probes := flag.Int("probes", 100, "compatible placement probes")
	flag.Parse()
	if *count < 1 || *count > 100000 || *probes < 1 || *probes > 1000 {
		return fmt.Errorf("jobs 1..100000, probes 1..1000")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	dsn := os.Getenv("RUNGRID_TEST_DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("RUNGRID_TEST_DATABASE_URL required")
	}
	admin, e := pgxpool.New(ctx, dsn)
	if e != nil {
		return e
	}
	defer admin.Close()
	schema := "bench_" + strings.ReplaceAll(scheduler.ID(), "-", "")
	if _, e = admin.Exec(ctx, "CREATE SCHEMA "+schema); e != nil {
		return e
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, e := admin.Exec(c, "DROP SCHEMA "+schema+" CASCADE"); e != nil {
			fmt.Fprintln(os.Stderr, "cleanup failed for", schema, e)
		}
	}()
	u, e := url.Parse(dsn)
	if e != nil {
		return e
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	s, e := store.Open(ctx, u.String(), 15)
	if e != nil {
		return e
	}
	defer s.Pool.Close()
	makeJob := func(gpu int) scheduler.Job {
		return scheduler.Job{Name: "queue-pressure", Command: []string{"true"}, Resources: scheduler.Resources{CPU: 1, MemoryMB: 64, GPU: gpu}, MaxAttempts: 1, TimeoutSeconds: 60, IdempotencyKey: scheduler.ID()}
	}
	started := time.Now()
	for offset := 0; offset < *count; offset += 1000 {
		batch := make([]scheduler.Job, min(1000, *count-offset))
		for i := range batch {
			batch[i] = makeJob(1)
		}
		if _, e = s.Submit(ctx, batch); e != nil {
			return e
		}
	}
	submittedSeconds := time.Since(started).Seconds()
	batch := make([]scheduler.Job, *probes)
	for i := range batch {
		batch[i] = makeJob(0)
	}
	if _, e = s.Submit(ctx, batch); e != nil {
		return e
	}
	r := store.Request{RequestID: scheduler.ID(), WorkerID: scheduler.ID(), SessionID: scheduler.ID(), Hostname: "queue-pressure", Resources: scheduler.Resources{CPU: 1, MemoryMB: 64}}
	if _, e = s.RPC(ctx, "RegisterWorker", r); e != nil {
		return e
	}
	samples := make([]float64, 0, *probes)
	for i := 0; i < *probes; i++ {
		r.RequestID = scheduler.ID()
		r.AttemptID = ""
		r.LeaseToken = ""
		start := time.Now()
		data, e := s.RPC(ctx, "LeaseJob", r)
		if e != nil {
			return e
		}
		samples = append(samples, time.Since(start).Seconds())
		var l store.Lease
		if e = json.Unmarshal(data, &l); e != nil {
			return e
		}
		if l.AttemptID == "" {
			return fmt.Errorf("no compatible lease")
		}
		r.AttemptID = l.AttemptID
		r.LeaseToken = l.LeaseToken
		r.RequestID = scheduler.ID()
		if _, e = s.RPC(ctx, "StartAttempt", r); e != nil {
			return e
		}
		r.RequestID = scheduler.ID()
		if _, e = s.RPC(ctx, "CompleteAttempt", r); e != nil {
			return e
		}
	}
	var queued, completed int
	if e = s.Pool.QueryRow(ctx, "SELECT count(*) FILTER(WHERE state='QUEUED'),count(*) FILTER(WHERE state='SUCCEEDED') FROM jobs").Scan(&queued, &completed); e != nil {
		return e
	}
	if queued != *count || completed != *probes {
		return fmt.Errorf("queue invariant violated")
	}
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	percentile := func(p float64) float64 {
		position := float64(len(sorted)-1) * p
		i := int(position)
		return sorted[i] + (sorted[min(i+1, len(sorted)-1)]-sorted[i])*(position-float64(i))
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"workload": "store-layer incompatible GPU backlog; no process execution", "queued_jobs": queued, "compatible_probes": completed, "submission_seconds": submittedSeconds, "submission_jobs_per_second": float64(*count) / submittedSeconds, "placement_samples_seconds": samples, "placement_p50_seconds": percentile(.5), "placement_p95_seconds": percentile(.95), "placement_p99_seconds": percentile(.99), "timestamp": time.Now().UTC()})
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
