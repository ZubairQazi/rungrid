package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	pb "github.com/ZubairQazi/rungrid/gen/rungrid/v1"
	"github.com/ZubairQazi/rungrid/internal/scheduler"
	"github.com/ZubairQazi/rungrid/internal/store"
	"github.com/ZubairQazi/rungrid/internal/telemetry"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestRESTAndGRPCWithPostgres(t *testing.T) {
	dsn := os.Getenv("RUNGRID_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set RUNGRID_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, e := pgxpool.New(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	schema := "test_api_" + strings.ReplaceAll(scheduler.ID(), "-", "")
	if _, e = admin.Exec(ctx, "CREATE SCHEMA "+schema); e != nil {
		t.Fatal(e)
	}
	defer func() { admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); admin.Close() }()
	u, _ := url.Parse(dsn)
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	s, e := store.Open(ctx, u.String(), 5)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Pool.Close()
	reg := prometheus.NewRegistry()
	reg.MustRegister(&telemetry.Collector{Store: s})
	httpServer := httptest.NewServer(Handler(s, promhttp.HandlerFor(reg, promhttp.HandlerOpts{})))
	defer httpServer.Close()
	request := func(method, path string, body any, statusCode int) json.RawMessage {
		t.Helper()
		b, _ := json.Marshal(body)
		req, _ := http.NewRequest(method, httpServer.URL+path, bytes.NewReader(b))
		req.Header.Set("X-Request-ID", scheduler.ID())
		res, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer res.Body.Close()
		data, _ := io.ReadAll(res.Body)
		if res.StatusCode != statusCode {
			t.Fatalf("%s %s => %d %s", method, path, res.StatusCode, data)
		}
		return data
	}
	j := scheduler.Job{Name: "wire", Command: []string{"true"}, Resources: scheduler.Resources{CPU: 1, MemoryMB: 64}, MaxAttempts: 1, TimeoutSeconds: 30, IdempotencyKey: scheduler.ID()}
	var row store.JobRow
	if e = json.Unmarshal(request("POST", "/v1/jobs", j, 200), &row); e != nil {
		t.Fatal(e)
	}
	request("GET", "/readyz", nil, 200)
	request("GET", "/v1/jobs", nil, 200)
	request("GET", "/v1/jobs/"+row.ID, nil, 200)
	request("GET", "/v1/jobs/"+scheduler.ID(), nil, 404)
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	g := grpc.NewServer()
	Register(g, s, nil)
	go g.Serve(listener)
	defer g.Stop()
	conn, e := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	client := pb.NewWorkerServiceClient(conn)
	r := &pb.Request{RequestId: scheduler.ID(), WorkerId: scheduler.ID(), SessionId: scheduler.ID(), PayloadJson: []byte(`{"resources":{"cpu":2,"memory_mb":256,"gpu":0},"hostname":"wire"}`)}
	if _, e = client.RegisterWorker(ctx, r); e != nil {
		t.Fatal(e)
	}
	r.PayloadJson = nil
	r.RequestId = scheduler.ID()
	res, e := client.LeaseJob(ctx, r)
	if e != nil {
		t.Fatal(e)
	}
	var l store.Lease
	if e = json.Unmarshal(res.PayloadJson, &l); e != nil {
		t.Fatal(e)
	}
	r.AttemptId = l.AttemptID
	r.LeaseToken = l.LeaseToken
	r.RequestId = scheduler.ID()
	if _, e = client.StartAttempt(ctx, r); e != nil {
		t.Fatal(e)
	}
	r.RequestId = scheduler.ID()
	if _, e = client.Heartbeat(ctx, r); e != nil {
		t.Fatal(e)
	}
	r.RequestId = scheduler.ID()
	r.PayloadJson = []byte(`{"logs":["structured progress"]}`)
	if _, e = client.ReportProgress(ctx, r); e != nil {
		t.Fatal(e)
	}
	request("GET", "/v1/workers", nil, 200)
	request("GET", "/metrics", nil, 200)
	r.RequestId = scheduler.ID()
	r.PayloadJson = []byte(`{"exit_code":1,"reason":"test"}`)
	if _, e = client.FailAttempt(ctx, r); e != nil {
		t.Fatal(e)
	}
	request("POST", "/v1/jobs/"+row.ID+"/retry", nil, 200)
	r.AttemptId = ""
	r.LeaseToken = ""
	r.PayloadJson = nil
	r.RequestId = scheduler.ID()
	res, e = client.LeaseJob(ctx, r)
	if e != nil {
		t.Fatal(e)
	}
	json.Unmarshal(res.PayloadJson, &l)
	r.AttemptId = l.AttemptID
	r.LeaseToken = l.LeaseToken
	r.RequestId = scheduler.ID()
	if _, e = client.StartAttempt(ctx, r); e != nil {
		t.Fatal(e)
	}
	r.RequestId = scheduler.ID()
	if _, e = client.CompleteAttempt(ctx, r); e != nil {
		t.Fatal(e)
	}
	if _, e = client.CompleteAttempt(ctx, r); e != nil {
		t.Fatal("receipt replay", e)
	}
	r.RequestId = scheduler.ID()
	if _, e = client.CompleteAttempt(ctx, r); e == nil {
		t.Fatal("stale wire completion accepted")
	}
	for _, kind := range []string{"attempts", "events", "artifacts"} {
		request("GET", "/v1/jobs/"+row.ID+"/"+kind, nil, 200)
	}
	r.RequestId = scheduler.ID()
	r.AttemptId = ""
	r.LeaseToken = ""
	if _, e = client.ReleaseWorker(ctx, r); e != nil {
		t.Fatal(e)
	}
	j.IdempotencyKey = scheduler.ID()
	var jobs []store.JobRow
	json.Unmarshal(request("POST", "/v1/jobs/batch", []scheduler.Job{j}, 200), &jobs)
	request("POST", "/v1/jobs/"+jobs[0].ID+"/cancel", nil, 200)
	request("GET", "/metrics", nil, 200)
}
