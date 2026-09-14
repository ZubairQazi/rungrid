package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	pb "github.com/ZubairQazi/rungrid/gen/rungrid/v1"
	"github.com/ZubairQazi/rungrid/internal/store"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log/slog"
	"time"
)

type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	Store   *store.Store
	OnError func(string)
}

func (s *WorkerServer) call(ctx context.Context, method string, in *pb.Request) (*pb.Response, error) {
	ctx, span := otel.Tracer("rungrid").Start(ctx, method)
	defer span.End()
	var r store.Request
	if len(in.PayloadJson) > 0 {
		d := json.NewDecoder(bytes.NewReader(in.PayloadJson))
		d.DisallowUnknownFields()
		if e := d.Decode(&r); e != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid payload")
		}
	}
	r.RequestID = in.RequestId
	r.WorkerID = in.WorkerId
	r.SessionID = in.SessionId
	r.AttemptID = in.AttemptId
	r.LeaseToken = in.LeaseToken
	if method == "LeaseJob" && in.WaitSeconds > 0 {
		wait := min(in.WaitSeconds, 20)
		deadline := time.NewTimer(time.Duration(wait) * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
	poll:
		for {
			var exists bool
			e := s.Store.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM jobs WHERE state='QUEUED') OR EXISTS(SELECT 1 FROM requests WHERE scope=$1 AND request_id=$2)`, "rpc:LeaseJob:"+r.WorkerID, r.RequestID).Scan(&exists)
			if e != nil || exists {
				break
			}
			select {
			case <-ctx.Done():
				return nil, status.FromContextError(ctx.Err()).Err()
			case <-deadline.C:
				break poll
			case <-tick.C:
			}
		}
	}
	out, e := s.Store.RPC(ctx, method, r)
	if e != nil {
		span.RecordError(e)
		if s.OnError != nil {
			s.OnError(method)
		}
		code := codes.Internal
		message := "internal error"
		switch {
		case errors.Is(e, store.ErrConflict):
			code = codes.FailedPrecondition
			message = e.Error()
		case errors.Is(e, store.ErrInvalid):
			code = codes.InvalidArgument
			message = e.Error()
		case errors.Is(e, store.ErrNotFound):
			code = codes.NotFound
			message = e.Error()
		case errors.Is(e, context.Canceled):
			code = codes.Canceled
		case errors.Is(e, context.DeadlineExceeded):
			code = codes.DeadlineExceeded
		}
		slog.Warn("worker RPC failed", "method", method, "worker_id", r.WorkerID, "attempt_id", r.AttemptID, "request_id", r.RequestID, "error", e)
		return nil, status.Error(code, message)
	}
	return &pb.Response{PayloadJson: out}, nil
}
func (s *WorkerServer) RegisterWorker(c context.Context, r *pb.Request) (*pb.Response, error) {
	return s.call(c, "RegisterWorker", r)
}
func (s *WorkerServer) LeaseJob(c context.Context, r *pb.Request) (*pb.Response, error) {
	return s.call(c, "LeaseJob", r)
}
func (s *WorkerServer) StartAttempt(c context.Context, r *pb.Request) (*pb.Response, error) {
	return s.call(c, "StartAttempt", r)
}
func (s *WorkerServer) Heartbeat(c context.Context, r *pb.Request) (*pb.Response, error) {
	return s.call(c, "Heartbeat", r)
}
func (s *WorkerServer) ReportProgress(c context.Context, r *pb.Request) (*pb.Response, error) {
	return s.call(c, "ReportProgress", r)
}
func (s *WorkerServer) CompleteAttempt(c context.Context, r *pb.Request) (*pb.Response, error) {
	return s.call(c, "CompleteAttempt", r)
}
func (s *WorkerServer) FailAttempt(c context.Context, r *pb.Request) (*pb.Response, error) {
	return s.call(c, "FailAttempt", r)
}
func (s *WorkerServer) ReleaseWorker(c context.Context, r *pb.Request) (*pb.Response, error) {
	return s.call(c, "ReleaseWorker", r)
}
func Register(g *grpc.Server, s *store.Store, onError func(string)) {
	pb.RegisterWorkerServiceServer(g, &WorkerServer{Store: s, OnError: onError})
}
