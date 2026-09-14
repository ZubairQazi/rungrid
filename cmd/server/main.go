package main

import (
	"context"
	"fmt"
	"github.com/ZubairQazi/rungrid/internal/api"
	"github.com/ZubairQazi/rungrid/internal/store"
	"github.com/ZubairQazi/rungrid/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	lease, e := strconv.Atoi(env("RUNGRID_LEASE_SECONDS", "15"))
	if e != nil {
		return e
	}
	s, e := store.Open(ctx, env("DATABASE_URL", "postgres://rungrid:rungrid@localhost:5432/rungrid?sslmode=disable"), lease)
	if e != nil {
		return e
	}
	defer s.Pool.Close()
	shutdown, e := telemetry.Tracing(ctx)
	if e != nil {
		return e
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdown(c)
	}()
	reg := prometheus.NewRegistry()
	reg.MustRegister(&telemetry.Collector{Store: s})
	rpcErrors := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "rungrid_rpc_errors_total", Help: "RPC errors"}, []string{"method"})
	reg.MustRegister(rpcErrors)
	server := &http.Server{Addr: env("RUNGRID_HTTP_ADDR", "127.0.0.1:8080"), Handler: api.Handler(s, promhttp.HandlerFor(reg, promhttp.HandlerOpts{})), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	g := grpc.NewServer(grpc.MaxRecvMsgSize(2 << 20))
	api.Register(g, s, func(method string) { rpcErrors.WithLabelValues(method).Inc() })
	listener, e := net.Listen("tcp", env("RUNGRID_GRPC_ADDR", "127.0.0.1:9090"))
	if e != nil {
		return e
	}
	errs := make(chan error, 2)
	go func() { errs <- server.ListenAndServe() }()
	go func() { errs <- g.Serve(listener) }()
	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				if _, e := s.Reap(ctx); e != nil && ctx.Err() == nil {
					slog.Error("lease reaper failed", "error", e)
				}
			}
		}
	}()
	slog.Info("RunGrid ready", "http", server.Addr, "grpc", listener.Addr())
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-errs:
		stop()
	}
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(c)
	done := make(chan struct{})
	go func() { g.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-c.Done():
		g.Stop()
	}
	if serveErr != nil && serveErr != http.ErrServerClosed {
		return serveErr
	}
	return nil
}
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
