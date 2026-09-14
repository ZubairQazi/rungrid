package telemetry

import (
	"context"
	"github.com/ZubairQazi/rungrid/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"os"
	"time"
)

func Tracing(ctx context.Context) (func(context.Context) error, error) {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, e := otlptracehttp.New(ctx)
	if e != nil {
		return nil, e
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(resource.NewWithAttributes("", attribute.String("service.name", "rungrid-control-plane"))))
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

type Collector struct {
	Store *store.Store
	desc  *prometheus.Desc
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc("rungrid_db_up", "Database scrape success", nil, nil)
}
func metric(ch chan<- prometheus.Metric, name, help string, typ prometheus.ValueType, value float64, labels []string, values ...string) {
	ch <- prometheus.MustNewConstMetric(prometheus.NewDesc(name, help, labels, nil), typ, value, values...)
}
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	rows, e := c.Store.Pool.Query(ctx, "SELECT state,count(*) FROM jobs GROUP BY state")
	if e != nil {
		metric(ch, "rungrid_db_up", "Database scrape success", prometheus.GaugeValue, 0, nil)
		return
	}
	counts := map[string]float64{}
	for rows.Next() {
		var state string
		var n float64
		if rows.Scan(&state, &n) == nil {
			counts[state] = n
		}
	}
	rows.Close()
	metric(ch, "rungrid_db_up", "Database scrape success", prometheus.GaugeValue, 1, nil)
	for _, state := range []string{"QUEUED", "LEASED", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"} {
		metric(ch, "rungrid_jobs", "Jobs by state", prometheus.GaugeValue, counts[state], []string{"state"}, state)
	}
	var expired, attempts, connections float64
	c.Store.Pool.QueryRow(ctx, "SELECT count(*) FROM attempts WHERE state='EXPIRED'").Scan(&expired)
	metric(ch, "rungrid_lease_expirations_total", "Expired leases retained in durable history", prometheus.CounterValue, expired, nil)
	c.Store.Pool.QueryRow(ctx, "SELECT coalesce(avg(n),0) FROM (SELECT count(*) n FROM attempts GROUP BY job_id) t").Scan(&attempts)
	metric(ch, "rungrid_attempts_per_job", "Mean attempts for attempted jobs", prometheus.GaugeValue, attempts, nil)
	c.Store.Pool.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()").Scan(&connections)
	metric(ch, "rungrid_database_connections", "Database connections", prometheus.GaugeValue, connections, nil)
	for _, q := range []struct{ name, expr, filter string }{{"scheduling_latency_seconds", "extract(epoch FROM a.created_at-j.created_at)", "a.attempt_number=1"}, {"job_runtime_seconds", "extract(epoch FROM a.finished_at-a.started_at)", "a.finished_at IS NOT NULL AND a.started_at IS NOT NULL"}} {
		var p50, p95, p99 float64
		e = c.Store.Pool.QueryRow(ctx, "SELECT coalesce(percentile_cont(0.5) WITHIN GROUP(ORDER BY "+q.expr+"),0),coalesce(percentile_cont(0.95) WITHIN GROUP(ORDER BY "+q.expr+"),0),coalesce(percentile_cont(0.99) WITHIN GROUP(ORDER BY "+q.expr+"),0) FROM attempts a JOIN jobs j ON j.id=a.job_id WHERE "+q.filter).Scan(&p50, &p95, &p99)
		if e == nil {
			for i, v := range []float64{p50, p95, p99} {
				metric(ch, "rungrid_"+q.name, "Historical attempt distribution", prometheus.GaugeValue, v, []string{"quantile"}, []string{"0.5", "0.95", "0.99"}[i])
			}
		}
	}
	rows, e = c.Store.Pool.Query(ctx, `SELECT w.id,w.state,extract(epoch FROM clock_timestamp()-w.last_heartbeat_at),w.cpu-coalesce(sum(j.cpu),0),w.gpu-coalesce(sum(j.gpu),0) FROM workers w LEFT JOIN attempts a ON a.worker_id=w.id AND a.state IN ('LEASED','RUNNING') LEFT JOIN jobs j ON j.id=a.job_id GROUP BY w.id`)
	if e == nil {
		defer rows.Close()
		for rows.Next() {
			var id, state string
			var age, cpu, gpu float64
			if rows.Scan(&id, &state, &age, &cpu, &gpu) == nil {
				metric(ch, "rungrid_worker_heartbeat_age_seconds", "Age of last contact", prometheus.GaugeValue, age, []string{"worker_id"}, id)
				if state != "ACTIVE" || age > float64(2*c.Store.LeaseSeconds) {
					cpu, gpu = 0, 0
				}
				metric(ch, "rungrid_available_cpu", "Unreserved CPU", prometheus.GaugeValue, cpu, []string{"worker_id"}, id)
				metric(ch, "rungrid_available_gpu", "Unreserved GPU", prometheus.GaugeValue, gpu, []string{"worker_id"}, id)
			}
		}
	}
}
