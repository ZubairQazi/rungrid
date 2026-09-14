"""Reproducible Compose scaling experiment. Uses the dedicated RunGrid cluster."""
import argparse
import csv
import io
import json
import platform
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from rungrid import Client

COMPOSE = ["docker", "compose", "-f", "deploy/docker-compose.yml"]


def compose(*args):
    return subprocess.check_output(COMPOSE + list(args), text=True).strip()


def percentile(values, p):
    values = sorted(values)
    if not values:
        return None
    i = (len(values) - 1) * p
    lo = int(i)
    return values[lo] + (values[min(lo + 1, len(values) - 1)] - values[lo]) * (i - lo)


def database_samples(run_id):
    assert all(c in "abcdef0123456789-" for c in run_id)
    query = f"""COPY (SELECT j.id,j.state,j.created_at,a.id attempt_id,a.attempt_number,a.state attempt_state,
    a.created_at leased_at,a.started_at,a.finished_at,
    extract(epoch FROM a.created_at-j.created_at) scheduling_latency_seconds,
    extract(epoch FROM a.finished_at-a.started_at) runtime_seconds
    FROM jobs j LEFT JOIN attempts a ON a.job_id=j.id
    WHERE j.idempotency_key LIKE 'bench-{run_id}-%' ORDER BY j.created_at,a.attempt_number)
    TO STDOUT WITH CSV HEADER"""
    return compose("exec", "-T", "postgres", "psql", "-U", "rungrid", "-d", "rungrid", "-c", query) + "\n"


def sample_resources(stop, samples):
    container = compose("ps", "-q", "postgres")
    while not stop.is_set():
        try:
            stats = json.loads(subprocess.check_output(["docker", "stats", "--no-stream", "--format", "{{json .}}", container], text=True))
            connections = compose("exec", "-T", "postgres", "psql", "-U", "rungrid", "-d", "rungrid", "-Atc", "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()")
            samples.append({"time": datetime.now(timezone.utc).isoformat(), "cpu_percent": float(stats["CPUPerc"].rstrip("%")), "connections": int(connections), "memory": stats["MemUsage"]})
        except (subprocess.CalledProcessError, ValueError, KeyError) as exc:
            samples.append({"error": str(exc)})
        stop.wait(1)


def run_case(client, count, workers, workload, output, timeout):
    run_id = str(uuid.uuid4())
    compose("stop", "worker")
    command = ["true"] if workload == "noop" else ["python", "-c", "sum(i*i for i in range(10000))"]
    started = time.perf_counter()
    submitted = []
    for offset in range(0, count, 1000):
        batch = [{"name": f"bench-{workload}", "command": command, "environment": {},
                  "resources": {"cpu": 1, "memory_mb": 64, "gpu": 0}, "max_attempts": 3,
                  "timeout_seconds": 60, "idempotency_key": f"bench-{run_id}-{i}",
                  "artifact_prefix": f"benchmarks/{run_id}"} for i in range(offset, min(count, offset + 1000))]
        submitted.extend(client.batch(batch))
    submit_seconds = time.perf_counter() - started
    samples, stop = [], threading.Event()
    sampler = threading.Thread(target=sample_resources, args=(stop, samples), daemon=True)
    sampler.start()
    execution_started = time.perf_counter()
    try:
        compose("up", "-d", "--scale", f"worker={workers}", "worker")
        while True:
            raw = database_samples(run_id)
            rows = list(csv.DictReader(io.StringIO(raw)))
            terminal_ids = {r["id"] for r in rows if r["state"] in ("SUCCEEDED", "FAILED", "CANCELLED")}
            if len(terminal_ids) == count:
                break
            if time.perf_counter() - execution_started > timeout:
                raise TimeoutError(f"benchmark timed out: {len(terminal_ids)}/{count}")
            time.sleep(0.5)
    finally:
        stop.set()
        sampler.join(timeout=10)
    elapsed = time.perf_counter() - execution_started
    case = output / f"{workload}-{workers}workers-{count}jobs-{run_id[:8]}"
    case.mkdir(parents=True)
    (case / "attempts.csv").write_text(raw)
    (case / "resources.json").write_text(json.dumps(samples, indent=2))
    first = [r for r in rows if r["attempt_number"] == "1"]
    latencies = [float(r["scheduling_latency_seconds"]) for r in first]
    leases = [datetime.fromisoformat(r["leased_at"]).timestamp() for r in first]
    schedule_span = max(leases) - min(leases) if len(leases) > 1 else 0
    summary = {"run_id": run_id, "jobs": count, "workers": workers, "workload": workload,
               "submission_seconds": submit_seconds, "submission_jobs_per_second": count / submit_seconds,
               "execution_seconds_including_worker_startup": elapsed, "end_to_end_jobs_per_second": count / elapsed,
               "scheduling_span_seconds": schedule_span,
               "scheduling_jobs_per_second": (count - 1) / schedule_span if schedule_span else None,
               "scheduling_latency_seconds": {f"p{p}": percentile(latencies, p / 100) for p in (50, 95, 99)},
               "succeeded": len({r["id"] for r in rows if r["state"] == "SUCCEEDED"}),
               "attempts": len(rows), "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
               "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], text=True)),
               "platform": platform.platform(), "python": platform.python_version(),
               "docker": subprocess.check_output(["docker", "version", "--format", "{{.Server.Version}}"], text=True).strip(),
               "compose_config": compose("config"), "timestamp": datetime.now(timezone.utc).isoformat()}
    (case / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps({k: summary[k] for k in ("workers", "workload", "jobs", "succeeded", "end_to_end_jobs_per_second")} ), flush=True)
    if summary["succeeded"] != count:
        raise AssertionError("failed jobs: inspect raw results")
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs", type=int, default=100)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8, 16])
    parser.add_argument("--workloads", nargs="+", choices=["noop", "python"], default=["noop", "python"])
    parser.add_argument("--output", type=Path, default=Path("results/benchmarks"))
    parser.add_argument("--timeout", type=int, default=7200)
    parser.add_argument("--repeat", type=int, default=1)
    args = parser.parse_args()
    if args.jobs < 1 or args.jobs > 100000 or min(args.workers) < 1:
        parser.error("jobs must be 1..100000; workers must be positive")
    client = Client()
    try:
        for _ in range(args.repeat):
            for workload in args.workloads:
                for workers in args.workers:
                    run_case(client, args.jobs, workers, workload, args.output, args.timeout)
    finally:
        compose("up", "-d", "--scale", "worker=2", "worker")


if __name__ == "__main__":
    main()
