# RunGrid

[v0.1.0 downloads](https://github.com/ZubairQazi/rungrid/releases/tag/v0.1.0) ·
[CI](https://github.com/ZubairQazi/rungrid/actions/workflows/ci.yml)

A distributed experiment scheduler in Go and Python. PostgreSQL stores the queue,
resource reservations, leases, request receipts, attempts, and events. Concurrent
workers execute trusted commands over gRPC and checkpoint to S3-compatible storage.

**At-least-once execution with fenced result commitment.** A stale worker cannot
commit over a newer attempt. Commands may execute more than once and must make
their own external effects idempotent.

## Launch

Install Docker with Compose, then run from this repository:

```sh
make up
```

This builds the Go server and two Python workers and starts PostgreSQL, MinIO,
Prometheus, and an OpenTelemetry collector. No local Go/Python installation is needed.
First startup downloads images and dependencies. Submit the checkpoint demo:

```sh
curl http://localhost:8080/readyz
curl -H 'Content-Type: application/json' --data-binary @examples/job.json http://localhost:8080/v1/jobs
curl http://localhost:8080/v1/jobs
```

The counter runs for about five seconds, uploading checkpoints, logs, and metrics.
Repeating submission returns the same job; change its idempotency key for a new run.
`make down` stops services and preserves named data volumes. Ports bind to loopback.

## Architecture

```mermaid
flowchart LR
    C[Go CLI / Python SDK / REST] --> S[Go control plane]
    S --> DB[(PostgreSQL)]
    W[Concurrent Python workers] -->|gRPC / heartbeats| S
    W --> A[(MinIO / S3 artifacts)]
    S --> O[Prometheus + OpenTelemetry]
```

Placement locks a worker, computes reservations, and selects the oldest compatible
job with `FOR UPDATE SKIP LOCKED`. Attempts have unique tokens and deadlines.
Cancellation, expiry, retries, progress, and completion serialize through the parent
job. A partial unique index enforces one active attempt per job. Durable request
receipts survive control-plane restarts.

- CPU/memory/GPU admission accounting; command or importable Python-function execution.
- Heartbeats, leases, retries, per-attempt timeouts, graceful draining.
- Fenced results, append-only attempts/events, idempotent atomic batches.
- Checksum-verified checkpoint recovery and attempt-scoped artifacts.
- Real PostgreSQL concurrency tests, Compose fault tests, Go race detector, CI.
- Scaling/recovery benchmarks with raw CSV/JSON and checked-in plotting scripts.

[Design](docs/design.md) · [API/configuration](docs/api.md) ·
[Benchmarks](benchmarks/README.md) · [Failure tests](tests/faults/README.md)

## Slurm / HPC integration (experimental)

RunGrid can be used as an experiment queue **inside Slurm allocations**, not as a
replacement for Slurm. Slurm owns GPU/CPU allocation and wall-time enforcement;
RunGrid tracks individual experiments, attempts, and retries within those allocations.
This is intended for large independent configuration/fold/seed sweeps.

An example single-GPU worker allocation is provided in
[`deploy/slurm/worker.sbatch`](deploy/slurm/worker.sbatch). It uses an existing
Python environment, preserves Slurm's GPU visibility, and does not require Docker
on compute nodes. See the [Slurm setup guide](docs/slurm.md) for prerequisites,
submission instructions, research-workflow mapping, and a cluster validation checklist.

**Status:** deployment template, not a cluster-tested backend. There is no automatic
`sbatch` submission, allocation renewal, or Slurm accounting reconciliation. Existing
Lightning `.ckpt` files need an adapter before they can resume through RunGrid;
the current worker discovers JSON checkpoints. The local Compose benchmarks do not
establish Slurm performance or recovery guarantees.

## Develop and test

Recorded toolchain: Go 1.27.1 and Python 3.12. Generated bindings are committed.

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install --require-hashes -r requirements-dev.lock
pip install --no-deps -e .
make build
bin/rungrid submit examples/job.json
bin/rungrid list
RUNGRID_TEST_DATABASE_URL='postgres://rungrid:rungrid@localhost:55432/rungrid?sslmode=disable' make test
make integration
```

Without `RUNGRID_TEST_DATABASE_URL`, database tests explicitly skip; that is not a
complete verification run. Integration tests inject faults into the dedicated
`rungrid` Compose project; do not run them alongside research jobs.

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
make generate
make benchmark
python benchmarks/plot.py results/benchmarks
python benchmarks/recovery.py --repeat 3
```

Go coverage measures handwritten `internal/` packages across database/protocol tests.
Python coverage excludes generated bindings. External Compose workers are exercised
by fault tests but are not instrumented by the Python unit-coverage command.

## HeteroSplit demonstration

The real-data adapter preserves the original MF/GraphSAGE study's **eight dataset/
regime combinations × two models × five seeds = 80 jobs**. The proposed 160-job grid
requires additional scientifically valid definitions: source/destination regimes
are undefined for unordered DrugComb pairs and MovieLens has no context role.
See [provenance and scope](examples/heterosplit/NOTICE.md).

Supply `summary_v_1_5.csv` and `ml/ml-latest-small/ratings.csv` in your data directory.
Data is mounted read-only and is not redistributed.

```sh
export HETEROSPLIT_DATA_DIR=/absolute/path/to/heterosplit/data
docker compose -f deploy/docker-compose.yml build worker
docker compose -f deploy/docker-compose.yml -f deploy/heterosplit.yml up --build -d --scale worker=2
docker compose -f deploy/docker-compose.yml -f deploy/heterosplit.yml run --rm --no-deps --entrypoint python worker examples/heterosplit/preflight.py
python examples/heterosplit/submit.py --run-id study-1
python examples/heterosplit/aggregate.py results/heterosplit/jobs.json
# Return to lightweight workers after the study completes:
docker compose -f deploy/docker-compose.yml up -d --scale worker=2 worker
```

Defaults read up to 120,000 raw rows and use three epochs to demonstrate scheduling, not reproduce
published accuracy. Checkpoints include model, optimizer, RNG, and split identity.
Aggregation accepts only successful-attempt metrics with verified SHA-256 digests.

## Release and evidence

See [release checklist](docs/releasing.md) and [verification evidence](docs/evidence.md).
The harness supports 10K–100K backlogs; results exist only for configurations actually
run. There is no untrusted-code sandbox, authentication, GPU device assignment,
autoscaling, or DAG engine. Use trusted workloads on dedicated workers.

MIT licensed. See [LICENSE](LICENSE).
