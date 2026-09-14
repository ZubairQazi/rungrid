# Reproducing evidence

Use a dedicated RunGrid Compose cluster. The harness stops and scales its workers;
do not run it alongside research or integration jobs. It preserves database and
artifact history. It never truncates the database. `make up` builds the environment.

```sh
python benchmarks/run.py --jobs 100 --workers 1 2 4 8 16 --workloads noop python --repeat 3
python benchmarks/plot.py results/benchmarks
# Large durable backlogs, executed to completion (allow substantial runtime):
python benchmarks/run.py --jobs 10000 --workers 16 --workloads noop
python benchmarks/run.py --jobs 100000 --workers 16 --workloads noop --timeout 86400
```

Each case exports every attempt as CSV, CPU/connection samples as JSON, and a summary
with revision, dirty flag, timestamp, platform, Docker version, and resolved Compose
configuration. No-op jobs execute `true`; Python jobs launch an interpreter and a
small computation. Both retain normal protocol and log-artifact costs.

Submission is measured with workers stopped, in atomic batches of at most 1,000.
End-to-end throughput includes worker startup and all commits/uploads. Scheduling
throughput is `(N-1)/(last first-attempt lease - first first-attempt lease)`; with
finite capacity this includes execution backpressure. Latency is submission to first
lease and deliberately includes backlog waiting. These are closed finite-batch
measurements, not open-loop arrival-rate or saturation estimates. Retries are retained.
The database CPU percentage is Docker's container CPU measure, not host CPU.
Connection counts include the sampler. Run multiple repetitions and report dispersion.

Recovery measurements: `python benchmarks/recovery.py --repeat 3`. It reports
time from confirmed worker termination to replacement lease and successful completion,
and from control-plane restart initiation to readiness and completion.

For a bounded 10K/100K queue-pressure experiment:

```sh
RUNGRID_TEST_DATABASE_URL='postgres://rungrid:rungrid@localhost:55432/rungrid?sslmode=disable' python benchmarks/backlog.py
```

This creates and removes an isolated temporary database schema for each case. It
submits incompatible GPU jobs, then measures 100 compatible CPU placements behind
that backlog through the production store transactions. It records every placement
sample and submission throughput. It measures database-layer placement under an
adverse resource filter; it does not execute 100K processes or claim their throughput.
Run it separately from other benchmarks/research to avoid contention.

Coverage: `RUNGRID_TEST_DATABASE_URL=postgres://rungrid:rungrid@localhost:55432/rungrid?sslmode=disable make test`.
Read `go tool cover -func=coverage.out`; do not report unit-only coverage as integration
coverage. CI uploads its raw coverage and failure-test logs. Large backlog results
are not claimed unless those commands have actually been run.
