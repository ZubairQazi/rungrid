# Verification evidence

Measured locally on September 13–14, 2026 (Detroit / UTC), on an Apple M5 Pro,
15 host CPUs, 24 GiB RAM. Docker Desktop had 15 CPUs and about 7.75 GiB RAM.
All workers ran on this single host; these are not multi-machine capacity claims.

## Correctness

- Go database, HTTP, gRPC, concurrency, and race-detector tests passed.
- All seven Compose integration/fault scenarios passed: batches/checksums, worker
  kill/checkpoint recovery, server restart, cancellation/timeout/manual retry,
  adding workers, network partition, and artifact outage.
- Handwritten Go `internal/` statement coverage: **76.5%**.
- Python SDK/worker unit statement-and-branch coverage: **58%**; generated protobuf
  files excluded. External Docker workers were tested but not coverage-instrumented.
- Protocol regeneration produced no diff; `go vet` passed.

Raw [Go coverage](evidence/go-coverage.txt), [Python coverage](evidence/python-coverage.txt),
and [hardware metadata](evidence/hardware.json) are archived. Reproduce with the
commands in the root README; never omit the PostgreSQL test environment variable.

## Scaling baseline

One 100-job batch per cell, each worker advertising two CPU slots. All 1,000 jobs
succeeded. Startup, RPCs, process launch, log upload, and terminal commitment are
included; metrics-sampler teardown is excluded. No warmup or confidence interval is
claimed. The 8-to-16 worker Python result illustrates finite-batch/host limitations.

| Workers | No-op jobs/s | Short Python jobs/s |
|---:|---:|---:|
| 1 | 11.48 | 8.38 |
| 2 | 20.05 | 14.49 |
| 4 | 28.92 | 21.86 |
| 8 | 31.55 | 31.98 |
| 16 | 37.26 | 29.06 |

![Measured throughput and p95 queue latency](evidence/scaling/scaling.png)

Every case includes raw attempts, submission/scheduling throughput, p50/p95/p99
submit-to-lease latency, and database CPU/connection samples. Source revision:
`7f43c01`, clean worktree. [Raw cases](evidence/scaling/).

```sh
python benchmarks/run.py --jobs 100 --workers 1 2 4 8 16 --workloads noop python --output results/scaling-v1
python benchmarks/plot.py results/scaling-v1
```

## Recovery and backlog

Three worker-kill trials produced replacement leases in **14.97–15.66 seconds**,
consistent with the configured 15-second lease and one-second reaper tick. The
five-second commands completed in about 20–21 seconds after termination. Three
server restarts became ready in **2.25–2.36 seconds**; all three active jobs completed
on their original attempts. [Raw recovery samples](evidence/recovery.json), source
`e4086b3`. Reproduce: `python benchmarks/recovery.py --repeat 3`.

| Incompatible queued jobs | Compatible probes | Placement p95 | Submission jobs/s |
|---:|---:|---:|---:|
| 10,000 | 100 | 6.40 ms | 1,045 |
| 100,000 | 100 | 21.17 ms | 1,048 |

This is a database-layer resource-filter stress case, not execution of 100K processes.
Each case used a temporary schema and removed it after saving results.
[Raw backlog samples](evidence/backlog.json), source `d662090`.

```sh
RUNGRID_TEST_DATABASE_URL='postgres://rungrid:rungrid@localhost:55432/rungrid?sslmode=disable' python benchmarks/backlog.py
```

## Real-data workload

**80/80 experiments completed**: two datasets, four valid regimes per dataset, two
models, five seeds. Forty split configurations passed preflight audits. DrugComb used
46,308 valid records from 120,000 raw rows; MovieLens used 100,836 ratings. Three
training epochs and 16-dimensional embeddings were used. Successful metrics were
downloaded from MinIO and SHA-256 verified before aggregation. Manifests, training
checkpoints, and execution logs remain in the local artifact store.

[Aggregated report](evidence/heterosplit/summary.md) ·
[Per-job metrics](evidence/heterosplit/metrics.json).
The adapter pins HeteroSplit revision `9636559fc724d8b162afcfcadfa2ea1c385ed292`.
Earlier packaging/sample-size failures remain in the local database as separate
jobs; they are not included in this successful verification grid or its report.
This is not a replication of published model accuracy. The original spec's 160-job
grid remains a scope difference, documented in the research NOTICE.

## Release status

The local v0.1.0 release includes annotated Git history, source, reproducible build
scripts, and Linux/macOS amd64/arm64 archives. Public GitHub publication and hosted
CI verification are pending explicit authorization; no hosted CI pass is claimed.
Automatic approval review blocked creation/push to `ZubairQazi/rungrid` because that
exact public destination and source publication had not been explicitly authorized.

[Evidence checksums](evidence/SHA256SUMS). Full methodology and limitations are in
[benchmarks/README.md](../benchmarks/README.md).
