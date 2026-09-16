# Slurm integration: experimental worker allocation

This is an opt-in deployment template, not a tested Slurm backend. No cluster jobs
were submitted to validate it. Slurm schedules allocations; RunGrid schedules
individual commands inside them. Keep Slurm responsible for device assignment,
fair share, resource enforcement, and allocation lifetime.

## Prerequisites

- Obtain site approval for persistent services and worker networking. Host the
  control plane/PostgreSQL and artifact service on an approved service host, not
  a login node by assumption. Compute nodes must reach gRPC and S3; only the
  control plane needs PostgreSQL access.
- RunGrid currently uses unauthenticated, plaintext gRPC. Use an approved isolated
  network or authenticated encrypted tunnel; do not expose it to the public
  internet or untrusted cluster users. Compose's loopback bindings and default
  local credentials are not a remote deployment configuration.
- Provision the same artifact bucket for the server/workers, an S3 endpoint when
  using MinIO, and least-privilege credentials through the site's secret mechanism.
  Workers use boto3's standard AWS credential chain. Do not commit credentials or
  include them in job definitions/logs. See [configuration](api.md).
- Install RunGrid and training dependencies into a shared Python environment
  (Python 3.11+, tested locally with 3.12). For example, from the RunGrid checkout,
  run `/absolute/path/to/environment/bin/python -m pip install .`. Pin the research
  code/dependencies separately; do not install packages during every allocation.
- Put code and read-only datasets on compute-node-accessible storage. Workers
  inherit the research working directory; use absolute dataset paths. Load any
  site-required CUDA modules/environment before submission or adapt the template.

## Submit a pilot worker

From a Slurm submission host, replace these placeholders with approved values:

```sh
export RUNGRID_GRPC_ADDR=approved-service-host:9090
export RUNGRID_S3_BUCKET=research-rungrid
export RUNGRID_S3_ENDPOINT=https://approved-artifact-host  # omit for AWS S3
export RUNGRID_WORKDIR=/shared/research-checkout
export RUNGRID_PYTHON=/shared/environments/research/bin/python
sbatch --account=YOUR_ACCOUNT --partition=YOUR_GPU_PARTITION deploy/slurm/worker.sbatch
```

The template requests one GPU, four CPUs, 4 GiB memory, and one hour. It advertises
three CPUs and 3072 MiB to leave worker overhead. Tune both the allocation and
advertised capacity to your measured workload; these are not GNN sizing claims.
Use only a single-node, single-GPU allocation with this template. `srun` preserves
Slurm's job-step GPU assignment; never set `CUDA_VISIBLE_DEVICES` in submitted job
environments. RunGrid counts GPUs but does not bind individual devices.

Use a dedicated pilot RunGrid deployment/queue. Submit only compatible GPU jobs
with `resources.gpu=1`, `resources.cpu` at most 3, and `resources.memory_mb` at
most 3072. This limits the worker to one GPU job at a time. CPU-only jobs could
otherwise run alongside it; there are no queue labels or partition routing.
Resource requirements are admission declarations, not per-command isolation.
The worker's metrics port is dynamically assigned to avoid node-local collisions;
a stable Prometheus scrape configuration is not included.

After one worker succeeds, a two-worker pilot can use:

```sh
sbatch --account=YOUR_ACCOUNT --partition=YOUR_GPU_PARTITION --array=0-1%2 deploy/slurm/worker.sbatch
```

Each array task is a separate allocation/worker, not an experiment index. The
worker stays alive even when the queue is empty and consumes allocation time;
cancel unused allocations. RunGrid does not automatically submit replacements.

## Research sweep mapping

Keep your existing training commands and result-reporting logic. Generate one job
per independent configuration/fold/seed, with an idempotency key incorporating
code revision, configuration, seed, and data/split fingerprints. Do not submit the
entire sequential sweep as one job if you want experiment-level retry granularity.

For tune-then-train, first submit tuning jobs; a client-side coordinator should
validate their committed outputs before submitting training jobs with the selected
parameters. Aggregate only successful committed attempts. RunGrid has no DAG
engine and does not provide this research coordinator yet.

Write outputs to `RUNGRID_OUTPUT_DIR` and isolate external files by attempt ID.
Result fencing protects RunGrid state, not arbitrary writes to shared result
directories: stale processes must not overwrite newer attempts' research files.
Export verified committed artifacts into the layout expected by existing table
scripts rather than letting retries write directly into canonical result folders.

## Wall time, failures, and checkpoint limitations

Allocation expiry kills the worker. The control plane can requeue its job after
lease expiry if attempts remain, but execution waits for another compatible worker.
Slurm restarts and RunGrid retries are separate mechanisms; they are not reconciled.
A job timeout is per attempt, not aware of remaining Slurm allocation time.

SIGTERM asks the worker to stop accepting work and drain active attempts; it does
not ask the training process to save a checkpoint. The template does not add an
early warning signal or guarantee draining before the hard allocation deadline.
Wall-time-aware admission and a training checkpoint signal handler remain future work.

Current checkpoint discovery uploads new `*.json` paths in `RUNGRID_CHECKPOINT_DIR`
and restores a selected checkpoint at `RUNGRID_CHECKPOINT_PATH`. Use immutable,
atomically published checkpoint files. Lightning `.ckpt` binary files are not
automatically discovered, and `trainer.fit(..., ckpt_path=...)` must be wired by
the research adapter. Uploading a checkpoint does not by itself make training
resumable. Until that adapter is tested, expect interrupted GNN attempts to restart
from scratch, not resume training state.

## Cluster acceptance checklist (not yet executed)

- Verify reachability and registration from a compute node without exposing services.
- Run one short GPU command and confirm only the allocated GPU is visible.
- Check resource usage, successful output upload, checksum, and committed result.
- Cancel a test allocation; start a replacement and verify retry and stale fencing.
- Validate real model/optimizer/RNG/split restoration before claiming checkpoint recovery.
- Test wall-time expiry, idle allocation cleanup, and two concurrent allocations.
- Confirm result inventory generation excludes partial and superseded attempts.

For fixed sweeps, a manifest plus native Slurm arrays may be simpler than operating
RunGrid services. This integration is useful when durable per-experiment history
and selective retry across allocations justify the additional operational work.

References: [Slurm sbatch](https://slurm.schedmd.com/sbatch.html),
[GPU/GRES environment](https://slurm.schedmd.com/gres.html), and
[job arrays](https://slurm.schedmd.com/job_array.html).
