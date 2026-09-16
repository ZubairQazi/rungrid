#!/usr/bin/env bash
# Experimental single-node, single-GPU worker. Read docs/slurm.md first.
# Supply --account and --partition at submission; do not commit site credentials.
#SBATCH --job-name=rungrid-worker
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=4G
#SBATCH --gres=gpu:1
#SBATCH --time=01:00:00
#SBATCH --output=rungrid-worker-%j.out

set -euo pipefail
: "${SLURM_JOB_ID:?Submit this script with sbatch}"
: "${RUNGRID_GRPC_ADDR:?Set the compute-node-reachable control-plane address}"
: "${RUNGRID_S3_BUCKET:?Set an existing artifact bucket}"
: "${RUNGRID_WORKDIR:?Set an absolute shared research checkout path}"
: "${RUNGRID_PYTHON:?Set an absolute Python executable with RunGrid installed}"

# Keep admission below the allocation, leaving headroom for the worker itself.
# Adjust these together with SBATCH resources when adapting the template.
export RUNGRID_CPU=3
export RUNGRID_MEMORY_MB=3072
export RUNGRID_GPU=1
export RUNGRID_WORKER_ID="slurm-${SLURM_JOB_ID}-${SLURM_RESTART_COUNT:-0}"
# Avoid collisions when other allocations share a node; no fixed scrape endpoint.
export RUNGRID_METRICS_PORT=0
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1

cd "$RUNGRID_WORKDIR"
# srun establishes the job-step GPU environment. Do not override CUDA_VISIBLE_DEVICES.
# No pre-timeout checkpoint signal is implemented by this template.
exec srun --ntasks=1 "$RUNGRID_PYTHON" -m rungrid_worker.main
