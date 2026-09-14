"""Submit the original study's eight dataset/regime combinations as independent jobs."""
import argparse
import json
from pathlib import Path
from rungrid import Client

REGIMES = {
    "DrugComb": ["random", "pair_cold_start", "either_cold_start", "both_cold_start"],
    "MovieLens": ["random", "source_cold_start", "destination_cold_start", "both_cold_start"],
}


def definitions(run_id, seeds=5, epochs=3, max_rows=10000):
    for dataset, regimes in REGIMES.items():
        data = "/data/summary_v_1_5.csv" if dataset == "DrugComb" else "/data/ml/ml-latest-small/ratings.csv"
        for regime in regimes:
            for model in ["MF", "SAGE"]:
                for seed in range(seeds):
                    name = f"{dataset}-{regime}-{model}-{seed}"
                    yield {"name": name, "command": ["python", "examples/heterosplit/task.py", "--dataset", dataset,
                           "--data", data, "--regime", regime, "--model", model, "--seed", str(seed),
                           "--epochs", str(epochs), "--max-rows", str(max_rows)],
                           "environment": {"OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1"},
                           "resources": {"cpu": 1, "memory_mb": 2048, "gpu": 0}, "max_attempts": 3,
                           "timeout_seconds": 1800, "idempotency_key": f"heterosplit-{run_id}-{name}",
                           "artifact_prefix": f"heterosplit/{run_id}/{name}"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--seeds", type=int, default=5)
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--max-rows", type=int, default=10000)
    parser.add_argument("--output", type=Path, default=Path("results/heterosplit/jobs.json"))
    args = parser.parse_args()
    jobs = Client().batch(list(definitions(args.run_id, args.seeds, args.epochs, args.max_rows)))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(jobs, indent=2))
    print(f"Submitted {len(jobs)} jobs; tracking file: {args.output}")
