"""Local real-data model/checkpoint smoke test, not a distributed throughput test."""
import argparse
import json
import os
from pathlib import Path
from types import SimpleNamespace

from protocols import REGIMES
from task import run


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("use a fresh output directory to avoid mixing checkpoint histories")
    args.output.mkdir(parents=True)
    for dataset, regimes in REGIMES.items():
        path = args.data_dir / ("summary_v_1_5.csv" if dataset == "DrugComb" else "ml/ml-latest-small/ratings.csv")
        for regime in regimes:
            for model in ("MF", "SAGE"):
                root = args.output / f"{dataset}-{regime}-{model}"
                os.environ.update(RUNGRID_OUTPUT_DIR=str(root / "outputs"),
                                  RUNGRID_CHECKPOINT_DIR=str(root / "checkpoints"),
                                  RUNGRID_JOB_ID=root.name, RUNGRID_ATTEMPT_ID="local-smoke")
                os.environ.pop("RUNGRID_CHECKPOINT_PATH", None)
                config = SimpleNamespace(dataset=dataset, data=path, regime=regime, model=model,
                                         seed=0, epochs=1, dim=16, max_rows=120000, study="expanded-v2")
                run(config)
                metrics_path = root / "outputs" / "metrics.json"
                before = json.loads(metrics_path.read_text())
                os.environ["RUNGRID_CHECKPOINT_PATH"] = str(next((root / "checkpoints").glob("*.json")))
                run(config)
                after = json.loads(metrics_path.read_text())
                assert after["resumed_epoch"] == 1
                assert before["auc"] == after["auc"] and before["ap"] == after["ap"]
    print("32 local training/checkpoint round trips passed (seed 0, one epoch); not 160 distributed completions.")
