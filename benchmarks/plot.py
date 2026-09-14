import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser()
parser.add_argument("directory", type=Path)
args = parser.parse_args()
rows = [json.loads(path.read_text()) for path in args.directory.rglob("summary.json")]
if not rows:
    parser.error("no benchmark results")
fig, axes = plt.subplots(1, 2, figsize=(10, 4))
for workload in sorted({r["workload"] for r in rows}):
    selected = sorted((r for r in rows if r["workload"] == workload), key=lambda r: r["workers"])
    axes[0].plot([r["workers"] for r in selected], [r["end_to_end_jobs_per_second"] for r in selected], "o-", label=workload)
    axes[1].plot([r["workers"] for r in selected], [r["scheduling_latency_seconds"]["p95"] for r in selected], "o-", label=workload)
for axis in axes:
    axis.set_xlabel("Workers (2 advertised CPU each by default)")
    axis.legend()
    axis.grid(alpha=0.2)
axes[0].set_ylabel("Completed jobs / second (startup included)")
axes[1].set_ylabel("p95 submit-to-lease latency (seconds)")
fig.tight_layout()
fig.savefig(args.directory / "scaling.png", dpi=160)
fig.savefig(args.directory / "scaling.svg")
