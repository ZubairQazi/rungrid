"""Run isolated 10K/100K database queue-pressure cases and archive raw samples."""
import argparse
import json
import os
import platform
import subprocess
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--jobs", nargs="+", type=int, default=[10000, 100000])
parser.add_argument("--output", type=Path, default=Path("results/backlog.json"))
args = parser.parse_args()
subprocess.run([os.environ.get("GO", "go"), "build", "-o", "bin/benchqueue", "./cmd/benchqueue"], check=True)
results = {"revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
           "platform": platform.platform(), "samples": []}
for count in args.jobs:
    result = json.loads(subprocess.check_output(["bin/benchqueue", "-jobs", str(count)], text=True))
    results["samples"].append(result)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(results, indent=2))
    print(json.dumps({k: v for k, v in result.items() if k != "placement_samples_seconds"}), flush=True)
