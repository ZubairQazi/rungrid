"""Verify committed artifacts and aggregate only successful terminal attempts."""
import argparse
import hashlib
import json
import statistics
import time
from collections import defaultdict
from pathlib import Path

import boto3
from rungrid import Client

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("jobs", type=Path)
parser.add_argument("--output", type=Path, default=Path("results/heterosplit"))
parser.add_argument("--timeout", type=int, default=7200)
args = parser.parse_args()
client = Client()
jobs = json.loads(args.jobs.read_text())
deadline = time.monotonic() + args.timeout
while True:
    states = [client.get(j["id"]) for j in jobs]
    if all(j["state"] in ("SUCCEEDED", "FAILED", "CANCELLED") for j in states):
        break
    if time.monotonic() >= deadline:
        raise TimeoutError("research jobs still active")
    print(f"Completed {sum(j['state'] == 'SUCCEEDED' for j in states)}/{len(jobs)}", flush=True)
    time.sleep(5)
if any(j["state"] != "SUCCEEDED" for j in states):
    raise RuntimeError("not all experiments succeeded; inspect jobs before aggregating")
s3 = boto3.client("s3", endpoint_url="http://localhost:9000", aws_access_key_id="rungrid",
                   aws_secret_access_key="rungrid-secret", region_name="us-east-1")
rows = []
args.output.mkdir(parents=True, exist_ok=True)
for job in states:
    attempt = client.attempts(job["id"])[-1]
    artifacts = [a for a in client.artifacts(job["id"]) if a["attempt_id"] == attempt["id"]]
    metrics = [a for a in artifacts if a["object_key"].endswith("-metrics.json")]
    if len(metrics) != 1:
        raise ValueError("expected one committed metrics artifact")
    data = s3.get_object(Bucket="rungrid", Key=metrics[0]["object_key"])["Body"].read()
    if hashlib.sha256(data).hexdigest() != metrics[0]["checksum"]:
        raise ValueError("metrics checksum mismatch")
    rows.append(json.loads(data))
groups = defaultdict(list)
for row in rows:
    groups[(row.get("study", "original-v1"), row["dataset"], row["regime"], row["model"])].append(row["auc"])
lines = ["# HeteroSplit distributed experiment results", "", "A scheduler demonstration; reduced data/epochs are not a replication of the published study.", "",
         "Context holdouts use context-blind link predictors; month holdouts are not chronological forecasting.", "",
         "| Study | Dataset | Regime | Model | Seeds | AUC mean | AUC std |", "|---|---|---|---|---:|---:|---:|"]
for key, values in sorted(groups.items()):
    lines.append("| " + " | ".join(key) + f" | {len(values)} | {statistics.mean(values):.4f} | {statistics.stdev(values) if len(values)>1 else 0:.4f} |")
(args.output / "metrics.json").write_text(json.dumps(rows, indent=2))
(args.output / "summary.md").write_text("\n".join(lines) + "\n")
print(args.output / "summary.md")
