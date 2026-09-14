"""Measure real worker-kill and control-plane restart recovery on the Compose cluster."""
import argparse
import json
import platform
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from rungrid import Client
from run import compose


def wait(fn, timeout=90):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            if result := fn():
                return result
        except OSError:
            pass
        time.sleep(0.05)
    raise TimeoutError("recovery deadline exceeded")


def case(client, fault):
    job = client.submit({"name": "recovery-benchmark", "command": ["python", "-c", "import time;time.sleep(5)"],
                         "environment": {}, "resources": {"cpu": 1, "memory_mb": 128, "gpu": 0},
                         "max_attempts": 3, "timeout_seconds": 60, "idempotency_key": str(uuid.uuid4()),
                         "artifact_prefix": "recovery-benchmark"})
    wait(lambda: client.get(job["id"])["state"] == "RUNNING")
    attempts = client.attempts(job["id"])
    result = {"fault": fault, "job_id": job["id"], "started_at": datetime.now(timezone.utc).isoformat()}
    if fault == "worker_kill":
        worker = attempts[-1]["worker_id"]
        target = None
        for container in compose("ps", "-q", "worker").splitlines():
            hostname = subprocess.check_output(["docker", "inspect", "-f", "{{.Config.Hostname}}", container], text=True).strip()
            if worker.startswith(hostname + "-"):
                target = container
                break
        if target is None:
            raise ValueError("worker not found")
        subprocess.run(["docker", "kill", target], check=True, capture_output=True)
        start = time.monotonic()
        wait(lambda: len(client.attempts(job["id"])) >= 2)
        result["replacement_lease_seconds"] = time.monotonic() - start
    else:
        start = time.monotonic()
        compose("restart", "server")
        wait(lambda: client.request("GET", "/readyz")["status"] == "ready")
        result["ready_seconds"] = time.monotonic() - start
    wait(lambda: client.get(job["id"])["state"] in ("SUCCEEDED", "FAILED"))
    result["completion_seconds"] = time.monotonic() - start
    result["attempts"] = client.attempts(job["id"])
    result["state"] = client.get(job["id"])["state"]
    if result["state"] != "SUCCEEDED":
        raise AssertionError(result)
    compose("up", "-d", "--scale", "worker=2", "worker")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--output", type=Path, default=Path("results/recovery.json"))
    args = parser.parse_args()
    results = {"revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
               "platform": platform.platform(), "samples": []}
    try:
        for _ in range(args.repeat):
            for fault in ("worker_kill", "control_plane_restart"):
                result = case(Client(), fault)
                results["samples"].append(result)
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(json.dumps(results, indent=2))
                print(json.dumps(result), flush=True)
    finally:
        compose("up", "-d", "--scale", "worker=2", "worker")
