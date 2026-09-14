"""Destructive fault tests confined to the rungrid Compose project and test-owned jobs."""
import hashlib
import json
import os
import subprocess
import time
import uuid

import boto3
import pytest
from rungrid import Client

pytestmark = [pytest.mark.integration,
              pytest.mark.skipif(os.environ.get("RUNGRID_INTEGRATION") != "1", reason="opt-in Compose cluster required")]
COMPOSE = ["docker", "compose", "-f", "deploy/docker-compose.yml"]


def compose(*args):
    return subprocess.check_output(COMPOSE + list(args), text=True).strip()


def wait(predicate, timeout=60):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise AssertionError("condition timed out")


def definition(command=None, attempts=3, timeout=60):
    return {"name": "integration", "command": command or ["python", "-c", "print('hello')"],
            "environment": {}, "resources": {"cpu": 1, "memory_mb": 128, "gpu": 0},
            "max_attempts": attempts, "timeout_seconds": timeout,
            "idempotency_key": "test-" + str(uuid.uuid4()), "artifact_prefix": "integration"}


@pytest.fixture
def client():
    c = Client(os.environ.get("RUNGRID_URL", "http://localhost:8080"))
    owned = []
    original = c.submit

    def submit(job):
        result = original(job)
        owned.append(result["id"])
        return result

    c.submit = submit
    yield c
    for job_id in owned:
        if c.get(job_id)["state"] in ("QUEUED", "LEASED", "RUNNING"):
            c.cancel(job_id)


def terminal(client, job_id, timeout=60):
    return wait(lambda: (j if (j := client.get(job_id))["state"] in ("SUCCEEDED", "FAILED", "CANCELLED") else None), timeout)


def s3():
    return boto3.client("s3", endpoint_url="http://localhost:9000", aws_access_key_id="rungrid",
                        aws_secret_access_key="rungrid-secret", region_name="us-east-1")


def running_container(client, job_id):
    wait(lambda: client.get(job_id)["state"] == "RUNNING")
    wid = client.attempts(job_id)[-1]["worker_id"]
    for container in compose("ps", "-q", "worker").splitlines():
        hostname = subprocess.check_output(["docker", "inspect", "-f", "{{.Config.Hostname}}", container], text=True).strip()
        if wid.startswith(hostname + "-"):
            return container
    raise AssertionError(f"worker container not found: {wid}")


def test_batch_and_checksums(client):
    definitions = [definition() for _ in range(12)]
    jobs = client.batch(definitions)
    assert [j["id"] for j in jobs] == [j["id"] for j in client.batch(definitions)]
    for job in jobs:
        assert terminal(client, job["id"])["state"] == "SUCCEEDED"
        entries = client.artifacts(job["id"])
        assert any(a["kind"] == "log" for a in entries)
        for artifact in entries:
            body = s3().get_object(Bucket="rungrid", Key=artifact["object_key"])["Body"].read()
            assert len(body) == artifact["size_bytes"]
            assert hashlib.sha256(body).hexdigest() == artifact["checksum"]


def test_worker_kill_checkpoint_recovery(client):
    job = client.submit(definition(["python", "examples/checkpoint_counter.py"]))
    container = running_container(client, job["id"])
    wait(lambda: any(a["kind"] == "checkpoint" for a in client.artifacts(job["id"])))
    subprocess.run(["docker", "kill", container], check=True, capture_output=True)
    try:
        assert terminal(client, job["id"], 80)["state"] == "SUCCEEDED"
        attempts = client.attempts(job["id"])
        assert [a["attempt_number"] for a in attempts] == [1, 2]
        assert attempts[0]["state"] == "EXPIRED"
        logs = [a for a in client.artifacts(job["id"]) if a["attempt_id"] == attempts[-1]["id"] and a["kind"] == "log"]
        data = s3().get_object(Bucket="rungrid", Key=logs[0]["object_key"])["Body"].read().decode()
        assert json.loads(data.splitlines()[0])["resumed_from"] > 0
    finally:
        # A new container gets a fresh worker identity; registration cannot revive an old session.
        compose("up", "-d", "--scale", "worker=2", "worker")


def test_control_plane_restart(client):
    job = client.submit(definition(["python", "-c", "import time;time.sleep(5);print('done')"]))
    running_container(client, job["id"])
    compose("restart", "server")
    wait(lambda: ready(client))
    assert terminal(client, job["id"])["state"] == "SUCCEEDED"


def ready(client):
    try:
        return client.request("GET", "/readyz")["status"] == "ready"
    except OSError:
        return False


def test_cancel_timeout_and_manual_retry(client):
    job = client.submit(definition(["python", "-c", "import time;time.sleep(30)"]))
    running_container(client, job["id"])
    rid = str(uuid.uuid4())
    assert client.cancel(job["id"], rid) == client.cancel(job["id"], rid)
    time.sleep(1)
    assert client.get(job["id"])["state"] == "CANCELLED"
    job = client.submit(definition(["python", "-c", "import time;time.sleep(10)"], attempts=1, timeout=2))
    assert terminal(client, job["id"])["state"] == "FAILED"
    job = client.submit(definition(["python", "-c", "import os;raise SystemExit(0 if int(os.environ['RUNGRID_ATTEMPT_NUMBER'])>1 else 1)"], attempts=1))
    assert terminal(client, job["id"])["state"] == "FAILED"
    client.retry(job["id"])
    assert terminal(client, job["id"])["state"] == "SUCCEEDED"
    assert len(client.attempts(job["id"])) == 2


def test_add_worker_during_execution(client):
    jobs = [client.submit(definition(["python", "-c", "import time;time.sleep(1)"])) for _ in range(10)]
    try:
        compose("up", "-d", "--scale", "worker=3", "worker")
        for job in jobs:
            assert terminal(client, job["id"])["state"] == "SUCCEEDED"
    finally:
        compose("up", "-d", "--scale", "worker=2", "worker")


def test_network_partition_fences_old_worker(client):
    job = client.submit(definition(["python", "-c", "import time;time.sleep(20)"]))
    container = running_container(client, job["id"])
    network = "rungrid_default"
    subprocess.run(["docker", "network", "disconnect", network, container], check=True, capture_output=True)
    try:
        wait(lambda: len(client.attempts(job["id"])) >= 2, timeout=45)
    finally:
        subprocess.run(["docker", "network", "connect", network, container], check=True, capture_output=True)
    assert terminal(client, job["id"], 60)["state"] == "SUCCEEDED"
    assert client.attempts(job["id"])[0]["state"] == "EXPIRED"


def test_artifact_outage_recovers(client):
    job = client.submit(definition(["python", "-c", "import time;time.sleep(1);print('upload me')"]))
    running_container(client, job["id"])
    compose("pause", "minio")
    try:
        time.sleep(4)
    finally:
        compose("unpause", "minio")
    assert terminal(client, job["id"])["state"] == "SUCCEEDED"
    assert client.artifacts(job["id"])
