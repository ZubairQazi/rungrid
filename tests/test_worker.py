import json
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

from rungrid_worker.main import Worker


def test_worker_kills_command_on_lease_loss(monkeypatch, tmp_path):
    """A disconnected worker cannot keep executing beyond its admitted lease."""
    import grpc

    class Unavailable(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.UNAVAILABLE

    calls = []

    class RPC:
        def call(self, method, *args, **kwargs):
            calls.append(method)
            if method == "Heartbeat":
                raise Unavailable()
            return {}

    monkeypatch.setattr("rungrid_worker.main.Artifacts", lambda *_: object())
    marker = tmp_path / "should-not-exist"
    worker = Worker()
    worker.rpc.channel.close()
    worker.rpc = RPC()
    lease = {"local_deadline": time.monotonic() + 1, "attempt_id": "a", "attempt_number": 1,
             "job": {"id": "j", "definition": {"command": ["python3", "-c",
                     f"import time;from pathlib import Path;time.sleep(3);Path({str(marker)!r}).touch()"], "environment": {}}}}
    worker.execute(lease)
    time.sleep(2.2)
    assert not marker.exists()
    assert "CompleteAttempt" not in calls


def test_function_invocation(monkeypatch, tmp_path):
    monkeypatch.setenv("RUNGRID_OUTPUT_DIR", str(tmp_path))
    subprocess.run([os.sys.executable, "-m", "rungrid.invoke", "builtins:sum", "--args", "[[1,2,3]]"], check=True)
    assert json.loads((tmp_path / "result.json").read_text()) == 6
