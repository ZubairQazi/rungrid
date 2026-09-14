import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from rungrid import Client, checkpoint, resume_path
from rungrid_worker.artifacts import Artifacts, digest


def test_atomic_checkpoint(monkeypatch, tmp_path):
    monkeypatch.setenv("RUNGRID_CHECKPOINT_DIR", str(tmp_path))
    path = checkpoint({"epoch": 3})
    assert json.loads(path.read_text()) == {"epoch": 3}
    assert not list(tmp_path.glob("*.tmp"))
    monkeypatch.setenv("RUNGRID_CHECKPOINT_PATH", str(path))
    assert resume_path() == path


def test_client_preserves_request_id():
    seen = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            seen.append((self.path, self.headers["X-Request-ID"]))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"state":"CANCELLED"}')

        def log_message(self, *_):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        assert Client(f"http://127.0.0.1:{server.server_port}").cancel("job", "fixed")["state"] == "CANCELLED"
        assert seen == [("/v1/jobs/job/cancel", "fixed")]
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def test_artifact_retry_and_checksum(tmp_path, monkeypatch):
    path = tmp_path / "checkpoint.json"
    path.write_text('{"epoch":3}')
    calls = []
    failures = []

    class S3:
        def put_object(self, **kwargs):
            calls.append(kwargs["Key"])
            if len(calls) == 1:
                raise OSError("temporary outage")

        def download_file(self, bucket, key, target):
            from pathlib import Path
            Path(target).write_text("corrupt")

    monkeypatch.setattr("rungrid_worker.artifacts.boto3.client", lambda *a, **k: S3())
    monkeypatch.setattr("rungrid_worker.artifacts.time.sleep", lambda _: None)
    artifacts = Artifacts(lambda: failures.append(1))
    lease = {"job": {"id": "job", "definition": {"artifact_prefix": "demo"}}, "attempt_id": "attempt"}
    entry = artifacts.upload(path, lease, "checkpoint", lambda: True)
    assert len(failures) == 1 and calls[0] == calls[1]
    assert entry["checksum"] == digest(path)
    with pytest.raises(ValueError, match="checksum"):
        artifacts.restore(entry, tmp_path / "restored")
    with pytest.raises(RuntimeError, match="lease lost"):
        artifacts.upload(path, lease, "checkpoint", lambda: False)
