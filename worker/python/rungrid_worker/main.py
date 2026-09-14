import concurrent.futures
import json
import os
import queue
import signal
import socket
import subprocess
import tempfile
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import grpc
from .artifacts import Artifacts
from .rpc import RPC


def kill_group(process):
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    process.wait()


class Worker:
    def __init__(self):
        self.worker_id = os.environ.get("RUNGRID_WORKER_ID", f"{socket.gethostname()}-{uuid.uuid4()}")
        self.rpc = RPC(os.environ.get("RUNGRID_GRPC_ADDR", "localhost:9090"), self.worker_id, str(uuid.uuid4()))
        self.resources = {"cpu": int(os.environ.get("RUNGRID_CPU", "2")),
                          "memory_mb": int(os.environ.get("RUNGRID_MEMORY_MB", "4096")),
                          "gpu": int(os.environ.get("RUNGRID_GPU", "0"))}
        self.stopping = threading.Event()
        self.upload_failures = 0
        self.metric_lock = threading.Lock()

    def upload_failed(self):
        with self.metric_lock:
            self.upload_failures += 1

    def log(self, message, lease=None, request_id="", **fields):
        print(json.dumps({"message": message, "job_id": lease["job"]["id"] if lease else "",
                          "attempt_id": lease["attempt_id"] if lease else "", "worker_id": self.worker_id,
                          "request_id": request_id, **fields}), flush=True)

    def execute(self, lease):
        done = threading.Event()
        lost = threading.Event()
        deadline = [lease["local_deadline"]]
        process = None

        def alive():
            return not lost.is_set() and time.monotonic() < deadline[0]

        def renew():
            while not done.wait(0.5):
                if not alive():
                    lost.set()
                    if process is not None:
                        kill_group(process)
                    return
                try:
                    result = self.rpc.call("Heartbeat", lease, timeout=min(2, max(0.1, deadline[0] - time.monotonic())))
                    deadline[0] = result["local_deadline"]
                except grpc.RpcError as exc:
                    if exc.code() in (grpc.StatusCode.FAILED_PRECONDITION, grpc.StatusCode.NOT_FOUND):
                        lost.set()

        heartbeat = threading.Thread(target=renew, daemon=True)
        heartbeat.start()
        try:
            if not alive():
                return
            self.rpc.call("StartAttempt", lease, timeout=max(0.1, deadline[0] - time.monotonic()))
            artifacts = Artifacts(self.upload_failed)
            with tempfile.TemporaryDirectory(prefix="rungrid-") as directory:
                root = Path(directory)
                outputs, checkpoints = root / "outputs", root / "checkpoints"
                outputs.mkdir()
                checkpoints.mkdir()
                env = os.environ | lease["job"]["definition"].get("environment", {})
                env.update(RUNGRID_JOB_ID=lease["job"]["id"], RUNGRID_ATTEMPT_ID=lease["attempt_id"],
                           RUNGRID_ATTEMPT_NUMBER=str(lease["attempt_number"]), RUNGRID_OUTPUT_DIR=str(outputs),
                           RUNGRID_CHECKPOINT_DIR=str(checkpoints), PYTHONUNBUFFERED="1")
                env.pop("RUNGRID_CHECKPOINT_PATH", None)
                if lease.get("checkpoint"):
                    restored = root / "resume.json"
                    artifacts.restore(lease["checkpoint"], restored)
                    env["RUNGRID_CHECKPOINT_PATH"] = str(restored)
                if not alive():
                    return
                process = subprocess.Popen(lease["job"]["definition"]["command"], env=env,
                                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                           start_new_session=True)
                lines = queue.Queue(maxsize=1000)
                log_path = root / "execution.log"

                def read_logs():
                    with log_path.open("wb") as stream:
                        while chunk := process.stdout.readline(8192):
                            stream.write(chunk)
                            try:
                                lines.put_nowait(chunk.decode(errors="replace").rstrip())
                            except queue.Full:
                                pass  # Full log remains in the durable artifact.

                reader = threading.Thread(target=read_logs, daemon=True)
                reader.start()
                uploaded = set()

                def progress():
                    batch = []
                    for _ in range(100):
                        try:
                            batch.append(lines.get_nowait())
                        except queue.Empty:
                            break
                    entries = []
                    for path in sorted(checkpoints.glob("*.json"), key=lambda p: p.stat().st_mtime_ns):
                        if path not in uploaded:
                            entries.append(artifacts.upload(path, lease, "checkpoint", alive))
                            uploaded.add(path)
                    if batch or entries:
                        rid = str(uuid.uuid4())
                        self.rpc.call("ReportProgress", lease, logs=batch, artifacts=entries, request_id=rid,
                                      timeout=max(0.1, min(10, deadline[0] - time.monotonic())))
                        for line in batch:
                            self.log(line, lease, rid)

                while process.poll() is None:
                    if not alive():
                        kill_group(process)
                        return
                    progress()
                    time.sleep(0.1)
                reader.join(timeout=3)
                if reader.is_alive():
                    # Descendants must not outlive the command and hold log pipes open.
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    reader.join(timeout=3)
                if not alive():
                    return
                progress()
                entries = [artifacts.upload(log_path, lease, "log", alive)]
                for path in sorted(outputs.rglob("*")):
                    if path.is_file() and not path.is_symlink():
                        entries.append(artifacts.upload(path, lease, "output", alive))
                # Metadata batches stay under the RPC's bounded payload size.
                for index in range(0, len(entries), 100):
                    self.rpc.call("ReportProgress", lease, artifacts=entries[index:index + 100])
                method = "CompleteAttempt" if process.returncode == 0 else "FailAttempt"
                self.rpc.call(method, lease, exit_code=process.returncode,
                              reason="" if process.returncode == 0 else "command exited nonzero")
                self.log(method, lease, exit_code=process.returncode)
        except Exception as exc:
            self.log("attempt error", lease, error=str(exc))
            if alive():
                try:
                    self.rpc.call("FailAttempt", lease, exit_code=-1, reason=str(exc)[:8192])
                except grpc.RpcError:
                    pass
        finally:
            if process is not None:
                kill_group(process)
                if process.stdout:
                    process.stdout.close()
            done.set()
            heartbeat.join(timeout=4)

    def run(self):
        self.rpc.call("RegisterWorker", hostname=socket.gethostname(), resources=self.resources, timeout=60)
        self.log("registered", resources=self.resources)
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.resources["cpu"]) as pool:
            futures = set()
            last_heartbeat = 0.0
            while not self.stopping.is_set():
                for future in list(futures):
                    if future.done():
                        future.result()
                        futures.remove(future)
                try:
                    if time.monotonic() - last_heartbeat > 3:
                        self.rpc.call("Heartbeat", timeout=3)
                        last_heartbeat = time.monotonic()
                    if len(futures) >= self.resources["cpu"]:
                        self.stopping.wait(0.1)
                        continue
                    lease = self.rpc.call("LeaseJob", wait_seconds=2, timeout=5)
                    if not lease.get("idle"):
                        futures.add(pool.submit(self.execute, lease))
                    else:
                        self.stopping.wait(0.2)
                except grpc.RpcError as exc:
                    self.log("control plane unavailable", error=str(exc))
                    self.stopping.wait(1)
            self.rpc.call("ReleaseWorker", timeout=5)
        self.rpc.channel.close()


def main():
    worker = Worker()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: worker.stopping.set())

    class Metrics(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/metrics":
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.end_headers()
            self.wfile.write(f"# TYPE rungrid_artifact_upload_failures_total counter\nrungrid_artifact_upload_failures_total {worker.upload_failures}\n".encode())

        def log_message(self, *_):
            pass

    metrics = ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("RUNGRID_METRICS_PORT", "9101"))), Metrics)
    threading.Thread(target=metrics.serve_forever, daemon=True).start()
    try:
        worker.run()
    finally:
        metrics.shutdown()


if __name__ == "__main__":
    main()
