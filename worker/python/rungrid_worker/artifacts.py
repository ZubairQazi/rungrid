import hashlib
import os
import time
import uuid
from pathlib import Path

import boto3
from botocore.config import Config


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


class Artifacts:
    def __init__(self, on_failure=lambda: None):
        self.bucket = os.environ.get("RUNGRID_S3_BUCKET", "rungrid")
        self.client = boto3.client("s3", endpoint_url=os.environ.get("RUNGRID_S3_ENDPOINT"),
                                   config=Config(connect_timeout=2, read_timeout=3, retries={"max_attempts": 0},
                                                 s3={"addressing_style": "path"}))
        self.on_failure = on_failure

    def upload(self, path, lease, kind, alive):
        path = Path(path)
        checksum = digest(path)
        prefix = lease["job"]["definition"].get("artifact_prefix", "").strip("/")
        key = "/".join(filter(None, [prefix, lease["job"]["id"], lease["attempt_id"], kind,
                                    f"{uuid.uuid4()}-{path.name}"]))
        for attempt in range(5):
            if not alive():
                raise RuntimeError("lease lost during upload")
            try:
                with path.open("rb") as stream:
                    self.client.put_object(Bucket=self.bucket, Key=key, Body=stream,
                                           Metadata={"sha256": checksum}, ContentType="application/octet-stream")
                return {"object_key": key, "content_type": "application/octet-stream",
                        "size_bytes": path.stat().st_size, "checksum": checksum, "kind": kind}
            except Exception:
                self.on_failure()
                if attempt == 4:
                    raise
                time.sleep(min(0.2 * 2 ** attempt, 2))

    def restore(self, artifact, target):
        self.client.download_file(self.bucket, artifact["object_key"], str(target))
        if digest(target) != artifact["checksum"]:
            raise ValueError("checkpoint checksum mismatch")
