import json
import time
import uuid
from datetime import datetime

import grpc
from rungrid import rungrid_pb2 as pb
from rungrid import rungrid_pb2_grpc as service


class RPC:
    def __init__(self, address, worker_id, session_id):
        self.channel = grpc.insecure_channel(address, options=[
            ("grpc.initial_reconnect_backoff_ms", 100),
            ("grpc.min_reconnect_backoff_ms", 100),
            ("grpc.max_reconnect_backoff_ms", 1000),
            ("grpc.dns_min_time_between_resolutions_ms", 1000),
        ])
        self.stub = service.WorkerServiceStub(self.channel)
        self.worker_id, self.session_id = worker_id, session_id

    def call(self, method, lease=None, timeout=10, wait_seconds=0, request_id=None, **payload):
        req = pb.Request(request_id=request_id or str(uuid.uuid4()), worker_id=self.worker_id,
                         session_id=self.session_id, payload_json=json.dumps(payload).encode(),
                         attempt_id=lease["attempt_id"] if lease else "",
                         lease_token=lease["lease_token"] if lease else "", wait_seconds=wait_seconds)
        started = time.monotonic()
        end = started + timeout
        delay = 0.1
        while True:
            try:
                response = getattr(self.stub, method)(req, timeout=max(0.01, min(5 + wait_seconds, end - time.monotonic())), wait_for_ready=True)
                result = json.loads(response.payload_json)
                if "lease_expires_at" in result:
                    ttl = (datetime.fromisoformat(result["lease_expires_at"].replace("Z", "+00:00")) -
                           datetime.fromisoformat(result["server_time"].replace("Z", "+00:00"))).total_seconds()
                    # Original send time survives transport retries and receipt replay.
                    result["local_deadline"] = started + ttl - 0.5
                return result
            except grpc.RpcError as exc:
                if exc.code() not in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED) or time.monotonic() + delay >= end:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 1)
