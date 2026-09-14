import json
import uuid
from urllib.request import Request, urlopen


class Client:
    def __init__(self, url="http://localhost:8080"):
        self.url = url.rstrip("/")

    def request(self, method, path, payload=None, request_id=None):
        data = json.dumps(payload).encode() if payload is not None else None
        req = Request(self.url + path, data=data, method=method, headers={
            "Content-Type": "application/json", "X-Request-ID": request_id or str(uuid.uuid4())})
        with urlopen(req, timeout=30) as response:
            return json.load(response)

    def submit(self, definition):
        return self.request("POST", "/v1/jobs", definition)

    def batch(self, definitions):
        return self.request("POST", "/v1/jobs/batch", definitions)

    def get(self, job_id):
        return self.request("GET", f"/v1/jobs/{job_id}")

    def list(self, limit=100, offset=0):
        return self.request("GET", f"/v1/jobs?limit={limit}&offset={offset}")

    def cancel(self, job_id, request_id=None):
        return self.request("POST", f"/v1/jobs/{job_id}/cancel", request_id=request_id)

    def retry(self, job_id, request_id=None):
        return self.request("POST", f"/v1/jobs/{job_id}/retry", request_id=request_id)

    def attempts(self, job_id):
        return self.request("GET", f"/v1/jobs/{job_id}/attempts?limit=1000")

    def artifacts(self, job_id):
        return self.request("GET", f"/v1/jobs/{job_id}/artifacts?limit=1000")
