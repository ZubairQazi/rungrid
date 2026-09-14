# APIs and configuration

REST binds to `127.0.0.1:8080`, gRPC to `127.0.0.1:9090`. Compose exposes both on host
loopback. v0.1 has no authentication or tenant isolation.

| Method | Path | Behavior |
|---|---|---|
| POST | `/v1/jobs` | Submit a definition; mandatory idempotency key |
| POST | `/v1/jobs/batch` | Atomic JSON array of 1–1,000 definitions |
| GET | `/v1/jobs` | Oldest first; `limit=1..1000`, `offset>=0` |
| GET | `/v1/jobs/{id}` | Definition, state, effective retry budget, timestamps |
| POST | `/v1/jobs/{id}/cancel` | Cancel queued/active work |
| POST | `/v1/jobs/{id}/retry` | Requeue failed job; explicitly extend budget by one |
| GET | `/v1/jobs/{id}/attempts` | Paginated history, lease tokens redacted |
| GET | `/v1/jobs/{id}/events` | Paginated append-only events |
| GET | `/v1/jobs/{id}/artifacts` | Paginated committed artifact metadata |
| GET | `/v1/workers` | Capacity, reservations, heartbeat timestamps; first 1,000 |
| GET | `/healthz`, `/readyz` | Process liveness / database readiness |
| GET | `/metrics` | Prometheus metrics |

Cancel/retry require `X-Request-ID`; reuse it on transport retries. Submission uses
`idempotency_key`, which cannot identify different definitions. Success/replay returns
200; invalid input 400, unknown IDs 404, conflicts 409. Bodies are capped at 4 MiB.
Batch conflicts roll back the entire batch. Pagination does not guarantee a snapshot.

## gRPC

`proto/rungrid.proto` defines eight unary RPCs; LeaseJob long-polls up to 20 seconds.
The v1 envelope types identity/fencing fields; `payload_json` carries resources,
hostname, logs, exit code, reason, and artifacts. Responses contain JSON job/lease
data. This trades field-level protobuf typing for a shared JSON schema initially.
Generated Go/Python bindings are committed and checked for drift in CI.

A worker ID belongs to one session. Use a fresh ID after restart; a different session
cannot revive an old ID. ReleaseWorker drains; active heartbeats still renew leases.
Every mutation requires `request_id`. Repeating method/request/payload returns its
original receipt; changing payload conflicts. Receipt replay acknowledges history,
not current lease validity. Workers track conservative monotonic deadlines from the
original send time, including retries. Timeouts include startup, execution, and uploads.

## Settings

| Environment variable | Default / purpose |
|---|---|
| `DATABASE_URL` | Control-plane PostgreSQL URL |
| `RUNGRID_HTTP_ADDR` | `127.0.0.1:8080` |
| `RUNGRID_GRPC_ADDR` | Server bind / worker connect address |
| `RUNGRID_LEASE_SECONDS` | 15, minimum 2 |
| `RUNGRID_WORKER_ID` | hostname + UUID |
| `RUNGRID_CPU`, `RUNGRID_MEMORY_MB`, `RUNGRID_GPU` | 2, 4096, 0 |
| `RUNGRID_S3_ENDPOINT`, `RUNGRID_S3_BUCKET` | AWS default endpoint, `rungrid` |
| `AWS_*` | Standard S3 credential chain / region |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Optional server OTLP export |
| `RUNGRID_URL`, `RUNGRID_REQUEST_ID` | Go CLI endpoint / replay identity |

Children receive reserved `RUNGRID_*` IDs, attempt number, output/checkpoint paths,
and an optional restored checkpoint path. Resources are admission reservations, not
cgroups. GPU counts do not assign device identities. Commands run as a non-root user.

## Operations and limits

Back up PostgreSQL and artifacts together. Receipts/events/attempts are retained
indefinitely; plan retention before sustained use. Historical metric percentiles scan
attempt history. Replace them with incremental aggregates at larger scale. Worker log
queues are bounded; overflow is omitted from events but retained in the log artifact.
Temporary artifacts/logs must fit local disk. Orphaned uploads can remain after crashes.
Available-capacity metrics report zero for draining workers or workers with no contact
for two lease durations; their historical heartbeat-age series remain visible.

The local demo pins an archived MinIO image from Quay for reproducibility, not as a
maintained production storage recommendation. Any compatible S3 destination can be
configured. [MinIO upstream status](https://github.com/minio/minio).
