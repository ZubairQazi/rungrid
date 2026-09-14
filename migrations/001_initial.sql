CREATE TABLE IF NOT EXISTS jobs (
 id uuid PRIMARY KEY, name text NOT NULL, idempotency_key text NOT NULL UNIQUE,
 definition jsonb NOT NULL, state text NOT NULL DEFAULT 'QUEUED'
 CHECK (state IN ('QUEUED','LEASED','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
 cpu integer NOT NULL CHECK(cpu>=0), memory_mb integer NOT NULL CHECK(memory_mb>=0),
 gpu integer NOT NULL CHECK(gpu>=0), max_attempts integer NOT NULL CHECK(max_attempts>0),
 timeout_seconds integer NOT NULL CHECK(timeout_seconds>0),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(created_at,id) WHERE state='QUEUED';
CREATE TABLE IF NOT EXISTS workers (
 id text PRIMARY KEY, session_id text NOT NULL, hostname text NOT NULL,
 cpu integer NOT NULL CHECK(cpu>=0), memory_mb integer NOT NULL CHECK(memory_mb>=0),
 gpu integer NOT NULL CHECK(gpu>=0), state text NOT NULL CHECK(state IN ('ACTIVE','DRAINING')),
 registered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 last_heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS attempts (
 id uuid PRIMARY KEY, job_id uuid NOT NULL REFERENCES jobs(id),
 attempt_number integer NOT NULL, worker_id text NOT NULL REFERENCES workers(id),
 state text NOT NULL CHECK(state IN ('LEASED','RUNNING','SUCCEEDED','FAILED','EXPIRED','CANCELLED')),
 lease_token uuid NOT NULL, lease_expires_at timestamptz NOT NULL,
 deadline_at timestamptz NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 started_at timestamptz, finished_at timestamptz, exit_code integer, failure_reason text,
 UNIQUE(job_id,attempt_number)
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_attempt ON attempts(job_id) WHERE state IN ('LEASED','RUNNING');
CREATE INDEX IF NOT EXISTS worker_active ON attempts(worker_id) WHERE state IN ('LEASED','RUNNING');
CREATE INDEX IF NOT EXISTS expired_attempts ON attempts(lease_expires_at) WHERE state IN ('LEASED','RUNNING');
CREATE TABLE IF NOT EXISTS artifacts (
 id uuid PRIMARY KEY, job_id uuid NOT NULL REFERENCES jobs(id), attempt_id uuid NOT NULL REFERENCES attempts(id),
 object_key text NOT NULL UNIQUE, content_type text NOT NULL, size_bytes bigint NOT NULL CHECK(size_bytes>=0),
 checksum text NOT NULL, kind text NOT NULL CHECK(kind IN ('checkpoint','output','log')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS artifacts_job ON artifacts(job_id,created_at DESC);
CREATE TABLE IF NOT EXISTS events (
 id bigserial PRIMARY KEY, job_id uuid NOT NULL REFERENCES jobs(id), attempt_id uuid REFERENCES attempts(id),
 event_type text NOT NULL, payload jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS events_job ON events(job_id,id);
CREATE TABLE IF NOT EXISTS requests (
 scope text NOT NULL, request_id text NOT NULL, payload_hash text NOT NULL, response jsonb,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(scope,request_id)
);
