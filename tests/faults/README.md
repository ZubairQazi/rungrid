# Failure scenarios

The runnable fault suite is `tests/integration/test_cluster.py` (opt-in with
`RUNGRID_INTEGRATION=1`). It kills a selected worker, restarts the control plane,
disconnects a worker network, pauses MinIO, cancels running commands, expires
timeouts, retries failures, and adds a worker while jobs run. `finally` blocks restore
network and service availability. Run only against the dedicated RunGrid Compose
project with no unrelated jobs; it never targets other Compose projects.

PostgreSQL concurrency tests in `internal/store/store_test.go` use unique temporary
schemas and cover cancellation/completion races, resource conservation, request
replays, stale heartbeats/results, session fencing, and monotonically numbered attempts.
