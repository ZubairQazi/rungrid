GO ?= go
PYTHON ?= python3
COMPOSE = docker compose -f deploy/docker-compose.yml
.PHONY: up down build test integration generate benchmark release-check
up:
	$(COMPOSE) up --build -d --scale worker=2
down:
	$(COMPOSE) down
build:
	$(GO) build -o bin/server ./cmd/server
	$(GO) build -o bin/rungrid ./cmd/cli
test:
	$(GO) test -race -coverprofile=coverage.out ./...
	$(PYTHON) -m pytest -m 'not integration' --cov=rungrid --cov=rungrid_worker
integration:
	RUNGRID_INTEGRATION=1 $(PYTHON) -m pytest -m integration -v
generate:
	$(PYTHON) scripts/generate.py
benchmark:
	$(PYTHON) benchmarks/run.py --jobs 100 --workers 1 2 4 8 16 --workloads noop python
release-check: test integration
	$(PYTHON) scripts/release.py --check
