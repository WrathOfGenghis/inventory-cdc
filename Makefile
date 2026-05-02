# Inventory CDC orchestrator — developer tasks.
#
# `make help` prints the available targets. Targets are kept idempotent
# wherever possible so a re-run does not need a clean step in between.

.PHONY: help up down migrate connector load bench bench.sustained bench.burst \
        bench.all demo lint test build image \
        helm-template helm-lint clean

COMPOSE := docker compose -f deploy/docker-compose.yaml
PSQL_WAREHOUSE := postgresql://postgres:postgres@localhost:5432/warehouse

help:
	@echo "make up             - bring up the local Docker Compose stack"
	@echo "make down           - tear it down (keep volumes)"
	@echo "make clean          - tear down AND wipe Postgres/Kafka volumes"
	@echo "make migrate        - run schema/postgres/*.sql in order"
	@echo "make connector      - register the Debezium connector"
	@echo "make load           - run the headline load-test scenario"
	@echo "make bench          - run all load-test scenarios and aggregate"
	@echo "make bench.sustained- 800 ev/s sustained scenario only (3 runs)"
	@echo "make bench.burst    - 100k burst scenario only (3 runs)"
	@echo "make bench.all      - run every scenario (sustained + burst)"
	@echo "make demo           - one-shot: up + migrate + connector + 60s load"
	@echo "make test           - go test -race ./..."
	@echo "make lint           - go vet + gofmt -l (enforced in CI)"
	@echo "make build          - build the orchestrator binary into bin/"
	@echo "make image          - build the Docker image"
	@echo "make helm-template  - render the Helm chart against values-prod.yaml"

# ---- Local stack -----------------------------------------------------------

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

migrate:
	@for f in schema/postgres/*.sql; do \
	  echo "applying $$f"; \
	  psql $(PSQL_WAREHOUSE) -v ON_ERROR_STOP=1 -f $$f; \
	done

connector:
	curl -X POST http://localhost:8083/connectors \
	  -H 'Content-Type: application/json' \
	  -d @deploy/debezium/connector.json

# ---- Load and benchmark ----------------------------------------------------

load:
	python load-test/load_gen.py --scenario sustained --rate 800 --duration 60

bench: bench.all

bench.sustained:
	mkdir -p results
	python load-test/load_gen.py --scenario sustained --rate 800 --duration 60 \
	  --runs 3 --report results/sustained.json

bench.burst:
	mkdir -p results
	python load-test/load_gen.py --scenario burst --total 10000 \
	  --runs 3 --report results/burst.json

bench.all: bench.sustained bench.burst
	python load-test/benchmark.py --aggregate results/ --out figures/

# `make demo` wraps the steps a reviewer needs to see the pipeline working
# end-to-end: stack up, schema applied, connector registered, 60 seconds of
# synthetic traffic. Referenced in §21.6 of the design report.
demo: up
	@echo "[demo] waiting for Postgres to be ready..."
	@for i in $$(seq 1 30); do \
	  $(COMPOSE) exec -T postgres pg_isready -U postgres >/dev/null 2>&1 && break; \
	  sleep 2; \
	done
	$(MAKE) migrate
	@echo "[demo] waiting for Kafka Connect to be ready..."
	@for i in $$(seq 1 30); do \
	  curl -sf http://localhost:8083/connectors >/dev/null 2>&1 && break; \
	  sleep 2; \
	done
	$(MAKE) connector
	@echo "[demo] driving 60s of synthetic traffic..."
	$(MAKE) load
	@echo "[demo] done. Grafana: http://localhost:3000 (admin/admin)"

# ---- Build and CI gates ----------------------------------------------------

test:
	go test -race ./...

lint:
	go vet ./...
	@if [ -n "$$(gofmt -l . | grep -v vendor/)" ]; then \
	  echo "gofmt -l failed:"; gofmt -l .; exit 1; \
	fi

build:
	mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
	  -o bin/orchestrator ./cmd/orchestrator

image:
	docker build -t inventory-cdc/orchestrator:dev .

# ---- Helm ------------------------------------------------------------------

helm-template:
	helm template inventory-cdc deploy/helm/inventory-cdc \
	  -f deploy/helm/inventory-cdc/values-prod.yaml

helm-lint:
	helm lint deploy/helm/inventory-cdc
