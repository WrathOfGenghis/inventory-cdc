# Inventory CDC orchestrator — developer tasks.
#
# `make help` prints the available targets. Targets are kept idempotent
# wherever possible so a re-run does not need a clean step in between.

.PHONY: help up down migrate connector load bench lint test build image \
        helm-template helm-lint clean

COMPOSE := docker compose -f deploy/docker-compose.yaml
PSQL_WAREHOUSE := postgresql://postgres:postgres@localhost:5432/warehouse
PSQL_WEBSITE   := postgresql://postgres:postgres@localhost:5432/website

help:
	@echo "make up           - bring up the local Docker Compose stack"
	@echo "make down         - tear it down (keep volumes)"
	@echo "make clean        - tear down AND wipe Postgres/Kafka volumes"
	@echo "make migrate      - run schema/postgres/*.sql in order"
	@echo "make connector    - register the Debezium connector"
	@echo "make load         - run the headline load-test scenario"
	@echo "make bench        - run all load-test scenarios and aggregate"
	@echo "make test         - go test -race ./..."
	@echo "make lint         - go vet + gofmt -l (enforced in CI)"
	@echo "make build        - build the orchestrator binary into bin/"
	@echo "make image        - build the Docker image"
	@echo "make helm-template- render the Helm chart against values-prod.yaml"

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

bench:
	mkdir -p results
	python load-test/load_gen.py --scenario sustained --rate 800 --duration 60 \
	  --report results/sustained.json
	python load-test/load_gen.py --scenario burst --total 10000 \
	  --report results/burst.json
	python load-test/benchmark.py --aggregate results/

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
