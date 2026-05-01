# Inventory CDC — Live Sync Pipeline

Streaming Change Data Capture pipeline that keeps the e-commerce website
inventory store in sync with the warehouse system in near real time.
Replaces the legacy nightly ETL job with a continuous log-based stream
that delivers sub-second median sync latency.

> Companion code for the design report
> *E-Commerce Inventory Live Sync — CDC, Project 7, Enhanced Report v2*.

## Architecture at a glance

```
Postgres (warehouse)  ──WAL──▶  Debezium  ──▶  Kafka  ──▶  Go orchestrator
                                                          │
                              ┌───────────────────────────┤
                              ▼                           ▼
                     website_inventory             Iceberg audit log
                     (read model)                  (long-term history)
```

| Layer | Tool |
| --- | --- |
| Source | Postgres 15 with logical replication |
| Capture | Debezium 2.5 on Kafka Connect |
| Transport | Apache Kafka (Strimzi 0.39 on GKE) |
| Process | Go 1.22 orchestrator |
| Serve | Postgres (read model) + Redis (idempotency) |
| Audit | Apache Iceberg on GCS |
| Observe | Prometheus + Grafana |

See `docs/architecture.md` for the full breakdown and `docs/slo.md` for
the SLOs.

## Quick start (local)

Prerequisites: Docker 24+ with Compose v2, Go 1.22+, Python 3.11+.

```bash
# 1. Bring up Postgres, Kafka, Debezium, Redis, Prometheus, Grafana.
make up

# 2. Apply migrations (creates tables + replication slot).
make migrate

# 3. Register the Debezium connector.
make connector

# 4. Drive synthetic CDC traffic at 800 events/sec.
make load

# 5. Open the dashboard at http://localhost:3000 (admin / admin).
```

When you are done:

```bash
make clean   # wipes volumes too
```

## Repository layout

```
cmd/orchestrator/     # service entrypoint
internal/             # private packages (handler, schema, idempotency, ...)
pkg/cdcevent/         # CDC event struct (importable)
schema/contracts/     # versioned schema contracts (YAML)
schema/postgres/      # SQL migrations
deploy/docker-compose # local dev stack
deploy/strimzi/       # production Kafka cluster CR
deploy/helm/          # Helm chart for the orchestrator
deploy/grafana/       # dashboards and alert rules (as code)
load-test/            # synthetic event generator and benchmark scripts
runbooks/             # incident response procedures
docs/                 # architecture and SLO documents
```

## Testing

```bash
make test           # go test -race ./...
make lint           # go vet + gofmt enforcement
make helm-lint      # helm lint of the chart
```

## CI / CD

Every push runs the GitHub Actions pipeline in `.github/workflows/ci.yaml`:
lint → unit + integration tests → schema-contract validation → image build.
Merges to `main` deploy to staging automatically; production deploys are a
canary 10 % → 50 % → 100 % rollout with manual approval.

## License

MIT — see `LICENSE`.
