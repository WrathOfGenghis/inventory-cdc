# Architecture

This document is the engineering-facing summary of the design report
(`E-Commerce Inventory Live Sync — CDC` v2). It exists so that a new
engineer can land on the repository and understand the system without
opening the 87-page PDF.

## One-line goal

Stop overselling by syncing warehouse inventory changes to the website
in near real time, with measurable latency, replayability, and schema
safety.

## Tech stack

| Layer | Tool | Why |
| --- | --- | --- |
| Source | Postgres | Authoritative warehouse store with logical replication |
| Capture | Debezium (Kafka Connect) | Mature log-based CDC, JSON / Avro support |
| Transport | Kafka (Strimzi on GKE) | Durable log, per-partition ordering, replay |
| Process | Go orchestrator | Tiny image, fast startup, first-class concurrency |
| Serve | Postgres (read model) + Redis (idempotency) | Fast lookups for the storefront |
| Audit | Apache Iceberg on GCS | Schema-evolution friendly, engine-agnostic |
| Observe | Prometheus + Grafana | Industry standard, scrape-based |

## Data flow

```
warehouse_inventory  ──WAL──▶  Debezium  ──▶  Kafka topic  ──▶  Go orchestrator
                                                                 │
              ┌──────────────────────────────────────────────────┤
              ▼                                                  ▼
        website_inventory                                  Iceberg audit
        (read model used                                  (long-term history)
         by storefront API)
```

## Correctness invariants

1. **Per-product ordering** — events are partitioned by `product_id`; a
   single partition is processed strictly in offset order.
2. **Version-checked upsert** — the SQL `WHERE row_version <
   EXCLUDED.row_version` clause silently rejects stale events, even if
   delivery is out of order.
3. **Idempotency** — every applied event ID is recorded in Redis with a
   24-hour TTL. Redelivery is detected and skipped.
4. **Delayed offset commit** — the Kafka offset is committed only after
   the Postgres upsert and Redis mark have both succeeded.
5. **Schema contract** — every event is evaluated against a versioned
   contract; breaking changes go to the DLQ, never silently dropped.

The combination of (1)+(2)+(3) gives exactly-once *effects* on top of
at-least-once delivery, with no distributed transaction.

## Failure handling

| Failure | Behaviour |
| --- | --- |
| Orchestrator pod crash | Offset un-committed → Kafka redelivers → idempotency skip |
| Postgres outage | Circuit breaker opens; consumer pauses; resume on heal |
| Redis outage | WAS_APPLIED returns false; version predicate is the safety net |
| Kafka broker outage | `min.insync.replicas=2` keeps the topic available |
| Schema break | Event routed to DLQ; mainline keeps flowing; alert fires |

## Observability

The single most important metric is `sync_latency_seconds` — a histogram
of the time from `db_commit_timestamp` to `website_update_timestamp`.
The SLO is p95 ≤ 3 s, p99 ≤ 5 s. See `docs/slo.md` for the full set.

## Where to look in the code

- Entrypoint: `cmd/orchestrator/main.go`
- The 9-step ProcessMessage flow: `internal/handler/handler.go`
- Schema contract evaluation: `internal/schema/guard.go`
- Idempotency Lua script: `internal/idempotency/store_lua.go`
- Version-checked upsert SQL: `internal/repository/inventory.go`
- Reconciliation worker: `internal/recon/worker.go`
- DLQ envelope and producer: `internal/dlq/producer.go`
