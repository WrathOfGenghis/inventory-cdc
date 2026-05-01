# Runbook: Submission checklist

This runbook captures the exact sequence used to produce the evidence in
Appendix C of the design report. Anyone with the repository should be
able to reproduce every screenshot and log line by following the steps
below.

## Prerequisites

- Docker 24+ with Compose v2
- Go 1.22+
- Python 3.11+ with `psycopg2-binary` installed
- `make`, `curl`, `jq`

## Step 1: Bring up the local stack

```bash
docker compose -f deploy/docker-compose.yaml up -d
docker compose -f deploy/docker-compose.yaml ps
```

Capture: terminal screenshot for **C.1 Local stack — Docker containers running**.

## Step 2: Apply migrations

```bash
make migrate
psql postgresql://app:app@localhost:5432/warehouse \
  -c "SELECT * FROM warehouse_inventory WHERE product_id='SKU-AC-9482';"
```

Capture: psql output for **C.2 Postgres warehouse table — before update**.

## Step 3: Register the Debezium connector

```bash
curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @deploy/debezium/connector.json
curl -s http://localhost:8083/connectors/warehouse-pg-connector/status | jq
```

## Step 4: Trigger one update and capture the event

```bash
psql postgresql://app:app@localhost:5432/warehouse <<SQL
UPDATE warehouse_inventory
   SET available_qty = available_qty - 12,
       row_version = row_version + 1
 WHERE product_id = 'SKU-AC-9482' AND warehouse_id = 'WH-MUM-01';
SQL

docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic cdc.inventory.warehouse \
  --from-beginning --max-messages 1 | jq
```

Capture: **C.3 CDC event JSON**, **C.4 Kafka topic message in flight**.

## Step 5: Capture orchestrator logs and verify the upsert

```bash
docker compose logs --tail=20 orchestrator | grep SKU-AC-9482
psql postgresql://app:app@localhost:5432/website \
  -c "SELECT * FROM website_inventory WHERE product_id='SKU-AC-9482';"
```

Capture: **C.5 Go orchestrator logs**, **C.6 Postgres website table after**.

## Step 6: Test idempotency and DLQ

```bash
docker compose exec redis redis-cli KEYS 'cdc:applied:*'
python tools/inject_breaking.py --type column_renamed
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic cdc.inventory.dlq \
  --from-beginning --max-messages 1 | jq
```

Capture: **C.7 Redis idempotency**, **C.11 DLQ event**.

## Step 7: Capture metrics and dashboard

```bash
curl -s http://localhost:8080/metrics | head -40
```

Open `http://localhost:3000` (admin/admin) and screenshot the Sync
Latency dashboard during a sustained load run.

Capture: **C.8 Prometheus /metrics**, **C.9 Grafana dashboard**, **C.10 Consumer-group lag**.

## Step 8: Tear down

```bash
docker compose -f deploy/docker-compose.yaml down -v
```
