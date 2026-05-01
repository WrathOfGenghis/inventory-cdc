# Runbook: Pipeline halted

**Severity:** SEV-1
**Pages:** Primary on-call → secondary → manager
**SLA:** 5 min ack, 30 min mitigation

## Symptom

One or both of the following alerts is firing:

- `InventoryEventsThroughputZero` — no events applied for 5 minutes.
- `InventorySyncLatencySLOBurnFast` — p95 latency above 3 s and rising.

## First five minutes

1. Open the Sync Latency dashboard and check the **Consumer lag by partition** panel.
   - If lag is rising on every partition: the consumers are stuck. Go to step 2.
   - If lag is flat across all partitions but throughput is zero: Debezium is not publishing. Go to step 5.

2. Check orchestrator pod status:
   ```
   kubectl -n inventory-prod get pods -l app.kubernetes.io/name=inventory-cdc
   kubectl -n inventory-prod logs -l app.kubernetes.io/name=inventory-cdc --tail=200
   ```

3. Look for one of these patterns in the logs:
   - `circuit breaker opened` → Postgres is unhealthy. Page the DB on-call. The orchestrator will resume on its own once Postgres recovers and the cooldown elapses.
   - `redis: connection refused` → Memorystore is unhealthy. Failover to the secondary instance.
   - `kafka: rebalance in progress` for more than 60 seconds → broker may be down. Check the Kafka cluster health.

4. If pods are CrashLoopBackOff: read the most recent crash log carefully and follow the runbook for the underlying error.

5. Check Debezium connector status:
   ```
   curl -s http://debezium:8083/connectors/warehouse-pg-connector/status | jq
   ```
   - If `state` is FAILED: restart the task with
     `curl -X POST http://debezium:8083/connectors/warehouse-pg-connector/restart`.
   - If the replication slot is missing: see `runbooks/schema-break.md` for slot recreation.

## Mitigation paths

| Root cause | Mitigation |
| --- | --- |
| Bad deploy | `helm rollback inventory-cdc` to the previous release. |
| Postgres outage | Wait for failover. Confirm orchestrator resumes once writes succeed. |
| Kafka broker down | Verify ISR. Force leader election only with platform team approval. |
| Schema break | Follow `runbooks/schema-break.md`. |

## After mitigation

1. Confirm `events_processed_total{result="applied"}` is incrementing again.
2. Wait 15 minutes and verify the **Active SKUs with mismatch** stat panel is back to zero.
3. File a SEV-1 retro within 5 business days. Use the template in `docs/architecture.md` § post-mortem.
