# Runbook: Schema break detected

**Severity:** SEV-2 (mainline still flowing) or SEV-1 (mainline halted)
**Pages:** Primary on-call
**SLA:** 15 min ack, 2 h mitigation

## Symptom

`InventoryDLQSpike` is firing. The DLQ topic has more than 10 events / sec
for 5 minutes. Common reasons:

- Upstream DBA dropped or renamed a required column.
- A new column with `NOT NULL` and no default was added.
- Debezium emitted a snapshot event in an unexpected shape.

## First check: is mainline still flowing?

Open the Sync Latency dashboard. If `events_processed_total{result="applied"}`
is still incrementing, the bad events are isolated and the customer impact
is contained — proceed at SEV-2.

If `applied` has dropped to zero, the breakage is poisoning the mainline
and you must follow `runbooks/pipeline-halted.md` first.

## Investigate the DLQ

```
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic cdc.inventory.dlq \
  --max-messages 5 | jq
```

Each envelope contains a `reason`, a `reason_detail`, and the
`raw_payload` of the original message. Copy a representative payload.

## Decide on the response

| Reason | Action |
| --- | --- |
| `decode_error` | Producer is misconfigured. Roll back the upstream change. |
| `missing_required` | Required field is missing in the payload. Either restore the column upstream or update the schema contract. |
| `schema_breaking` | A required field has changed type or been renamed. New contract version needed. |
| `no_mapping` | Conditional change but no rename mapping exists. Add one. |

## Deploy a new contract version

1. Copy `schema/contracts/inventory.v3.yaml` to `inventory.v4.yaml`.
2. Bump the `version:` field and apply the change (new required field, type change, mapping, etc).
3. Update the orchestrator deployment to point at the new file:
   ```
   helm upgrade inventory-cdc deploy/helm/inventory-cdc \
     -f deploy/helm/inventory-cdc/values-prod.yaml \
     --set env.SCHEMA_CONTRACT=/etc/contracts/inventory.v4.yaml
   ```
4. Wait for rollout: `kubectl rollout status deploy/inventory-cdc -n inventory-prod`.

## Replay the DLQ

Once the new contract is live and the source of the bad events is fixed:

```
python tools/dlq_replay.py \
  --source-topic cdc.inventory.dlq \
  --target-topic cdc.inventory.warehouse \
  --since "1 hour ago"
```

The replay job re-publishes DLQ payloads back to the main topic.
Idempotency markers in Redis make this safe — already-applied events
become no-ops.

## Verify

1. DLQ rate falls to zero.
2. `inventory_mismatch_total` returns to zero within one hour.
3. File a post-mortem and update the schema-evolution test corpus
   in `load-test/scenarios/` with the failure pattern observed.
