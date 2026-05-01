# Runbook: Reconciliation mismatch / DLQ replay

**Severity:** SEV-2
**Pages:** Primary on-call
**SLA:** 30 min ack, 4 h resolution

## When to use this runbook

The `InventoryReconciliationMismatchHigh` alert is firing, or you are
performing a planned DLQ replay after `runbooks/schema-break.md`.

## Step 1: Confirm the scope

Query the audit view directly to see how many SKUs disagree:

```sql
SELECT count(*), reason
  FROM v_inventory_mismatch
 GROUP BY reason
 ORDER BY count DESC;
```

If the reason is overwhelmingly `missing_in_website`, the projection has
genuinely missed events — proceed to step 2.

If it is `value_mismatch` for a small number of rows, an out-of-band
write may have happened against `website_inventory`. Investigate the
audit log before replaying.

## Step 2: Identify affected event range

The `last_db_commit_ts` column on `website_inventory` tells you how far
behind the website is for each row. The minimum value across all
mismatched rows is the start of the replay window:

```sql
SELECT min(last_db_commit_ts), max(last_db_commit_ts)
  FROM website_inventory s
  JOIN v_inventory_mismatch v
    ON v.product_id = s.product_id
   AND v.warehouse_id = s.warehouse_id;
```

## Step 3: Run the targeted replay

A targeted replay reads the Iceberg audit table (full history) for the
affected window and republishes events to the main Kafka topic. The
orchestrator re-applies them; idempotency markers ensure no
double-counting.

```bash
python tools/iceberg_replay.py \
  --table lakehouse.inventory_cdc_events \
  --since "2026-04-30T08:00:00Z" \
  --until "2026-04-30T11:00:00Z" \
  --target-topic cdc.inventory.warehouse \
  --dry-run
```

Drop `--dry-run` once you have eyeballed the row count.

## Step 4: Verify

After replay, the reconciliation worker runs once an hour. Wait for the
next tick and confirm `inventory_mismatch_total` returns to zero. If it
does not, escalate to platform team — the warehouse may have a write
that bypasses the Postgres logical replication slot.

## Step 5: Post-mortem hygiene

- Record the mismatch count, the replay window, and the recovery time
  in the incident tracker.
- Add a regression test to `load-test/scenarios/` if the failure pattern
  is reproducible.
- File a follow-up to plug the gap that allowed the mismatch to grow
  large enough to alert.
