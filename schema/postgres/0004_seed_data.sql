-- 0004_seed_data.sql
-- Tiny seed used by the local Docker Compose stack so a developer can
-- exercise the full pipeline immediately after `make up`. The load
-- generator (load-test/load_gen.py) drives the realistic workload; this
-- seed exists only so the website storefront has rows to display before
-- any synthetic events arrive.

INSERT INTO warehouse_inventory
    (product_id, warehouse_id, available_qty, reserved_qty, stock_status, row_version)
VALUES
    ('SKU-AC-9482', 'WH-MUM-01',  50, 0, 'ACTIVE', 116),
    ('SKU-AC-9482', 'WH-DEL-02',  35, 2, 'ACTIVE',  88),
    ('SKU-BG-3310', 'WH-MUM-01', 120, 5, 'ACTIVE', 412),
    ('SKU-BG-3310', 'WH-BLR-03', 200, 0, 'ACTIVE', 309),
    ('SKU-CD-7001', 'WH-MUM-01',   8, 0, 'LOW',    901),
    ('SKU-DE-4422', 'WH-MUM-01',   0, 0, 'OOS',    150)
ON CONFLICT (product_id, warehouse_id) DO NOTHING;

-- Mirror the seed into website_inventory so the storefront has something
-- to read before the first CDC event flows through. In production this
-- table is exclusively maintained by the orchestrator.
INSERT INTO website_inventory
    (product_id, warehouse_id, available_qty, reserved_qty, stock_status,
     row_version, last_event_id, last_db_commit_ts)
SELECT
    product_id, warehouse_id, available_qty, reserved_qty, stock_status,
    row_version, 'seed-' || product_id || '-' || warehouse_id, NOW()
FROM warehouse_inventory
ON CONFLICT (product_id, warehouse_id) DO NOTHING;
