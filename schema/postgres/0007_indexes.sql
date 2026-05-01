-- 0007_indexes.sql
-- Indexes for the hot read paths.
--
-- Storefront API queries website_inventory by product_id (with optional
-- warehouse filter), so the existing primary key already covers
-- (product_id, warehouse_id). The additional indexes below support
-- recency filtering in dashboards and stock-status filtering in
-- merchandising tools.

CREATE INDEX IF NOT EXISTS idx_website_inventory_status
    ON website_inventory (stock_status);

CREATE INDEX IF NOT EXISTS idx_website_inventory_updated
    ON website_inventory (updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_website_inventory_last_commit
    ON website_inventory (last_db_commit_ts DESC);

-- Reservations are looked up by order_id during refunds and by
-- (product_id, warehouse_id) during checkout retries.
CREATE INDEX IF NOT EXISTS idx_reservations_order
    ON stock_reservations (order_id);

CREATE INDEX IF NOT EXISTS idx_reservations_sku_warehouse
    ON stock_reservations (product_id, warehouse_id, created_at DESC);

-- Warehouse table is mainly read by Debezium via the WAL, but the
-- reconciliation join also reads it. Keeping the index narrow.
CREATE INDEX IF NOT EXISTS idx_warehouse_inventory_updated
    ON warehouse_inventory (last_updated DESC);
