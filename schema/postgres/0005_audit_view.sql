-- 0005_audit_view.sql
-- A convenience view used by the hourly reconciliation worker and by
-- on-call engineers during incident triage. The view performs no
-- locking and is safe to query against the read replica.

CREATE OR REPLACE VIEW v_inventory_mismatch AS
SELECT
    w.product_id,
    w.warehouse_id,
    w.available_qty AS warehouse_qty,
    s.available_qty AS website_qty,
    w.row_version   AS warehouse_version,
    s.row_version   AS website_version,
    w.last_updated  AS warehouse_updated_at,
    s.updated_at    AS website_updated_at,
    CASE
        WHEN s.product_id IS NULL THEN 'missing_in_website'
        WHEN w.row_version > s.row_version THEN 'website_behind'
        WHEN w.row_version < s.row_version THEN 'website_ahead'
        ELSE 'value_mismatch'
    END AS reason
FROM warehouse_inventory w
LEFT JOIN website_inventory s
       ON w.product_id   = s.product_id
      AND w.warehouse_id = s.warehouse_id
WHERE s.product_id IS NULL
   OR w.available_qty <> s.available_qty
   OR w.row_version   <> s.row_version;

COMMENT ON VIEW v_inventory_mismatch IS
    'Rows where warehouse and website projections disagree; '
    'consumed by recon.Worker on an hourly schedule.';
