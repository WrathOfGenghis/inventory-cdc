-- 0006_constraints.sql
-- Integrity constraints that protect the website projection from
-- obviously-wrong data even if the orchestrator has a bug. These are
-- belt-and-braces checks; the application layer should already prevent
-- them, but the database remains the last line of defence.

ALTER TABLE website_inventory
    ADD CONSTRAINT chk_available_qty_nonneg
    CHECK (available_qty >= 0) NOT VALID;

ALTER TABLE website_inventory
    ADD CONSTRAINT chk_reserved_qty_nonneg
    CHECK (reserved_qty >= 0) NOT VALID;

ALTER TABLE website_inventory
    ADD CONSTRAINT chk_row_version_positive
    CHECK (row_version > 0) NOT VALID;

ALTER TABLE website_inventory
    ADD CONSTRAINT chk_stock_status_known
    CHECK (stock_status IN ('ACTIVE','LOW','OOS','HIDDEN','DELETED')) NOT VALID;

ALTER TABLE warehouse_inventory
    ADD CONSTRAINT chk_warehouse_available_qty_nonneg
    CHECK (available_qty >= 0) NOT VALID;

-- Validate constraints in a separate step so adding them is a fast,
-- non-blocking metadata change. Validation can run during off-peak.
ALTER TABLE website_inventory   VALIDATE CONSTRAINT chk_available_qty_nonneg;
ALTER TABLE website_inventory   VALIDATE CONSTRAINT chk_reserved_qty_nonneg;
ALTER TABLE website_inventory   VALIDATE CONSTRAINT chk_row_version_positive;
ALTER TABLE website_inventory   VALIDATE CONSTRAINT chk_stock_status_known;
ALTER TABLE warehouse_inventory VALIDATE CONSTRAINT chk_warehouse_available_qty_nonneg;
