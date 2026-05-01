-- 0003_app_role.sql
-- Least-privilege role used by the orchestrator service to write
-- website_inventory. The service does NOT need access to
-- warehouse_inventory; that table is read only by Debezium via the
-- replication slot.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app') THEN
        CREATE ROLE app WITH LOGIN PASSWORD 'app';
    END IF;
END $$;

GRANT CONNECT ON DATABASE warehouse TO app;
GRANT CONNECT ON DATABASE website   TO app;

GRANT USAGE ON SCHEMA public TO app;

-- The orchestrator writes only to website_inventory and reads
-- warehouse_inventory for the reconciliation worker.
GRANT SELECT, INSERT, UPDATE ON website_inventory TO app;
GRANT SELECT                  ON warehouse_inventory TO app;

-- The reservation table is written by the storefront checkout service,
-- but the orchestrator reads it during incident triage.
GRANT SELECT ON stock_reservations TO app;
