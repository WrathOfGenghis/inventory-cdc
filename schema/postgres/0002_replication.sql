-- 0002_replication.sql
-- Logical replication setup consumed by Debezium.
--
-- Run this once per Postgres primary. The publication scopes Debezium's
-- access to only the warehouse_inventory table; widening this requires
-- a deliberate change here and in deploy/debezium/connector.json.
--
-- Note: pg_create_logical_replication_slot returns an error if the slot
-- already exists, so this is wrapped in a DO block that swallows the
-- duplicate-object error.

DO $$
BEGIN
    -- Create the publication if missing.
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'inventory_pub'
    ) THEN
        CREATE PUBLICATION inventory_pub FOR TABLE warehouse_inventory;
    END IF;

    -- Create the replication slot if missing.
    IF NOT EXISTS (
        SELECT 1 FROM pg_replication_slots WHERE slot_name = 'inventory_slot'
    ) THEN
        PERFORM pg_create_logical_replication_slot('inventory_slot', 'pgoutput');
    END IF;
END $$;

-- Debezium needs REPLICATION privilege on the user it connects with.
-- The role and password come from the deploy environment; this DDL is
-- idempotent and safe to run repeatedly.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium_reader') THEN
        CREATE ROLE debezium_reader WITH LOGIN REPLICATION
            PASSWORD 'debezium';
    END IF;
END $$;

GRANT CONNECT ON DATABASE warehouse TO debezium_reader;
GRANT USAGE  ON SCHEMA public        TO debezium_reader;
GRANT SELECT ON warehouse_inventory  TO debezium_reader;
