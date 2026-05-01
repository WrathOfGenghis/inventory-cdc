-- 0001_init.sql
-- Core inventory tables for the CDC pipeline.
--
-- warehouse_inventory  : the source of truth, written by the warehouse
--                        management system. CDC reads from this via the
--                        Postgres logical replication slot.
-- website_inventory    : the projection consumed by the storefront API.
--                        Maintained by the orchestrator service.
-- stock_reservations   : checkout-time reservations described in §7 of
--                        the design report.

BEGIN;

CREATE TABLE IF NOT EXISTS warehouse_inventory (
    inventory_id      BIGSERIAL PRIMARY KEY,
    product_id        VARCHAR(100) NOT NULL,
    warehouse_id      VARCHAR(50)  NOT NULL,
    available_qty     INT          NOT NULL,
    reserved_qty      INT          NOT NULL DEFAULT 0,
    stock_status      VARCHAR(30)  NOT NULL DEFAULT 'ACTIVE',
    row_version       BIGINT       NOT NULL DEFAULT 1,
    last_updated      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (product_id, warehouse_id)
);

CREATE TABLE IF NOT EXISTS website_inventory (
    product_id          VARCHAR(100) NOT NULL,
    warehouse_id        VARCHAR(50)  NOT NULL,
    available_qty       INT          NOT NULL,
    reserved_qty        INT          NOT NULL DEFAULT 0,
    stock_status        VARCHAR(30)  NOT NULL DEFAULT 'ACTIVE',
    row_version         BIGINT       NOT NULL,
    last_event_id       VARCHAR(150) NOT NULL,
    last_db_commit_ts   TIMESTAMPTZ  NOT NULL,
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_id, warehouse_id)
);

CREATE TABLE IF NOT EXISTS stock_reservations (
    reservation_id      BIGSERIAL PRIMARY KEY,
    order_id            VARCHAR(100) NOT NULL,
    product_id          VARCHAR(100) NOT NULL,
    warehouse_id        VARCHAR(50)  NOT NULL,
    quantity            INT          NOT NULL,
    reservation_status  VARCHAR(30)  NOT NULL,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMIT;
