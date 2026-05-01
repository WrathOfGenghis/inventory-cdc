// Package repository implements the website_inventory projection. The
// upsert logic uses a version predicate to silently reject stale events,
// which is the SQL-level half of the exactly-once-effects guarantee
// described in §6 and §7 of the design.
package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/WrathOfGenghis/inventory-cdc/pkg/cdcevent"
)

// upsertSQL is the heart of the projection. The WHERE clause on the UPDATE
// branch ensures an older row_version cannot overwrite a newer one. The
// RETURNING clause exposes whether the row was actually changed so the
// caller can record an "applied" or "stale_rejected" metric without an
// extra query.
const upsertSQL = `
INSERT INTO website_inventory (
    product_id, warehouse_id, available_qty, reserved_qty,
    stock_status, row_version, last_event_id, last_db_commit_ts, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
ON CONFLICT (product_id, warehouse_id) DO UPDATE SET
    available_qty     = EXCLUDED.available_qty,
    reserved_qty      = EXCLUDED.reserved_qty,
    stock_status      = EXCLUDED.stock_status,
    row_version       = EXCLUDED.row_version,
    last_event_id     = EXCLUDED.last_event_id,
    last_db_commit_ts = EXCLUDED.last_db_commit_ts,
    updated_at        = NOW()
WHERE website_inventory.row_version < EXCLUDED.row_version
RETURNING row_version
`

// reconcileSQL returns rows where the warehouse and website projections
// disagree. Used by the hourly reconciliation worker.
const reconcileSQL = `
SELECT w.product_id, w.warehouse_id,
       w.available_qty AS warehouse_qty,
       s.available_qty AS website_qty,
       w.row_version   AS warehouse_version,
       s.row_version   AS website_version
FROM warehouse_inventory w
LEFT JOIN website_inventory s
       ON w.product_id   = s.product_id
      AND w.warehouse_id = s.warehouse_id
WHERE s.product_id IS NULL
   OR w.available_qty <> s.available_qty
   OR w.row_version   <> s.row_version
LIMIT $1
`

// Repo wraps a pgx connection pool and exposes the small set of operations
// the orchestrator needs.
type Repo struct {
	pool *pgxpool.Pool
}

// Open creates a connection pool against the supplied DSN and pings to
// verify connectivity. The pool is sized for the consumer's expected
// concurrency.
func Open(ctx context.Context, dsn string) (*Repo, error) {
	if dsn == "" {
		return nil, errors.New("empty postgres dsn")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = 16
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.HealthCheckPeriod = time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pool create: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres ping: %w", err)
	}

	return &Repo{pool: pool}, nil
}

// Close drains the connection pool.
func (r *Repo) Close() {
	if r != nil && r.pool != nil {
		r.pool.Close()
	}
}

// Mismatch is one row from the reconciliation query.
type Mismatch struct {
	ProductID        string
	WarehouseID      string
	WarehouseQty     int
	WebsiteQty       int
	WarehouseVersion int64
	WebsiteVersion   int64
}

// UpsertResult reports whether an upsert actually changed a row and what
// the resulting row_version is. A non-applied result is the expected
// outcome when an older event arrives after a newer one.
type UpsertResult struct {
	Applied    bool
	RowVersion int64
}

// Upsert applies a single CDC event to website_inventory. It returns
// Applied=false (without error) when the version predicate filters the
// event out as stale.
func (r *Repo) Upsert(ctx context.Context, e *cdcevent.Event) (UpsertResult, error) {
	row := e.LatestRow()
	if row == nil {
		return UpsertResult{}, errors.New("event has no row to project")
	}

	stockStatus := row.StockStatus
	if stockStatus == "" {
		stockStatus = "ACTIVE"
	}

	commitTS := e.EventTime
	if commitTS.IsZero() {
		commitTS = time.Now().UTC()
	}

	var version int64
	err := r.pool.QueryRow(ctx, upsertSQL,
		e.ProductID,
		e.WarehouseID,
		row.AvailableQty,
		row.ReservedQty,
		stockStatus,
		row.RowVersion,
		e.EventID,
		commitTS,
	).Scan(&version)

	if err != nil {
		// pgx returns ErrNoRows when the WHERE predicate filters the UPDATE
		// branch — that is a stale event, not a failure.
		if errors.Is(err, pgx.ErrNoRows) {
			return UpsertResult{Applied: false}, nil
		}
		return UpsertResult{}, fmt.Errorf("upsert: %w", err)
	}
	return UpsertResult{Applied: true, RowVersion: version}, nil
}

// Reconcile returns up to limit rows where warehouse and website disagree.
// The hourly worker uses this to feed an inventory_mismatch_total gauge.
func (r *Repo) Reconcile(ctx context.Context, limit int) ([]Mismatch, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := r.pool.Query(ctx, reconcileSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("reconcile query: %w", err)
	}
	defer rows.Close()

	var out []Mismatch
	for rows.Next() {
		var m Mismatch
		if err := rows.Scan(
			&m.ProductID, &m.WarehouseID,
			&m.WarehouseQty, &m.WebsiteQty,
			&m.WarehouseVersion, &m.WebsiteVersion,
		); err != nil {
			return nil, fmt.Errorf("reconcile scan: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
