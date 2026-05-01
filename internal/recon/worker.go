// Package recon runs the periodic reconciliation worker described in §13
// of the design. It compares the warehouse source-of-truth against the
// website projection and records any disagreement on the
// inventory_mismatch_total gauge for alerting.
package recon

import (
	"context"
	"log/slog"
	"time"

	"github.com/WrathOfGenghis/inventory-cdc/internal/metrics"
	"github.com/WrathOfGenghis/inventory-cdc/internal/repository"
)

// MaxRowsPerRun caps a single reconciliation pass to avoid long-running
// reads against the warehouse DB on the rare occasion a large mismatch
// surfaces. A follow-up run will pick up the remainder.
const MaxRowsPerRun = 1000

// Run blocks until ctx is done, executing the reconciliation query at the
// configured interval. The first run happens immediately on startup so
// the mismatch gauge has a fresh value as soon as the pod is ready.
func Run(ctx context.Context, repo *repository.Repo, m *metrics.Registry, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Hour
	}

	tick := time.NewTicker(interval)
	defer tick.Stop()

	// Run once on startup, then on every tick.
	runOnce(ctx, repo, m)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			runOnce(ctx, repo, m)
		}
	}
}

func runOnce(ctx context.Context, repo *repository.Repo, m *metrics.Registry) {
	// Bound a single run with a generous timeout. Real production should
	// also rate-limit the query against the warehouse replica.
	runCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	start := time.Now()
	rows, err := repo.Reconcile(runCtx, MaxRowsPerRun)
	if err != nil {
		slog.Warn("reconciliation query failed",
			"err", err,
			"elapsed_ms", time.Since(start).Milliseconds())
		return
	}

	m.MismatchCount.Set(float64(len(rows)))

	if len(rows) == 0 {
		slog.Info("reconciliation clean",
			"elapsed_ms", time.Since(start).Milliseconds())
		return
	}

	slog.Warn("reconciliation found mismatches",
		"count", len(rows),
		"first_product_id", rows[0].ProductID,
		"first_warehouse_qty", rows[0].WarehouseQty,
		"first_website_qty", rows[0].WebsiteQty,
		"elapsed_ms", time.Since(start).Milliseconds())
}
