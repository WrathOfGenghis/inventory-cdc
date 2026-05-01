// Package main is the entrypoint for the inventory CDC orchestrator service.
//
// The orchestrator consumes change-data-capture events from Kafka, validates
// them against a versioned schema contract, deduplicates via Redis, and
// applies a version-checked upsert into the website inventory store.
//
// See docs/architecture.md for the full design.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/WrathOfGenghis/inventory-cdc/internal/breaker"
	"github.com/WrathOfGenghis/inventory-cdc/internal/config"
	"github.com/WrathOfGenghis/inventory-cdc/internal/consumer"
	"github.com/WrathOfGenghis/inventory-cdc/internal/dlq"
	"github.com/WrathOfGenghis/inventory-cdc/internal/handler"
	"github.com/WrathOfGenghis/inventory-cdc/internal/idempotency"
	"github.com/WrathOfGenghis/inventory-cdc/internal/metrics"
	"github.com/WrathOfGenghis/inventory-cdc/internal/recon"
	"github.com/WrathOfGenghis/inventory-cdc/internal/repository"
	"github.com/WrathOfGenghis/inventory-cdc/internal/schema"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config load failed", "err", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	}))
	slog.SetDefault(logger)

	rootCtx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Wire up dependencies.
	prom := metrics.New()

	idem, err := idempotency.Dial(rootCtx, cfg.RedisAddr)
	if err != nil {
		logger.Error("redis dial failed", "err", err)
		os.Exit(1)
	}
	defer idem.Close()

	repo, err := repository.Open(rootCtx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("postgres open failed", "err", err)
		os.Exit(1)
	}
	defer repo.Close()

	guard, err := schema.LoadGuard(cfg.SchemaContractPath)
	if err != nil {
		logger.Error("schema contract load failed", "err", err)
		os.Exit(1)
	}

	dlqProducer := dlq.NewProducer(cfg.KafkaBrokers, cfg.DLQTopic)
	defer dlqProducer.Close()

	cb := breaker.New(5, 30*time.Second)

	h := handler.New(handler.Deps{
		Repo:    repo,
		Idem:    idem,
		Guard:   guard,
		DLQ:     dlqProducer,
		Metrics: prom,
		Breaker: cb,
	})

	g, ctx := errgroup.WithContext(rootCtx)

	// Kafka consumer loop.
	g.Go(func() error {
		logger.Info("starting consumer", "topic", cfg.KafkaTopic, "group", cfg.KafkaGroup)
		return consumer.Run(ctx, consumer.Config{
			Brokers: cfg.KafkaBrokers,
			Topic:   cfg.KafkaTopic,
			GroupID: cfg.KafkaGroup,
		}, h)
	})

	// Hourly reconciliation worker.
	g.Go(func() error {
		return recon.Run(ctx, repo, prom, time.Hour)
	})

	// HTTP server: /metrics, /healthz, /readyz.
	g.Go(func() error {
		return prom.Serve(ctx, cfg.MetricsAddr)
	})

	if err := g.Wait(); err != nil && rootCtx.Err() == nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}

	// Drain in-flight events with a 30s deadline.
	drainCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	consumer.Drain(drainCtx)

	logger.Info("orchestrator stopped cleanly")
}
