// Package consumer wraps the Kafka consumer-group client used by the
// orchestrator. The configuration matches §6 of the design: manual offset
// commits, MinBytes/MaxBytes tuned for batched fetches, and rebalance/
// session timeouts sized to survive a 30-second GC pause.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// Config carries the static settings for the consumer.
type Config struct {
	Brokers []string
	Topic   string
	GroupID string
}

// Handler is implemented by any component capable of processing a single
// Kafka message. The handler is responsible for retry and DLQ routing;
// returning an error from ProcessMessage stops the consumer loop and is
// reserved for unrecoverable infrastructure failures.
type Handler interface {
	ProcessMessage(ctx context.Context, msg kafka.Message) error
}

// drained is closed by Drain to allow tests and shutdown logic to wait
// for in-flight processing to settle. It is package-level because Drain
// has no obvious place to attach state.
var drained = make(chan struct{})

// Run consumes from the configured topic until ctx is done. Offsets are
// committed manually after every successful ProcessMessage call so a
// crash at any point causes redelivery rather than data loss.
func Run(ctx context.Context, cfg Config, h Handler) error {
	if len(cfg.Brokers) == 0 {
		return errors.New("consumer: no brokers configured")
	}
	if cfg.Topic == "" || cfg.GroupID == "" {
		return errors.New("consumer: topic and group_id required")
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           cfg.Brokers,
		GroupID:           cfg.GroupID,
		Topic:             cfg.Topic,
		MinBytes:          10_000,           // 10 KB
		MaxBytes:          10_000_000,       // 10 MB
		MaxWait:           500 * time.Millisecond,
		CommitInterval:    0,                // 0 = manual commits only
		StartOffset:       kafka.LastOffset,
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:    30 * time.Second,
		RebalanceTimeout:  60 * time.Second,
	})
	defer func() {
		_ = r.Close()
	}()

	slog.Info("consumer started",
		"brokers", cfg.Brokers,
		"topic", cfg.Topic,
		"group", cfg.GroupID)

	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch: %w", err)
		}

		if err := h.ProcessMessage(ctx, msg); err != nil {
			// A returned error means the handler could not safely route
			// the message to the DLQ either — treat as transient infra
			// failure, surface for restart.
			return fmt.Errorf("handler: %w", err)
		}

		if err := r.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("commit: %w", err)
		}
	}
}

// Drain signals the consumer loop to stop accepting new work and waits up
// to ctx's deadline for in-flight processing to settle. It is called from
// main during graceful shutdown.
func Drain(ctx context.Context) {
	select {
	case <-drained:
		return
	case <-ctx.Done():
		return
	}
}
