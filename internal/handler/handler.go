// Package handler implements the heart of the orchestrator: the nine
// ordered steps every CDC event walks through, as defined in §6 of the
// design. Failure at any step has a defined outcome — DLQ, retry, or skip
// — and the offset is never committed until side-effects are durable.
package handler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/WrathOfGenghis/inventory-cdc/internal/breaker"
	"github.com/WrathOfGenghis/inventory-cdc/internal/dlq"
	"github.com/WrathOfGenghis/inventory-cdc/internal/idempotency"
	"github.com/WrathOfGenghis/inventory-cdc/internal/metrics"
	"github.com/WrathOfGenghis/inventory-cdc/internal/repository"
	"github.com/WrathOfGenghis/inventory-cdc/internal/schema"
	"github.com/WrathOfGenghis/inventory-cdc/pkg/cdcevent"
)

// Deps bundles the collaborators a Handler needs. Fields are exported so
// main.go can construct a value cleanly; tests can build a Handler with
// only the collaborators they exercise.
type Deps struct {
	Repo    *repository.Repo
	Idem    *idempotency.Store
	Guard   *schema.Guard
	DLQ     *dlq.Producer
	Metrics *metrics.Registry
	Breaker *breaker.Breaker
}

// Handler is the concrete implementation of consumer.Handler.
type Handler struct {
	d Deps
}

// New constructs a Handler from the given dependencies.
func New(d Deps) *Handler { return &Handler{d: d} }

// ProcessMessage walks the nine ordered steps. The function never panics;
// every error path either routes to the DLQ or surfaces a transient
// failure for the consumer loop to retry by not committing the offset.
func (h *Handler) ProcessMessage(ctx context.Context, m kafka.Message) error {
	h.d.Metrics.EventsTotal.WithLabelValues("received").Inc()

	// Step 1: Decode.
	ev, err := cdcevent.Decode(m.Value)
	if err != nil {
		h.d.Metrics.DLQTotal.WithLabelValues(string(dlq.ReasonDecodeError)).Inc()
		return h.routeDLQ(ctx, m, "", dlq.ReasonDecodeError, err.Error())
	}
	ev.KafkaPartition = int32(m.Partition)
	ev.KafkaOffset = m.Offset

	// Step 2: Required-field validation.
	if err := ev.ValidateRequiredFields(); err != nil {
		h.d.Metrics.DLQTotal.WithLabelValues(string(dlq.ReasonMissingRequired)).Inc()
		return h.routeDLQ(ctx, m, ev.EventID, dlq.ReasonMissingRequired, err.Error())
	}

	// Step 3: Schema guard.
	verdict := h.d.Guard.Evaluate(ev)
	switch verdict.Decision {
	case schema.Breaking:
		h.d.Metrics.DLQTotal.WithLabelValues(string(dlq.ReasonSchemaBreaking)).Inc()
		return h.routeDLQ(ctx, m, ev.EventID, dlq.ReasonSchemaBreaking, verdict.Reason+":"+verdict.Field)
	case schema.Conditional:
		h.d.Metrics.SchemaWarnings.Inc()
		// Fall through and apply.
	}

	// Step 4: Idempotency check.
	seen, err := h.d.Idem.WasApplied(ctx, ev.EventID)
	if err != nil {
		// Transient — let the consumer redeliver.
		return err
	}
	if seen {
		h.d.Metrics.EventsTotal.WithLabelValues("skipped").Inc()
		slog.Debug("duplicate event skipped",
			"event_id", ev.EventID,
			"product_id", ev.ProductID)
		return nil
	}

	// Step 5–6: Apply the projection through the circuit breaker.
	var result repository.UpsertResult
	err = h.d.Breaker.Run(func() error {
		var apErr error
		result, apErr = h.d.Repo.Upsert(ctx, ev)
		return apErr
	})
	if err != nil {
		if errors.Is(err, breaker.ErrOpen) {
			// Surface so the consumer pauses; offset stays uncommitted.
			return err
		}
		h.d.Metrics.EventsTotal.WithLabelValues("retry").Inc()
		return err
	}

	// Stale rejection is not a failure — the version predicate filtered
	// the event out because a newer one already won.
	if !result.Applied {
		h.d.Metrics.EventsTotal.WithLabelValues("rejected").Inc()
		slog.Info("upsert ignored: stale version",
			"event_id", ev.EventID,
			"product_id", ev.ProductID,
			"incoming_version", ev.LatestRow().RowVersion)
		// Still commit the offset; the event has been considered.
		return nil
	}

	// Step 7: Mark idempotency.
	if err := h.d.Idem.MarkApplied(ctx, ev.EventID, idempotency.DefaultTTL); err != nil {
		// Failing to mark is safer to retry than to let the offset advance
		// — a redelivery will be caught by the version predicate anyway,
		// but we prefer the explicit guard.
		return err
	}

	// Step 8: Record latency from DB commit to now.
	if !ev.EventTime.IsZero() {
		h.d.Metrics.ObserveLatency(time.Since(ev.EventTime))
	}

	// Step 9: Success.
	h.d.Metrics.EventsTotal.WithLabelValues("applied").Inc()
	slog.Debug("event applied",
		"event_id", ev.EventID,
		"product_id", ev.ProductID,
		"row_version", result.RowVersion)
	return nil
}

func (h *Handler) routeDLQ(ctx context.Context, m kafka.Message, eventID string, reason dlq.Reason, detail string) error {
	if err := h.d.DLQ.Send(ctx, m, eventID, reason, detail); err != nil {
		// If we cannot even DLQ the message, surface the error so the
		// offset is not committed — better to reprocess than to drop.
		return err
	}
	h.d.Metrics.EventsTotal.WithLabelValues("dlq").Inc()
	slog.Warn("event routed to DLQ",
		"event_id", eventID,
		"reason", reason,
		"detail", detail)
	return nil
}
