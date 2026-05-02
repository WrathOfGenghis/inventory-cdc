// Package metrics exposes the Prometheus collectors documented in §10 and
// §18 of the design. A single Registry value is wired into every
// component; the HTTP server exports /metrics, /healthz, and /readyz.
package metrics

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry bundles every collector the orchestrator publishes. A pointer
// receiver is used everywhere so a single instance can be shared safely.
type Registry struct {
	reg *prometheus.Registry

	SyncLatency    *prometheus.HistogramVec
	EventsTotal    *prometheus.CounterVec
	DLQTotal       *prometheus.CounterVec
	ConsumerLag    *prometheus.GaugeVec
	SchemaWarnings prometheus.Counter
	MismatchCount  prometheus.Gauge
	BreakerState   *prometheus.GaugeVec

	ready atomic.Bool
}

// New constructs a Registry with collectors registered against a private
// prometheus.Registry. Using a private registry avoids polluting the
// global default with collectors that belong to this service alone.
func New() *Registry {
	r := &Registry{
		reg: prometheus.NewRegistry(),
	}

	r.SyncLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sync_latency_seconds",
		Help:    "End-to-end sync latency from DB commit to website update.",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 3, 5, 10, 30},
	}, []string{})

	r.EventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "events_processed_total",
		Help: "CDC events by terminal outcome.",
	}, []string{"result"}) // applied|skipped|rejected|dlq|retry

	r.DLQTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "events_dlq_total",
		Help: "Events routed to DLQ by reason.",
	}, []string{"reason"})

	r.ConsumerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumergroup_lag",
		Help: "Approximate consumer-group lag per partition.",
	}, []string{"topic", "partition"})

	r.SchemaWarnings = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "schema_warnings_total",
		Help: "Conditional schema decisions observed.",
	})

	r.MismatchCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "inventory_mismatch_total",
		Help: "Reconciliation rows where warehouse and website disagree.",
	})

	r.BreakerState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "circuit_breaker_state",
		Help: "Circuit breaker state: 0=closed, 1=half_open, 2=open.",
	}, []string{"name"})

	r.reg.MustRegister(
		r.SyncLatency,
		r.EventsTotal,
		r.DLQTotal,
		r.ConsumerLag,
		r.SchemaWarnings,
		r.MismatchCount,
		r.BreakerState,
	)

	return r
}

// MarkReady flips the readiness flag. The readiness probe returns 503
// until this is called (which happens once the consumer has joined the
// group and at least one heartbeat has succeeded).
func (r *Registry) MarkReady() { r.ready.Store(true) }

// ObserveLatency is a convenience wrapper for the SyncLatency histogram.
func (r *Registry) ObserveLatency(d time.Duration) {
	r.SyncLatency.WithLabelValues().Observe(d.Seconds())
}

// Serve starts the metrics HTTP server. It blocks until ctx is done.
func (r *Registry) Serve(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !r.ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		return err
	}
}
