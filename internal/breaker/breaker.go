// Package breaker implements the small circuit breaker described in §6 of
// the design. It wraps any func() error and trips into the open state after
// a configurable number of consecutive failures, preventing the service
// from hammering an unhealthy Postgres or Redis.
package breaker

import (
	"errors"
	"sync"
	"time"
)

type state int

const (
	closed state = iota
	open
	halfOpen
)

// ErrOpen is returned by Run when the breaker is open and refusing calls.
var ErrOpen = errors.New("circuit breaker open")

// Breaker is safe for concurrent use.
type Breaker struct {
	mu        sync.Mutex
	state     state
	failures  int
	threshold int
	openedAt  time.Time
	cooldown  time.Duration
}

// New returns a Breaker that opens after `threshold` consecutive failures
// and stays open for `cooldown` before allowing a single trial request.
func New(threshold int, cooldown time.Duration) *Breaker {
	if threshold <= 0 {
		threshold = 5
	}
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}
	return &Breaker{threshold: threshold, cooldown: cooldown}
}

// Run executes fn under the breaker. While open, Run returns ErrOpen
// immediately. After the cooldown elapses, the breaker enters half-open
// and a single trial request decides whether to close it.
func (b *Breaker) Run(fn func() error) error {
	b.mu.Lock()
	if b.state == open {
		if time.Since(b.openedAt) < b.cooldown {
			b.mu.Unlock()
			return ErrOpen
		}
		b.state = halfOpen
	}
	b.mu.Unlock()

	err := fn()

	b.mu.Lock()
	defer b.mu.Unlock()
	if err != nil {
		b.failures++
		if b.failures >= b.threshold {
			b.state = open
			b.openedAt = time.Now()
		}
		return err
	}
	b.state = closed
	b.failures = 0
	return nil
}

// State returns a human-readable representation of the current state. It
// is intended for logging and metrics; behaviour is unaffected.
func (b *Breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case closed:
		return "closed"
	case open:
		return "open"
	case halfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}
