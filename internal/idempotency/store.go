// Package idempotency implements the Redis-backed deduplication store
// described in §6 and §7 of the design. The store guarantees that an event
// applied successfully will be skipped on any subsequent redelivery.
package idempotency

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultTTL is the retention window for idempotency markers. It must be
// longer than the worst plausible redelivery delay (Kafka retention,
// consumer downtime, manual replay) so that legitimate duplicates are
// always caught. 24 hours covers every recovery scenario described in §13.
const DefaultTTL = 24 * time.Hour

// Store is a thin wrapper around go-redis that exposes only the two
// operations the orchestrator needs.
type Store struct {
	rdb    *redis.Client
	script *redis.Script
}

// Dial connects to a Redis instance reachable at addr and returns a ready
// Store. It pings the server to fail fast on misconfiguration.
func Dial(ctx context.Context, addr string) (*Store, error) {
	if addr == "" {
		return nil, errors.New("empty redis addr")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &Store{
		rdb:    rdb,
		script: redis.NewScript(setIfAbsentLua),
	}, nil
}

// Close releases the underlying Redis client.
func (s *Store) Close() error {
	if s == nil || s.rdb == nil {
		return nil
	}
	return s.rdb.Close()
}

// WasApplied reports whether the given event_id has already been processed.
// A true return means the event should be skipped; false means proceed.
func (s *Store) WasApplied(ctx context.Context, eventID string) (bool, error) {
	n, err := s.rdb.Exists(ctx, key(eventID)).Result()
	if err != nil {
		return false, fmt.Errorf("redis exists: %w", err)
	}
	return n == 1, nil
}

// MarkApplied records the event_id as applied with the configured TTL.
// The operation is atomic and idempotent: a double call is harmless because
// the underlying Lua script checks EXISTS before SET.
func (s *Store) MarkApplied(ctx context.Context, eventID string, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	_, err := s.script.Run(ctx, s.rdb,
		[]string{key(eventID)},
		"1",
		int(ttl.Seconds()),
	).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("redis script: %w", err)
	}
	return nil
}
