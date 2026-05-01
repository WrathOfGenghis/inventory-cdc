// Package cdcevent defines the wire format for CDC events flowing through
// the inventory pipeline. The contract is documented in §5 of the design.
package cdcevent

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Op enumerates the SQL operations Debezium emits.
type Op string

const (
	OpInsert   Op = "c"
	OpUpdate   Op = "u"
	OpDelete   Op = "d"
	OpSnapshot Op = "r"
)

// Source describes the origin of the event in the upstream database.
type Source struct {
	DB    string `json:"db"`
	Table string `json:"table"`
	LSN   string `json:"lsn"`
}

// Row holds the column values for either the before or after side of an event.
type Row struct {
	AvailableQty int   `json:"available_qty"`
	ReservedQty  int   `json:"reserved_qty,omitempty"`
	StockStatus  string `json:"stock_status,omitempty"`
	RowVersion   int64 `json:"row_version"`
}

// Event is the canonical representation of a single CDC event after the
// Debezium ExtractNewRecordState transform has flattened the envelope.
type Event struct {
	EventID        string    `json:"event_id"`
	EventTime      time.Time `json:"event_time"`
	Source         Source    `json:"source"`
	Op             Op        `json:"op"`
	ProductID      string    `json:"product_id"`
	WarehouseID    string    `json:"warehouse_id"`
	Before         *Row      `json:"before,omitempty"`
	After          *Row      `json:"after,omitempty"`
	SchemaVersion  string    `json:"schema_version"`
	KafkaPartition int32     `json:"-"`
	KafkaOffset    int64     `json:"-"`
}

// Decode parses a raw Kafka message body into an Event.
func Decode(payload []byte) (*Event, error) {
	if len(payload) == 0 {
		return nil, errors.New("empty payload")
	}
	var e Event
	if err := json.Unmarshal(payload, &e); err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}
	return &e, nil
}

// ValidateRequiredFields checks structural invariants that every event must
// satisfy regardless of schema version. It is the cheapest gate in the
// pipeline and runs before SchemaGuard.
func (e *Event) ValidateRequiredFields() error {
	if e.EventID == "" {
		return errors.New("missing event_id")
	}
	if e.Op == "" {
		return errors.New("missing op")
	}
	if e.ProductID == "" {
		return errors.New("missing product_id")
	}
	if e.WarehouseID == "" {
		return errors.New("missing warehouse_id")
	}
	// Tombstone (delete) events legitimately have no after row; everything
	// else must carry one.
	if e.Op != OpDelete && e.After == nil {
		return errors.New("missing after row")
	}
	return nil
}

// LatestRow returns whichever row represents the post-event state. For
// deletes this is the before row (so consumers can act on the last-known
// values); for everything else it is the after row.
func (e *Event) LatestRow() *Row {
	if e.Op == OpDelete {
		return e.Before
	}
	return e.After
}
