// Package cdcevent defines the wire format for CDC events flowing through
// the inventory pipeline. The contract is documented in §5 of the design.
//
// Two payload shapes are accepted by Decode:
//
//  1. The canonical contract envelope used in the report and tests
//     (event_id, op, product_id, warehouse_id at top level, etc.).
//
//  2. The native Debezium 2.x envelope produced by the connector in
//     deploy/debezium/connector.json — that is, with the
//     ExtractNewRecordState SMT *disabled* so before/after rows and the
//     source block are preserved.
//
// Decode probes the payload, picks the right path, and returns a populated
// Event in either case so downstream code (handler, schema guard,
// repository) does not need to care which producer wrote the message.
package cdcevent

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
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
// Field names line up with what Debezium's Postgres connector writes so
// the same struct can be unmarshalled from either envelope shape.
type Source struct {
	DB     string `json:"db"`
	Table  string `json:"table"`
	LSN    string `json:"lsn"`
	TsMs   int64  `json:"ts_ms,omitempty"`
	TxID   int64  `json:"txId,omitempty"`
	Schema string `json:"schema,omitempty"`
}

// Row holds the column values for either the before or after side of an
// event. Only the columns that the projection cares about are listed; the
// JSON decoder ignores any others Debezium happens to emit.
type Row struct {
	AvailableQty int    `json:"available_qty"`
	ReservedQty  int    `json:"reserved_qty,omitempty"`
	StockStatus  string `json:"stock_status,omitempty"`
	RowVersion   int64  `json:"row_version"`
}

// Event is the canonical representation of a single CDC event. The shape
// matches the contract documented in §5 of the report; Decode normalises
// Debezium's envelope into the same shape.
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

// defaultSchemaVersion is stamped on Debezium-shaped events that do not
// carry an application-level schema_version field of their own. The
// connector publishes a single table whose contract version is fixed at
// deploy time, so this is a safe default.
const defaultSchemaVersion = "v3"

// Decode parses a raw Kafka message body into an Event. The function
// tolerates two payload shapes (see package doc) and is the single point
// where Debezium-specific quirks are absorbed.
func Decode(payload []byte) (*Event, error) {
	if len(payload) == 0 {
		return nil, errors.New("empty payload")
	}

	// Probe just enough of the JSON to tell which shape we are looking
	// at. Debezium 2.x always writes a `source` object alongside the
	// before/after rows; the canonical contract envelope, by contrast,
	// has a top-level `event_id` string.
	var probe struct {
		EventID string          `json:"event_id"`
		Source  json.RawMessage `json:"source"`
	}
	if err := json.Unmarshal(payload, &probe); err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}

	if probe.EventID == "" && len(probe.Source) > 0 {
		return decodeDebezium(payload)
	}

	var e Event
	if err := json.Unmarshal(payload, &e); err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}
	return &e, nil
}

// debeziumRow mirrors what Debezium publishes for warehouse_inventory: the
// inventory columns plus the business keys. product_id and warehouse_id
// live inside the row in this shape, not at the top level.
type debeziumRow struct {
	ProductID    string `json:"product_id"`
	WarehouseID  string `json:"warehouse_id"`
	AvailableQty int    `json:"available_qty"`
	ReservedQty  int    `json:"reserved_qty"`
	StockStatus  string `json:"stock_status"`
	RowVersion   int64  `json:"row_version"`
}

// debeziumEnvelope is what the Postgres connector writes when no unwrap
// SMT is configured. Only the fields the orchestrator actually needs are
// listed; everything else is ignored by the JSON decoder.
type debeziumEnvelope struct {
	Op     string       `json:"op"`
	TsMs   int64        `json:"ts_ms"`
	Source debeziumSrc  `json:"source"`
	Before *debeziumRow `json:"before"`
	After  *debeziumRow `json:"after"`
}

type debeziumSrc struct {
	DB     string          `json:"db"`
	Schema string          `json:"schema"`
	Table  string          `json:"table"`
	TxID   int64           `json:"txId"`
	TsMs   int64           `json:"ts_ms"`
	LSN    json.RawMessage `json:"lsn"` // pgoutput emits a number; Avro uses a string
}

// decodeDebezium translates a Debezium envelope into the canonical Event
// shape. The translation is purely structural: it does not call out to
// any registry and never blocks.
func decodeDebezium(payload []byte) (*Event, error) {
	var d debeziumEnvelope
	if err := json.Unmarshal(payload, &d); err != nil {
		return nil, fmt.Errorf("debezium unmarshal: %w", err)
	}

	row := d.After
	if row == nil {
		row = d.Before
	}
	if row == nil {
		return nil, errors.New("debezium envelope has no before or after row")
	}

	lsn := lsnString(d.Source.LSN)

	e := &Event{
		// EventID is derived from a Debezium-stable triple. The same
		// row mutation always produces the same id, which is exactly
		// what the idempotency store needs.
		EventID:       fmt.Sprintf("%s-%s-%d", d.Source.Table, lsn, d.Source.TxID),
		EventTime:     msToTime(firstNonZero(d.Source.TsMs, d.TsMs)),
		Op:            Op(d.Op),
		ProductID:     row.ProductID,
		WarehouseID:   row.WarehouseID,
		SchemaVersion: defaultSchemaVersion,
		Source: Source{
			DB:     d.Source.DB,
			Table:  d.Source.Table,
			Schema: d.Source.Schema,
			LSN:    lsn,
			TsMs:   d.Source.TsMs,
			TxID:   d.Source.TxID,
		},
	}
	if d.Before != nil {
		e.Before = &Row{
			AvailableQty: d.Before.AvailableQty,
			ReservedQty:  d.Before.ReservedQty,
			StockStatus:  d.Before.StockStatus,
			RowVersion:   d.Before.RowVersion,
		}
	}
	if d.After != nil {
		e.After = &Row{
			AvailableQty: d.After.AvailableQty,
			ReservedQty:  d.After.ReservedQty,
			StockStatus:  d.After.StockStatus,
			RowVersion:   d.After.RowVersion,
		}
	}
	return e, nil
}

func lsnString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	// The Postgres connector emits the LSN as a JSON number; Avro mode
	// emits it quoted. Try both.
	s := string(raw)
	if s[0] == '"' {
		var unquoted string
		if err := json.Unmarshal(raw, &unquoted); err == nil {
			return unquoted
		}
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		return strconv.FormatInt(n, 10)
	}
	return s
}

func firstNonZero(a, b int64) int64 {
	if a != 0 {
		return a
	}
	return b
}

func msToTime(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// ValidateRequiredFields checks structural invariants that every event
// must satisfy regardless of schema version. It is the cheapest gate in
// the pipeline and runs before SchemaGuard.
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
	// Tombstone (delete) events legitimately have no after row;
	// everything else must carry one.
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
