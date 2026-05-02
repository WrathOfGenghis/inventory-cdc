package cdcevent

import (
	"testing"
	"time"
)

// TestDecode_CanonicalEnvelope verifies the long-standing contract format
// (event_id, op, product_id, ... at the top level, plus nested before/after
// rows) is still accepted unchanged.
func TestDecode_CanonicalEnvelope(t *testing.T) {
	payload := []byte(`{
		"event_id": "01JXR4P5K2W7ZBN8M3VQ7T2YHA",
		"event_time": "2026-04-30T10:15:32.120+05:30",
		"op": "u",
		"product_id": "SKU-AC-9482",
		"warehouse_id": "WH-MUM-01",
		"schema_version": "v3",
		"after": {"available_qty": 38, "row_version": 117}
	}`)

	e, err := Decode(payload)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if e.EventID != "01JXR4P5K2W7ZBN8M3VQ7T2YHA" {
		t.Errorf("event_id mismatch: %q", e.EventID)
	}
	if e.ProductID != "SKU-AC-9482" || e.WarehouseID != "WH-MUM-01" {
		t.Errorf("business keys mismatch: %q / %q", e.ProductID, e.WarehouseID)
	}
	if e.After == nil || e.After.RowVersion != 117 {
		t.Errorf("after row missing or wrong version: %+v", e.After)
	}
	if e.SchemaVersion != "v3" {
		t.Errorf("schema_version mismatch: %q", e.SchemaVersion)
	}
}

// TestDecode_DebeziumEnvelope verifies the native Debezium 2.x envelope
// (no unwrap SMT) is normalised into the canonical Event shape.
func TestDecode_DebeziumEnvelope(t *testing.T) {
	payload := []byte(`{
		"op": "u",
		"ts_ms": 1714451732150,
		"source": {
			"version": "2.5.0.Final",
			"connector": "postgresql",
			"name": "warehouse_db",
			"ts_ms": 1714451732120,
			"db": "warehouse",
			"schema": "public",
			"table": "warehouse_inventory",
			"txId": 1284701,
			"lsn": 24139800
		},
		"before": {
			"product_id": "SKU-AC-9482",
			"warehouse_id": "WH-MUM-01",
			"available_qty": 50,
			"reserved_qty": 0,
			"stock_status": "ACTIVE",
			"row_version": 116
		},
		"after": {
			"product_id": "SKU-AC-9482",
			"warehouse_id": "WH-MUM-01",
			"available_qty": 38,
			"reserved_qty": 0,
			"stock_status": "ACTIVE",
			"row_version": 117
		}
	}`)

	e, err := Decode(payload)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// EventID should be derived deterministically from source coordinates.
	if e.EventID == "" {
		t.Error("event_id should be derived from source")
	}
	if e.Op != OpUpdate {
		t.Errorf("op mismatch: %q", e.Op)
	}
	// Business keys are promoted from the row to the top level.
	if e.ProductID != "SKU-AC-9482" {
		t.Errorf("product_id not promoted: %q", e.ProductID)
	}
	if e.WarehouseID != "WH-MUM-01" {
		t.Errorf("warehouse_id not promoted: %q", e.WarehouseID)
	}
	// schema_version defaults when Debezium does not supply one.
	if e.SchemaVersion != defaultSchemaVersion {
		t.Errorf("expected default schema_version, got %q", e.SchemaVersion)
	}
	// EventTime comes from source.ts_ms.
	expected := time.UnixMilli(1714451732120).UTC()
	if !e.EventTime.Equal(expected) {
		t.Errorf("event_time mismatch: got %s, want %s", e.EventTime, expected)
	}
	// Source block is populated.
	if e.Source.Table != "warehouse_inventory" {
		t.Errorf("source.table missing: %q", e.Source.Table)
	}
	if e.Source.LSN != "24139800" {
		t.Errorf("source.lsn mismatch: %q", e.Source.LSN)
	}
	// Before and after rows are normalised.
	if e.Before == nil || e.Before.RowVersion != 116 {
		t.Errorf("before row not normalised: %+v", e.Before)
	}
	if e.After == nil || e.After.AvailableQty != 38 || e.After.RowVersion != 117 {
		t.Errorf("after row not normalised: %+v", e.After)
	}
}

// TestDecode_EmptyPayload guards against the trivial empty-input case
// the consumer might hit with a malformed Kafka message.
func TestDecode_EmptyPayload(t *testing.T) {
	if _, err := Decode(nil); err == nil {
		t.Error("expected error for nil payload")
	}
	if _, err := Decode([]byte{}); err == nil {
		t.Error("expected error for empty payload")
	}
}

// TestDecode_DebeziumDelete checks that delete events (op=d) carry the
// before row and validate without an after row.
func TestDecode_DebeziumDelete(t *testing.T) {
	payload := []byte(`{
		"op": "d",
		"ts_ms": 1714451732150,
		"source": {
			"db": "warehouse", "schema": "public",
			"table": "warehouse_inventory", "txId": 1284799, "lsn": 24139900,
			"ts_ms": 1714451732120
		},
		"before": {
			"product_id": "SKU-AC-9482",
			"warehouse_id": "WH-MUM-01",
			"available_qty": 0,
			"row_version": 200
		},
		"after": null
	}`)

	e, err := Decode(payload)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if e.Op != OpDelete {
		t.Errorf("expected op=d, got %q", e.Op)
	}
	if err := e.ValidateRequiredFields(); err != nil {
		t.Errorf("validation should pass for delete: %v", err)
	}
	if e.LatestRow() != e.Before {
		t.Errorf("LatestRow on delete should return before row")
	}
}
