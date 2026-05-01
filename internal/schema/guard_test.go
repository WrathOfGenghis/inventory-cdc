package schema

import (
	"testing"
	"time"

	"github.com/WrathOfGenghis/inventory-cdc/pkg/cdcevent"
)

func testContract() *Contract {
	return &Contract{
		Version: "v3",
		RequiredFields: []string{
			"event_id", "op", "product_id", "warehouse_id",
			"after.available_qty", "after.row_version",
		},
		Types: map[string]string{
			"product_id":           "string",
			"warehouse_id":         "string",
			"after.available_qty":  "int",
			"after.row_version":    "long",
		},
		AllowUnknownOptional: true,
	}
}

func validEvent() *cdcevent.Event {
	return &cdcevent.Event{
		EventID:       "01JXR4P5K2W7ZBN8M3VQ7T2YHA",
		EventTime:     time.Now(),
		Op:            cdcevent.OpUpdate,
		ProductID:     "SKU-AC-9482",
		WarehouseID:   "WH-MUM-01",
		SchemaVersion: "v3",
		After:         &cdcevent.Row{AvailableQty: 38, RowVersion: 117},
	}
}

func TestGuard_Compatible(t *testing.T) {
	g := NewGuard(testContract())
	v := g.Evaluate(validEvent())
	if v.Decision != Compatible {
		t.Fatalf("expected Compatible, got %s (reason=%s)", v.Decision, v.Reason)
	}
}

func TestGuard_BreakingMissingRequired(t *testing.T) {
	g := NewGuard(testContract())
	e := validEvent()
	e.ProductID = ""
	v := g.Evaluate(e)
	if v.Decision != Breaking {
		t.Fatalf("expected Breaking, got %s", v.Decision)
	}
	if v.Field != "product_id" {
		t.Fatalf("expected field=product_id, got %q", v.Field)
	}
}

func TestGuard_ConditionalSchemaDrift(t *testing.T) {
	g := NewGuard(testContract())
	e := validEvent()
	e.SchemaVersion = "v2"
	v := g.Evaluate(e)
	if v.Decision != Conditional {
		t.Fatalf("expected Conditional, got %s", v.Decision)
	}
}
