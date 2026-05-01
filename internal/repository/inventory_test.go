package repository

import (
	"testing"
	"time"

	"github.com/WrathOfGenghis/inventory-cdc/pkg/cdcevent"
)

// TestUpsertResult_ZeroValue documents the expected semantics of the
// UpsertResult zero value: a stale event must surface as Applied=false
// without an error so the caller can distinguish "rejected by predicate"
// from "infrastructure failure".
func TestUpsertResult_ZeroValue(t *testing.T) {
	var r UpsertResult
	if r.Applied {
		t.Fatal("zero UpsertResult must have Applied=false")
	}
	if r.RowVersion != 0 {
		t.Fatal("zero UpsertResult must have RowVersion=0")
	}
}

// TestEventLatestRow checks that delete events fall back to the before
// row, while updates and inserts use the after row. This invariant is
// what lets the repository write a meaningful projection for tombstones.
func TestEventLatestRow(t *testing.T) {
	upd := &cdcevent.Event{
		Op:        cdcevent.OpUpdate,
		EventTime: time.Now(),
		Before:    &cdcevent.Row{AvailableQty: 50, RowVersion: 116},
		After:     &cdcevent.Row{AvailableQty: 38, RowVersion: 117},
	}
	if upd.LatestRow().AvailableQty != 38 {
		t.Fatal("update event should project the after row")
	}

	del := &cdcevent.Event{
		Op:     cdcevent.OpDelete,
		Before: &cdcevent.Row{AvailableQty: 12, RowVersion: 200},
	}
	if del.LatestRow().AvailableQty != 12 {
		t.Fatal("delete event should project the before row")
	}
}
