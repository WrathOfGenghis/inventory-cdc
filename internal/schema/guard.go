package schema

import (
	"github.com/WrathOfGenghis/inventory-cdc/pkg/cdcevent"
)

// Decision is the three-way verdict SchemaGuard returns for every event.
type Decision int

const (
	// Compatible means the event matches the contract exactly and can be
	// applied without intervention.
	Compatible Decision = iota
	// Conditional means the event has unknown optional fields or other
	// non-fatal differences. It is applied but a warning metric increments.
	Conditional
	// Breaking means a required field is missing or a type is incompatible.
	// The event must not be applied; route to DLQ.
	Breaking
)

// String renders the Decision in a human-readable form for logging.
func (d Decision) String() string {
	switch d {
	case Compatible:
		return "compatible"
	case Conditional:
		return "conditional"
	case Breaking:
		return "breaking"
	default:
		return "unknown"
	}
}

// Verdict is the full output of an evaluation: the decision plus a
// human-readable reason and the offending field (if any).
type Verdict struct {
	Decision Decision
	Reason   string
	Field    string
	Contract string
}

// Guard evaluates events against a single active contract. The design
// supports multiple contracts in flight via a registry; this implementation
// keeps it simple with a single active version and is sufficient for the
// MVP described in §9.
type Guard struct {
	active *Contract
}

// LoadGuard reads a contract from disk and returns a ready-to-use Guard.
func LoadGuard(path string) (*Guard, error) {
	c, err := LoadContract(path)
	if err != nil {
		return nil, err
	}
	return &Guard{active: c}, nil
}

// NewGuard constructs a Guard from an in-memory contract; primarily useful
// for tests.
func NewGuard(c *Contract) *Guard {
	return &Guard{active: c}
}

// Evaluate runs the contract checks on the given event and returns a
// Verdict. The event is not mutated.
func (g *Guard) Evaluate(e *cdcevent.Event) Verdict {
	if e == nil {
		return Verdict{Decision: Breaking, Reason: "nil event"}
	}

	// Required-field check using a small dot-notation path lookup.
	for _, field := range g.active.RequiredFields {
		if !fieldPresent(e, field) {
			return Verdict{
				Decision: Breaking,
				Reason:   "REQUIRED_FIELD_MISSING",
				Field:    field,
				Contract: g.active.Version,
			}
		}
	}

	// Schema version mismatch is conditional, not breaking — older producers
	// may continue to emit the prior version during a rolling migration.
	if e.SchemaVersion != "" && e.SchemaVersion != g.active.Version {
		return Verdict{
			Decision: Conditional,
			Reason:   "SCHEMA_VERSION_DRIFT",
			Field:    "schema_version",
			Contract: g.active.Version,
		}
	}

	return Verdict{Decision: Compatible, Contract: g.active.Version}
}

// fieldPresent does a shallow check on the well-known event fields. The
// dot-notation path style ("after.available_qty") is used by the YAML
// contract; here we map a small number of explicit paths because the event
// struct is fixed at compile time. Adding a new required field to the
// contract requires updating both the YAML and this switch.
func fieldPresent(e *cdcevent.Event, field string) bool {
	switch field {
	case "event_id":
		return e.EventID != ""
	case "op":
		return e.Op != ""
	case "product_id":
		return e.ProductID != ""
	case "warehouse_id":
		return e.WarehouseID != ""
	case "schema_version":
		return e.SchemaVersion != ""
	case "after.available_qty":
		return e.After != nil
	case "after.row_version":
		return e.After != nil && e.After.RowVersion > 0
	case "event_time":
		return !e.EventTime.IsZero()
	default:
		// Unknown required field in the contract is itself a breaking
		// condition: the contract author intends a check we cannot perform.
		return false
	}
}
