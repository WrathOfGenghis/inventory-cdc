// Package schema implements the SchemaGuard described in §9 of the design.
// Every event passing through the pipeline is evaluated against a versioned
// contract that classifies the change as Compatible, Conditional, or
// Breaking. Bad events are routed to the DLQ, never silently dropped.
package schema

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Contract is the on-disk YAML representation of a schema version. It is
// loaded once at startup; in production a SIGHUP handler triggers a reload.
type Contract struct {
	Version              string            `yaml:"version"`
	RequiredFields       []string          `yaml:"required_fields"`
	Types                map[string]string `yaml:"types"`
	AllowUnknownOptional bool              `yaml:"allow_unknown_optional"`
	Mapping              map[string]string `yaml:"mapping,omitempty"`
}

// LoadContract reads a single contract YAML file from disk.
func LoadContract(path string) (*Contract, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read contract %q: %w", path, err)
	}
	var c Contract
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("parse contract %q: %w", path, err)
	}
	if c.Version == "" {
		return nil, fmt.Errorf("contract %q has no version", path)
	}
	if len(c.RequiredFields) == 0 {
		return nil, fmt.Errorf("contract %q has no required_fields", path)
	}
	return &c, nil
}
