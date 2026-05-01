// Package config loads orchestrator configuration from environment variables
// and validates required fields. All knobs are documented in docs/slo.md.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// Config holds the runtime configuration for the orchestrator.
type Config struct {
	KafkaBrokers       []string
	KafkaTopic         string
	KafkaGroup         string
	DLQTopic           string
	PostgresDSN        string
	RedisAddr          string
	SchemaContractPath string
	MetricsAddr        string
	LogLevel           slog.Level
}

// Load reads configuration from environment variables and returns a populated
// Config or an error listing every missing required value.
func Load() (Config, error) {
	cfg := Config{
		KafkaBrokers:       splitCSV(getenv("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:         getenv("KAFKA_TOPIC", "cdc.inventory.warehouse"),
		KafkaGroup:         getenv("KAFKA_GROUP", "inventory-orchestrator"),
		DLQTopic:           getenv("KAFKA_DLQ_TOPIC", "cdc.inventory.dlq"),
		PostgresDSN:        os.Getenv("POSTGRES_DSN"),
		RedisAddr:          getenv("REDIS_ADDR", "localhost:6379"),
		SchemaContractPath: getenv("SCHEMA_CONTRACT", "schema/contracts/inventory.v3.yaml"),
		MetricsAddr:        getenv("METRICS_ADDR", ":8080"),
		LogLevel:           parseLevel(getenv("LOG_LEVEL", "info")),
	}

	var missing []string
	if cfg.PostgresDSN == "" {
		missing = append(missing, "POSTGRES_DSN")
	}
	if len(cfg.KafkaBrokers) == 0 {
		missing = append(missing, "KAFKA_BROKERS")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required env: %s", strings.Join(missing, ", "))
	}

	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
