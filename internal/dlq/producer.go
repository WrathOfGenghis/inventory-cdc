// Package dlq implements the dead-letter queue producer described in §13
// of the design. Events that cannot be applied — decode errors, missing
// required fields, breaking schema changes — are published here with a
// reason code and the original payload so an operator can investigate.
package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// Reason enumerates the DLQ rejection categories. The same set of strings
// is used as the value of the Prometheus label inventory_dlq_total{reason}.
type Reason string

const (
	ReasonDecodeError      Reason = "decode_error"
	ReasonMissingRequired  Reason = "missing_required"
	ReasonSchemaBreaking   Reason = "schema_breaking"
	ReasonProjectionError  Reason = "projection_error"
	ReasonNoMapping        Reason = "no_mapping"
)

// Envelope is the JSON written to the DLQ topic. Keeping the envelope
// stable makes the replay job in runbooks/reconcile-replay.md mechanical.
type Envelope struct {
	OriginalTopic     string    `json:"original_topic"`
	OriginalPartition int       `json:"original_partition"`
	OriginalOffset    int64     `json:"original_offset"`
	EventID           string    `json:"event_id,omitempty"`
	Reason            Reason    `json:"reason"`
	ReasonDetail      string    `json:"reason_detail,omitempty"`
	FirstFailedAt     time.Time `json:"first_failed_at"`
	RawPayload        string    `json:"raw_payload"`
}

// Producer wraps a kafka-go Writer scoped to the DLQ topic.
type Producer struct {
	writer *kafka.Writer
	topic  string
}

// NewProducer constructs a Producer for the given brokers and topic. The
// Writer uses RequireAll acks so a DLQ message is durable before the
// caller commits the source offset.
func NewProducer(brokers []string, topic string) *Producer {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
		WriteTimeout:           10 * time.Second,
	}
	return &Producer{writer: w, topic: topic}
}

// Close flushes any pending messages and releases the writer.
func (p *Producer) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

// Send publishes a DLQ envelope. It is the caller's responsibility to
// supply a meaningful reason; the message body is the original Kafka
// message value.
func (p *Producer) Send(ctx context.Context, src kafka.Message, eventID string, reason Reason, detail string) error {
	env := Envelope{
		OriginalTopic:     src.Topic,
		OriginalPartition: src.Partition,
		OriginalOffset:    src.Offset,
		EventID:           eventID,
		Reason:            reason,
		ReasonDetail:      detail,
		FirstFailedAt:     time.Now().UTC(),
		RawPayload:        string(src.Value),
	}

	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("dlq marshal: %w", err)
	}

	msg := kafka.Message{
		Key:   src.Key,
		Value: body,
		Time:  env.FirstFailedAt,
		Headers: []kafka.Header{
			{Key: "x-dlq-reason", Value: []byte(reason)},
			{Key: "x-original-topic", Value: []byte(src.Topic)},
		},
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("dlq write: %w", err)
	}
	return nil
}
