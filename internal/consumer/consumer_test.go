package consumer

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
)

type stubHandler struct {
	called int
}

func (s *stubHandler) ProcessMessage(_ context.Context, _ kafka.Message) error {
	s.called++
	return nil
}

func TestRun_RejectsEmptyBrokers(t *testing.T) {
	err := Run(context.Background(), Config{Topic: "t", GroupID: "g"}, &stubHandler{})
	if err == nil {
		t.Fatal("expected error for empty brokers")
	}
}

func TestRun_RejectsMissingTopic(t *testing.T) {
	err := Run(context.Background(), Config{Brokers: []string{"localhost:9092"}, GroupID: "g"}, &stubHandler{})
	if err == nil {
		t.Fatal("expected error for missing topic")
	}
}

func TestRun_RejectsMissingGroup(t *testing.T) {
	err := Run(context.Background(), Config{Brokers: []string{"localhost:9092"}, Topic: "t"}, &stubHandler{})
	if err == nil {
		t.Fatal("expected error for missing group")
	}
}
