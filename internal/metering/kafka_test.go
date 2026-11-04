package metering

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// fakeWriter records produced messages without a broker.
type fakeWriter struct {
	err  error
	msgs chan []kafka.Message
}

func (w *fakeWriter) WriteMessages(_ context.Context, msgs []kafka.Message) error {
	if w.err != nil {
		return w.err
	}
	w.msgs <- append([]kafka.Message(nil), msgs...)
	return nil
}

func (w *fakeWriter) Close() error { return nil }

func TestKafkaSinkProducesEvents(t *testing.T) {
	w := &fakeWriter{msgs: make(chan []kafka.Message, 8)}
	s := newKafkaSink(w, testLogger())
	t.Cleanup(func() { _ = s.Close() })

	s.Emit(Usage{TenantID: "tenant-a", CompletionTokens: 5})
	s.Emit(Usage{TenantID: "tenant-a", CompletionTokens: 7})

	got := 0
	deadline := time.After(2 * time.Second)
	for got < 2 {
		select {
		case msgs := <-w.msgs:
			got += len(msgs)
			var u Usage
			if err := json.Unmarshal(msgs[0].Value, &u); err != nil {
				t.Fatal(err)
			}
			if u.TenantID != "tenant-a" {
				t.Fatalf("message = %+v, want tenant-a", u)
			}
		case <-deadline:
			t.Fatalf("only %d messages produced within 2s", got)
		}
	}
}

func TestKafkaSinkDropsOnProduceError(t *testing.T) {
	w := &fakeWriter{err: errors.New("broker down"), msgs: make(chan []kafka.Message, 8)}
	s := newKafkaSink(w, testLogger())

	s.Emit(Usage{TenantID: "tenant-a"})
	time.Sleep(500 * time.Millisecond) // let one flush tick fail

	if got := s.dropped.Load(); got == 0 {
		t.Error("dropped counter = 0, want > 0")
	}
	_ = s.Close()
}

func TestKafkaSinkCloseDrains(t *testing.T) {
	w := &fakeWriter{msgs: make(chan []kafka.Message, 8)}
	s := newKafkaSink(w, testLogger())

	s.Emit(Usage{TenantID: "tenant-a"})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case msgs := <-w.msgs:
		if len(msgs) != 1 {
			t.Fatalf("drained %d messages, want 1", len(msgs))
		}
	default:
		t.Fatal("Close did not drain the buffered event")
	}
	s.Emit(Usage{}) // must not panic after Close
}
