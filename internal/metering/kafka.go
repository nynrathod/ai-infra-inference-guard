package metering

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	kafkaSinkBuffer = 1024                   // events buffered before dropping
	kafkaSinkBatch  = 100                    // max events per produce call
	kafkaSinkFlush  = 250 * time.Millisecond // max time an event waits
)

// messageWriter decouples KafkaSink from *kafka.Writer for testing.
type messageWriter interface {
	WriteMessages(ctx context.Context, msgs []kafka.Message) error
	Close() error
}

// KafkaSink is a non-blocking EventSink: Emit queues events on a buffered
// channel and a background goroutine produces them in batches. When the
// buffer is full or a produce fails, events are dropped and counted —
// metering must never stall or fail inference.
type KafkaSink struct {
	w       messageWriter
	log     *slog.Logger
	ch      chan Usage
	done    chan struct{} // closed by run() when fully drained
	closed  atomic.Bool
	dropped atomic.Uint64
}

// NewKafkaSink returns a sink producing JSON-encoded Usage events to the
// topic on the given brokers.
func NewKafkaSink(brokers []string, topic string, log *slog.Logger) *KafkaSink {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.RoundRobin{},
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
	}
	return newKafkaSink(kafkaWriterAdapter{Writer: w}, log)
}

func newKafkaSink(w messageWriter, log *slog.Logger) *KafkaSink {
	if log == nil {
		log = slog.Default()
	}
	s := &KafkaSink{
		w:    w,
		log:  log,
		ch:   make(chan Usage, kafkaSinkBuffer),
		done: make(chan struct{}),
	}
	go s.run()
	return s
}

// Emit enqueues the event; it never blocks. Events are dropped once the
// buffer is full or the sink is closed.
func (s *KafkaSink) Emit(u Usage) {
	if s.closed.Load() {
		return
	}
	select {
	case s.ch <- u:
	default:
		if n := s.dropped.Add(1); n%100 == 1 {
			s.log.Warn("usage events dropped (sink buffer full)", "dropped_total", n)
		}
	}
}

// Close stops accepting events, flushes what is buffered and closes the
// producer. It is safe to call more than once.
func (s *KafkaSink) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	<-s.done
	return s.w.Close()
}

func (s *KafkaSink) run() {
	defer close(s.done)
	ticker := time.NewTicker(kafkaSinkFlush)
	defer ticker.Stop()

	batch := make([]Usage, 0, kafkaSinkBatch)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		msgs := make([]kafka.Message, 0, len(batch))
		for _, u := range batch {
			value, err := json.Marshal(u)
			if err != nil {
				continue
			}
			msgs = append(msgs, kafka.Message{Value: value})
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := s.w.WriteMessages(ctx, msgs)
		cancel()
		if err != nil {
			n := s.dropped.Add(uint64(len(msgs)))
			s.log.Warn("usage events dropped (produce failed)", "dropped_total", n, "error", err)
		}
		batch = batch[:0]
	}

	for {
		if s.closed.Load() {
			for {
				select {
				case u := <-s.ch:
					batch = append(batch, u)
					if len(batch) >= kafkaSinkBatch {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
		select {
		case u := <-s.ch:
			batch = append(batch, u)
			if len(batch) >= kafkaSinkBatch {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// kafkaWriterAdapter adapts *kafka.Writer to the messageWriter interface.
type kafkaWriterAdapter struct {
	*kafka.Writer
}

func (a kafkaWriterAdapter) WriteMessages(ctx context.Context, msgs []kafka.Message) error {
	return a.Writer.WriteMessages(ctx, msgs...)
}
