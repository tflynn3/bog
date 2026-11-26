package wal

import "sync"

// DiagnosticEvent contains only bounded operation and error codes.
type DiagnosticEvent struct {
	OpCode      uint8
	ErrCategory uint8
}

const (
	OpWALWrite uint8 = iota + 1
	OpWALSync
	OpMarkerPreparation
)

const (
	ErrCategoryOther uint8 = iota
	ErrCategoryENOSPC
	ErrCategoryEIO
)

type DiagnosticsSink interface {
	Emit(DiagnosticEvent)
}

// AsyncSink owns one delivery worker and a bounded queue. A full queue drops
// the newest event. Stop does not wait for a consumer blocked in Emit.
type AsyncSink struct {
	queue chan DiagnosticEvent
	stop  chan struct{}
	done  chan struct{}
	once  sync.Once
}

func NewAsyncSink(capacity int, consumer DiagnosticsSink) *AsyncSink {
	if capacity < 0 || consumer == nil {
		panic("invalid diagnostics sink configuration")
	}
	s := &AsyncSink{queue: make(chan DiagnosticEvent, capacity), stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(s.done)
		for {
			select {
			case <-s.stop:
				return
			default:
			}
			select {
			case <-s.stop:
				return
			case event := <-s.queue:
				func() {
					defer func() {
						if recover() != nil {
							DiagnosticsDeliveryFailures.Add(1)
						}
					}()
					consumer.Emit(event)
				}()
			}
		}
	}()
	return s
}

// Emit never waits for queue space or for the consumer.
func (s *AsyncSink) Emit(event DiagnosticEvent) {
	select {
	case <-s.stop:
		DiagnosticsDropped.Add(1)
		return
	default:
	}
	select {
	case <-s.stop:
		DiagnosticsDropped.Add(1)
	case s.queue <- event:
	default:
		DiagnosticsDropped.Add(1)
	}
}

// Stop requests termination without waiting for an in-flight delivery. Events
// still queued at shutdown are abandoned. Done closes when the worker exits.
func (s *AsyncSink) Stop() { s.once.Do(func() { close(s.stop) }) }

func (s *AsyncSink) Done() <-chan struct{} { return s.done }
