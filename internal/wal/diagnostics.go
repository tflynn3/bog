package wal

import (
	"errors"
	"runtime"
	"sync/atomic"
	"syscall"
)

// DiagnosticEvent contains only bounded identifiers, never caller-supplied text.
type DiagnosticEvent struct {
	OpCode      DiagnosticOpCode
	ErrCategory DiagnosticErrCategory
}

type DiagnosticOpCode uint8

const (
	DiagnosticOpUnknown DiagnosticOpCode = iota
	DiagnosticOpWALWrite
	DiagnosticOpWALSync
	DiagnosticOpMarkerPreparation
)

type DiagnosticErrCategory uint8

const (
	DiagnosticErrOther DiagnosticErrCategory = iota
	DiagnosticErrENOSPC
	DiagnosticErrEIO
)

// DiagnosticCategory classifies wrapped errors without retaining their text.
func DiagnosticCategory(err error) DiagnosticErrCategory {
	if errors.Is(err, syscall.ENOSPC) {
		return DiagnosticErrENOSPC
	}
	if errors.Is(err, syscall.EIO) {
		return DiagnosticErrEIO
	}
	return DiagnosticErrOther
}

// DiagnosticsSink receives diagnostic events. Implementations may block.
type DiagnosticsSink interface{ Emit(DiagnosticEvent) }

const MaxDiagnosticQueue = 1024
const diagnosticClosed = uint64(1) << 63

// AsyncSink owns one delivery worker and a bounded queue. Close discards queued
// events and returns immediately, even when the downstream sink is blocked.
// Done closes once the worker has actually exited; callers need not wait for it.
type AsyncSink struct {
	sink  DiagnosticsSink
	queue chan DiagnosticEvent
	stop  chan struct{}
	done  chan struct{}
	// The high bit marks closure; the remaining bits count active Emit calls.
	state atomic.Uint64
}

// NewAsyncSink starts one worker. Capacity is clamped to [1, MaxDiagnosticQueue].
func NewAsyncSink(sink DiagnosticsSink, capacity int) *AsyncSink {
	if sink == nil {
		panic("wal: nil diagnostics sink")
	}
	if capacity < 1 {
		capacity = 1
	}
	if capacity > MaxDiagnosticQueue {
		capacity = MaxDiagnosticQueue
	}
	s := &AsyncSink{sink: sink, queue: make(chan DiagnosticEvent, capacity), stop: make(chan struct{}), done: make(chan struct{})}
	go s.run()
	return s
}

// Emit never waits for queue space or downstream delivery. New events are
// dropped when the queue is full or the sink has been closed.
func (s *AsyncSink) Emit(event DiagnosticEvent) {
	for {
		state := s.state.Load()
		if state&diagnosticClosed != 0 {
			DiagnosticsDropped.Add(1)
			return
		}
		if s.state.CompareAndSwap(state, state+1) {
			break
		}
	}
	defer s.state.Add(^uint64(0))
	select {
	case s.queue <- event:
	default:
		DiagnosticsDropped.Add(1)
	}
}

// Close requests shutdown without waiting for an in-progress delivery.
func (s *AsyncSink) Close() {
	for {
		state := s.state.Load()
		if state&diagnosticClosed != 0 {
			return
		}
		if s.state.CompareAndSwap(state, state|diagnosticClosed) {
			close(s.stop)
			return
		}
	}
}

func (s *AsyncSink) Done() <-chan struct{} { return s.done }

func (s *AsyncSink) run() {
	defer close(s.done)
	for {
		select {
		case <-s.stop:
			// Producers already admitted before Close must finish before accounting
			// for abandoned events. This wait belongs only to the delivery worker.
			for s.state.Load() != diagnosticClosed {
				runtime.Gosched()
			}
			for {
				select {
				case <-s.queue:
					DiagnosticsDropped.Add(1)
				default:
					return
				}
			}
		default:
		}
		select {
		case <-s.stop:
			continue
		case event := <-s.queue:
			s.deliver(event)
		}
	}
}

func (s *AsyncSink) deliver(event DiagnosticEvent) {
	defer func() {
		if recover() != nil {
			DiagnosticsDeliveryFailures.Add(1)
		}
	}()
	s.sink.Emit(event)
}
