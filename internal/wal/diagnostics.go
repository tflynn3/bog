package wal

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"syscall"
)

// OpCode identifies the operation whose failure was rolled back.
type OpCode uint8

const (
	OpWALWrite OpCode = iota + 1
	OpWALSync
	OpMarkerPreparation
)

// ErrorCategory identifies a bounded class of errors without exposing error text.
type ErrorCategory uint8

const (
	ErrorOther ErrorCategory = iota
	ErrorENOSPC
	ErrorEIO
)

// DiagnosticEvent contains no payload, path, or raw error text.
type DiagnosticEvent struct {
	OpCode        OpCode
	ErrorCategory ErrorCategory
}

// DiagnosticsDeliveryFailures counts sink write failures independently of queue drops.
var DiagnosticsDeliveryFailures atomic.Uint64

var rollbackDiagnostics = newDiagnosticSink()

type diagnosticSink struct {
	events  chan DiagnosticEvent
	stop    chan struct{}
	once    sync.Once
	closed  atomic.Bool
	deliver func(DiagnosticEvent) error
}

func newDiagnosticSink() *diagnosticSink {
	s := &diagnosticSink{events: make(chan DiagnosticEvent, 1024), stop: make(chan struct{}), deliver: deliverDiagnostic}
	go s.run()
	return s
}

// ReportRollback hands an event to the delivery worker without waiting for space.
// A full or stopped queue drops the new event and increments DiagnosticsDropped.
func ReportRollback(op OpCode, err error) {
	category := ErrorOther
	if errors.Is(err, syscall.ENOSPC) {
		category = ErrorENOSPC
	} else if errors.Is(err, syscall.EIO) {
		category = ErrorEIO
	}
	rollbackDiagnostics.report(DiagnosticEvent{OpCode: op, ErrorCategory: category})
}

func (s *diagnosticSink) report(event DiagnosticEvent) {
	if s.closed.Load() {
		DiagnosticsDropped.Add(1)
		return
	}
	select {
	case s.events <- event:
	default:
		DiagnosticsDropped.Add(1)
	}
}

// ShutdownDiagnostics requests worker shutdown without waiting for a blocked sink.
// Queued events may be discarded. The worker exits after any in-flight write returns.
func ShutdownDiagnostics() {
	rollbackDiagnostics.shutdown()
}

func (s *diagnosticSink) shutdown() {
	s.once.Do(func() {
		s.closed.Store(true)
		close(s.stop)
	})
}

func (s *diagnosticSink) run() {
	for {
		select {
		case <-s.stop:
			return
		case event := <-s.events:
			if err := s.deliver(event); err != nil {
				DiagnosticsDeliveryFailures.Add(1)
			}
		}
	}
}

// deliverDiagnostic runs only on the consumer goroutine.
func deliverDiagnostic(event DiagnosticEvent) error {
	op := "unknown"
	switch event.OpCode {
	case OpWALWrite:
		op = "wal_write"
	case OpWALSync:
		op = "wal_sync"
	case OpMarkerPreparation:
		op = "marker_preparation"
	}
	category := "other"
	switch event.ErrorCategory {
	case ErrorENOSPC:
		category = "enospc"
	case ErrorEIO:
		category = "eio"
	}
	return log.Output(2, "WAL rollback confirmed operation="+op+" category="+category)
}
