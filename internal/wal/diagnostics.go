package wal

import (
	"errors"
	"log"
	"sync"
	"syscall"
	"time"
)

const diagnosticQueueCapacity = 16
const diagnosticInterval = time.Minute

type rollbackDiagnostic struct {
	operation string
	category  string
}

// All WALs share one queue and one worker. A sink blocked indefinitely can
// occupy only that worker, even across repeated WAL close and reopen cycles.
var rollbackDiagnostics = sync.OnceValue(func() chan rollbackDiagnostic {
	queue := make(chan rollbackDiagnostic, diagnosticQueueCapacity)
	go func() {
		for event := range queue {
			log.Printf("WAL rollback confirmed operation=%s category=%s", event.operation, event.category)
		}
	}()
	return queue
})

// DroppedDiagnostics reports the number of this WAL's rollback diagnostics
// discarded because the delivery queue was full. Rate-limited events are excluded.
func (l *Log) DroppedDiagnostics() uint64 {
	return l.diagnosticsDropped.Load()
}

// The append worker serializes calls per Log, so the limiter needs no lock.
func (l *Log) diagnoseRollback(operation string, err error, now time.Time) {
	if !l.lastDiagnostic.IsZero() && now.Sub(l.lastDiagnostic) < diagnosticInterval {
		return
	}
	l.lastDiagnostic = now
	queue := l.diagnosticQueue
	if queue == nil {
		queue = rollbackDiagnostics()
	}
	select {
	case queue <- rollbackDiagnostic{operation: operation, category: errorCategory(err)}:
	default:
		l.diagnosticsDropped.Add(1)
	}
}

func errorCategory(err error) string {
	switch {
	case errors.Is(err, syscall.ENOSPC):
		return "enospc"
	case errors.Is(err, syscall.EIO):
		return "eio"
	default:
		return "other"
	}
}
