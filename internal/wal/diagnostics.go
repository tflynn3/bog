package wal

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const diagnosticQueueCapacity = 16

type rollbackDiagnostic struct {
	operation string
	category  string
}

type diagnosticQueue chan rollbackDiagnostic

// Share one delivery worker across WALs. A sink whose Write never returns
// can occupy only this worker, even when WALs are closed and reopened.
var defaultDiagnosticQueue = sync.OnceValue(func() diagnosticQueue {
	return newDiagnosticQueue(log.Default())
})

func newDiagnosticQueue(logger *log.Logger) diagnosticQueue {
	queue := make(diagnosticQueue, diagnosticQueueCapacity)
	go func() {
		for event := range queue {
			logger.Printf("WAL rollback confirmed operation=%s category=%s", event.operation, event.category)
		}
	}()
	return queue
}

type rollbackDiagnostics struct {
	queue   diagnosticQueue
	last    time.Time
	dropped atomic.Uint64
}

// DroppedDiagnostics reports how many of this WAL's diagnostics could not be
// queued because delivery was saturated. The count excludes rate limiting.
func (l *Log) DroppedDiagnostics() uint64 {
	return l.diagnostics.dropped.Load()
}

// The append worker is the only producer for this WAL. Allow one event per
// minute across all operations and categories, even when the queue is full.
func (d *rollbackDiagnostics) report(operation string, err error, now time.Time) {
	if !d.last.IsZero() && now.Sub(d.last) < time.Minute {
		return
	}
	d.last = now
	queue := d.queue
	if queue == nil {
		queue = defaultDiagnosticQueue()
	}
	select {
	case queue <- rollbackDiagnostic{operation: operation, category: errorCategory(err)}:
	default:
		d.dropped.Add(1)
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
