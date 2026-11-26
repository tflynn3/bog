package wal

import (
	"errors"
	"log"
	"sync"
	"syscall"
	"time"
)

// CommitState describes the terminal result of an admitted group.
type CommitState string

const (
	CommitDurable    CommitState = "durable"
	CommitRolledBack CommitState = "rolled_back"
	CommitUncertain  CommitState = "uncertain"
)

// CommitOutcome reports the most recent terminal group. At is the time the
// worker reached its result. Operation and Category contain fixed labels.
type CommitOutcome struct {
	State     CommitState
	At        time.Time
	Operation string
	Category  string
}

// LastCommitOutcome returns a copy of the latest result in worker order.
// Its zero value means that no append group has finished on this Log yet.
func (l *Log) LastCommitOutcome() CommitOutcome {
	if outcome := l.lastOutcome.Load(); outcome != nil {
		return *outcome
	}
	return CommitOutcome{}
}

func newCommitOutcome(operation string, err error) CommitOutcome {
	outcome := CommitOutcome{State: CommitDurable, At: time.Now(), Operation: operation}
	if err != nil {
		outcome.State = CommitRolledBack
		if errors.Is(err, ErrUncertain) {
			outcome.State = CommitUncertain
		}
		switch {
		case errors.Is(err, syscall.ENOSPC):
			outcome.Category = "enospc"
		case errors.Is(err, syscall.EIO):
			outcome.Category = "eio"
		default:
			outcome.Category = "other"
		}
	}
	return outcome
}

// DroppedDiagnostics counts rate-eligible diagnostics this Log could not queue.
func (l *Log) DroppedDiagnostics() uint64 {
	return l.diagnosticDrops.Load()
}

// Each Log admits at most one event per minute, including events dropped by
// the dispatcher. The append worker owns the limiter.
func (l *Log) diagnoseRollback(outcome CommitOutcome) {
	const interval = time.Minute
	if !l.lastDiagnostic.IsZero() && outcome.At.Sub(l.lastDiagnostic) < interval {
		return
	}
	l.lastDiagnostic = outcome.At
	dispatcher := l.diagnostics
	if dispatcher == nil {
		dispatcher = &walDiagnostics
	}
	if !dispatcher.offer(diagnostic{outcome.Operation, outcome.Category}) {
		l.diagnosticDrops.Add(1)
	}
}

const diagnosticQueueSize = 16

type diagnostic struct {
	operation string
	category  string
}

// Share one bounded queue and sink writer across Logs, including across
// close and reopen. The worker exits when the queue drains. A blocked sink
// can hold one worker and one event indefinitely, with no WAL resources held.
type diagnosticDispatcher struct {
	mu     sync.Mutex
	queue  chan diagnostic
	active bool
	logger *log.Logger
}

var walDiagnostics = diagnosticDispatcher{
	queue:  make(chan diagnostic, diagnosticQueueSize),
	logger: log.Default(),
}

func (d *diagnosticDispatcher) offer(event diagnostic) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	select {
	case d.queue <- event:
		if !d.active {
			d.active = true
			go d.run()
		}
		return true
	default:
		return false
	}
}

func (d *diagnosticDispatcher) run() {
	for {
		d.mu.Lock()
		select {
		case event := <-d.queue:
			d.mu.Unlock()
			d.logger.Printf("WAL rollback confirmed operation=%s category=%s", event.operation, event.category)
		default:
			d.active = false
			d.mu.Unlock()
			return
		}
	}
}
