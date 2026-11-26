package wal

import (
	"errors"
	"time"
)

// CommitState describes the result of the most recent append group.
type CommitState string

const (
	CommitNone       CommitState = ""
	CommitDurable    CommitState = "durable"
	CommitRolledBack CommitState = "rolled_back"
	CommitUncertain  CommitState = "uncertain"
)

// CommitOutcome holds bounded metadata about a completed append group.
// Operation and Category are populated for failures only.
type CommitOutcome struct {
	State     CommitState
	At        time.Time
	Operation string
	Category  string
}

// LastCommitOutcome returns the latest completed append group, or a zero
// outcome before the first one. It is independent of diagnostic delivery and
// excludes bootstrap and requests rejected before they could attempt a write.
func (l *Log) LastCommitOutcome() CommitOutcome {
	l.outcomeMu.RLock()
	defer l.outcomeMu.RUnlock()
	return l.outcome
}

func (l *Log) recordCommitOutcome(operation string, err error) {
	outcome := CommitOutcome{State: CommitDurable, At: time.Now()}
	if err != nil {
		outcome.State = CommitRolledBack
		if errors.Is(err, ErrUncertain) {
			outcome.State = CommitUncertain
		}
		outcome.Operation = operation
		outcome.Category = errorCategory(err)
	}
	l.outcomeMu.Lock()
	l.outcome = outcome
	l.outcomeMu.Unlock()
}
