package idempotency

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"errors"
	"fmt"
	"time"
)

const (
	DefaultRetryWindow = 24 * time.Hour
	MaxCleanupBatch    = 1024
)

var (
	ErrConflict        = errors.New("ingest retry key already used for a different batch")
	ErrRolledBack      = errors.New("ingest attempt confirmed uncommitted")
	ErrUnknown         = errors.New("ingest commit outcome unknown")
	ErrInvalidConfig   = errors.New("invalid ingest retry configuration")
	ErrCleanupLimit    = errors.New("invalid expiry cleanup limit")
	ErrBackendContract = errors.New("ingest retry backend contract violated")
)

// Config defines the deadline for new commits. Committed records retain their
// original expiry across process restarts and configuration changes.
type Config struct {
	RetryWindow time.Duration    // Zero selects DefaultRetryWindow.
	Clock       func() time.Time // Nil selects time.Now. Must be concurrency-safe.
	// All coordinators serving the same namespace must use the same secret
	// until records made with it expire. Generate it from a secure source
	// and persist it separately from the retry records.
	FingerprintSecret [SecretBytes]byte
}

// Outcome classifies a submission so callers can distinguish rejected input,
// confirmed rollback, and uncertain delivery.
type Outcome uint8

const (
	OutcomeUnknown Outcome = iota
	OutcomeCommitted
	OutcomeReplayed
	OutcomeRejected
	OutcomeUncommitted
)

// Coordinator serializes in-process submissions by scoped key and consults the
// transactional backend for every decision. Use New to construct it. It is
// safe for concurrent use and must not be copied after first use.
type Coordinator struct {
	backend Backend
	clock   func() time.Time
	window  time.Duration
	secret  [SecretBytes]byte
	gate    keyGate
}

// New constructs a coordinator over the backend's committed state. It
// starts no workers and needs no shutdown or in-memory index reconstruction.
func New(backend Backend, config Config) (*Coordinator, error) {
	if backend == nil {
		return nil, fmt.Errorf("%w: a backend is required", ErrInvalidConfig)
	}
	if config.FingerprintSecret == ([SecretBytes]byte{}) {
		return nil, fmt.Errorf("%w: a fingerprint secret is required", ErrInvalidConfig)
	}
	if config.RetryWindow < 0 {
		return nil, fmt.Errorf("%w: retry window must be positive", ErrInvalidConfig)
	}
	if config.RetryWindow == 0 {
		config.RetryWindow = DefaultRetryWindow
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	return &Coordinator{
		backend: backend,
		clock:   config.Clock,
		window:  config.RetryWindow,
		secret:  config.FingerprintSecret,
	}, nil
}

// Submit attempts one validated keyed ingest. A committed or replayed result
// has a nil error. A conflict returns OutcomeRejected and ErrConflict. If an
// attempt is uncertain, Submit asks the backend to resolve it once while ctx
// permits. Further attempts must use the same scoped key and original bytes
// within the retry window.
func (c *Coordinator) Submit(ctx context.Context, request Request) (Outcome, error) {
	if err := request.validate(); err != nil {
		return OutcomeRejected, err
	}
	release, err := c.gate.acquire(ctx, request.Key)
	if err != nil {
		return OutcomeUncommitted, errors.Join(ErrRolledBack, err)
	}
	defer release()

	req := CommitRequest{
		Key:           request.Key,
		Fingerprint:   fingerprint(c.secret, request.Original),
		PolicyVersion: request.PolicyVersion,
		Batch:         request.Batch,
		clock:         c.clock,
		window:        c.window,
	}
	if _, err := rand.Read(req.AttemptID[:]); err != nil {
		return OutcomeUncommitted, errors.Join(ErrRolledBack, err)
	}
	if err := ctx.Err(); err != nil {
		return OutcomeUncommitted, errors.Join(ErrRolledBack, err)
	}

	result, err := c.backend.Commit(ctx, req)
	outcome, cause := classify(req, result, err)
	if outcome != OutcomeUnknown {
		return outcome, cause
	}
	if err := ctx.Err(); err != nil {
		return OutcomeUnknown, errors.Join(ErrUnknown, cause, err)
	}

	// Resolve this attempt without resubmitting or using an unbounded context
	// after a client disconnect. A later retry consults the same authority.
	result, err = c.backend.Resolve(ctx, req.Key, req.AttemptID)
	outcome, resolutionErr := classify(req, result, err)
	switch outcome {
	case OutcomeUnknown:
		return OutcomeUnknown, errors.Join(ErrUnknown, cause, resolutionErr)
	case OutcomeUncommitted:
		return OutcomeUncommitted, errors.Join(resolutionErr, cause)
	default:
		return outcome, resolutionErr
	}
}

// classify turns a backend decision into a client outcome. Malformed replies
// are uncertain, since the backend may already have appended.
func classify(req CommitRequest, result BackendResult, cause error) (Outcome, error) {
	record := result.Record
	switch result.State {
	case BackendUnknown, BackendRolledBack:
		if record != (Record{}) {
			break
		}
		if result.State == BackendRolledBack {
			return OutcomeUncommitted, errors.Join(ErrRolledBack, cause)
		}
		return OutcomeUnknown, cause
	case BackendStored, BackendExisting:
		if cause != nil || !record.ExpiresAt.After(record.CommittedAt) {
			break
		}
		matches := hmac.Equal(record.Fingerprint[:], req.Fingerprint[:])
		if result.State == BackendStored {
			if record.AttemptID != req.AttemptID || !matches || record.PolicyVersion != req.PolicyVersion ||
				!record.ExpiresAt.Equal(record.CommittedAt.Add(req.window)) {
				break
			}
			return OutcomeCommitted, nil
		}
		if !matches {
			return OutcomeRejected, ErrConflict
		}
		return OutcomeReplayed, nil
	}
	return OutcomeUnknown, errors.Join(ErrBackendContract, cause)
}

// Cleanup runs one bounded pass against the backend using a single cutoff
// sampled from the coordinator clock. It returns the count actually removed,
// including partial progress on a backend error. Expiry checks during Commit
// remain mandatory even if Cleanup has never been called.
func (c *Coordinator) Cleanup(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || limit > MaxCleanupBatch {
		return 0, ErrCleanupLimit
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	n, err := c.backend.DeleteExpired(ctx, c.clock().UTC(), limit)
	if n < 0 || n > limit {
		return 0, errors.Join(ErrBackendContract, err)
	}
	return n, err
}
