package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Resource identifies the unit charged by a Budget.
type Resource uint8

const (
	StagingBytes Resource = iota
	ReplayTailBytes
	PinnedGenerations
	LiveCaptures
	CaptureBytes
	resourceCount
)

func (r Resource) String() string {
	switch r {
	case StagingBytes:
		return "staging bytes"
	case ReplayTailBytes:
		return "replay tail bytes"
	case PinnedGenerations:
		return "pinned generations"
	case LiveCaptures:
		return "live captures"
	case CaptureBytes:
		return "capture bytes"
	default:
		return "unknown resource"
	}
}

// ErrExhausted indicates that admission failed without consuming resources.
var ErrExhausted = errors.New("retained history resource exhausted")

// ExhaustedError records the rejected request and the usage at admission.
type ExhaustedError struct {
	Resource Resource
	Limit    uint64
	InUse    uint64
	Need     uint64
}

func (e *ExhaustedError) Error() string {
	return fmt.Sprintf("%s: %s: need %d, in use %d, limit %d",
		ErrExhausted, e.Resource, e.Need, e.InUse, e.Limit)
}

func (e *ExhaustedError) Unwrap() error { return ErrExhausted }

// Limits are inclusive. Zero disables admission for positive requests.
type Limits [resourceCount]uint64

// Budget measures and limits resource use across one writable owner. Share a
// single instance between the publisher, catalog and replay/append path.
type Budget struct {
	mu    sync.Mutex
	limit Limits
	inUse Limits
	peak  Limits
}

func NewBudget(limits Limits) *Budget {
	return &Budget{limit: limits}
}

// Usage is a consistent sample. Peak is the high-water mark since creation.
type Usage struct {
	Limit Limits
	InUse Limits
	Peak  Limits
}

func (b *Budget) Usage() Usage {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Usage{Limit: b.limit, InUse: b.inUse, Peak: b.peak}
}

// Reserve admits a charge immediately or returns an ExhaustedError. Release the
// charge after the associated resource is reclaimed. A successful reservation
// may outlive ctx. The caller owns cleanup if subsequent work is cancelled.
func (b *Budget) Reserve(ctx context.Context, r Resource, units uint64) (*Reservation, error) {
	if r >= resourceCount {
		return nil, errors.New("invalid retained history resource")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if units > b.limit[r]-b.inUse[r] {
		return nil, &ExhaustedError{Resource: r, Limit: b.limit[r], InUse: b.inUse[r], Need: units}
	}
	b.inUse[r] += units
	b.peak[r] = max(b.peak[r], b.inUse[r])
	return &Reservation{charge: &charge{budget: b, resource: r, units: units, refs: 1}}, nil
}

// ReserveReplayTail charges the encoded bytes of a whole-frame logical WAL
// range, including frame headers. It must succeed before reading or appending
// the range. Transfer the reservation with the backing data on commit.
func (b *Budget) ReserveReplayTail(ctx context.Context, start, end int64) (*Reservation, error) {
	if start < 0 || end < start {
		return nil, errors.New("invalid replay tail range")
	}
	return b.Reserve(ctx, ReplayTailBytes, uint64(end-start))
}

type charge struct {
	budget   *Budget
	resource Resource
	units    uint64
	refs     uint64 // Protected by budget.mu.
}

// Reservation owns one reference to a charge. It must be used by pointer.
// Independent references share the charge until the final Release.
type Reservation struct {
	mu     sync.Mutex
	charge *charge
}

// split transfers part of a private replay reservation to an independent batch.
func (r *Reservation) split(units uint64) *Reservation {
	c := r.charge // The replay reservation has no other users.
	c.units -= units
	return &Reservation{charge: &charge{budget: c.budget, resource: c.resource, units: units, refs: 1}}
}

// Retain takes an independent reference, or returns nil if released. Use this
// to share immutable backing data with a capture at no additional byte cost.
func (r *Reservation) Retain() *Reservation {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.charge
	if c == nil {
		return nil
	}
	c.budget.mu.Lock()
	defer c.budget.mu.Unlock()
	c.refs++
	return &Reservation{charge: c}
}

// Release is idempotent and safe to race with Retain.
func (r *Reservation) Release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.charge
	if c == nil {
		return
	}
	r.charge = nil
	c.budget.mu.Lock()
	defer c.budget.mu.Unlock()
	c.refs--
	if c.refs == 0 {
		c.budget.inUse[c.resource] -= c.units
	}
}
