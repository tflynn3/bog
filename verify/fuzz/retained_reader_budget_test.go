//go:build linux || (darwin && cgo)

package fuzz

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

type retainedResource uint8

const (
	retainedBytes retainedResource = iota
	retainedMemory
	retainedDescriptors
	retainedResourceCount
)

func (r retainedResource) String() string {
	return [...]string{"fixture bytes", "memory bytes", "descriptors"}[r]
}

type retainedResources [retainedResourceCount]uint64

var errRetainedBudgetExhausted = errors.New("retained reader budget exhausted")

type retainedBudgetExhaustion struct {
	resource          retainedResource
	limit, used, need uint64
}

func (e *retainedBudgetExhaustion) Error() string {
	return fmt.Sprintf("%s: %s (limit %d, in use %d, requested %d)",
		errRetainedBudgetExhausted, e.resource, e.limit, e.used, e.need)
}

func (e *retainedBudgetExhaustion) Unwrap() error { return errRetainedBudgetExhausted }

type retainedReaderUsage struct {
	inUse, peak    retainedResources
	open, peakOpen uint64
	queued         int
}

// Reserve a worker's entire vector before it touches its fixture. A waiter
// owns no partial charge and an admitted worker never waits for another
// reservation while holding resources. FIFO admission lets older workers
// drain even if smaller requests would fit ahead of them.
type retainedReaderBudget struct {
	mu      sync.Mutex
	limit   retainedResources
	usage   retainedReaderUsage
	queue   list.List
	changed chan struct{}
}

func newRetainedReaderBudget(limit retainedResources) *retainedReaderBudget {
	return &retainedReaderBudget{limit: limit, changed: make(chan struct{})}
}

func (b *retainedReaderBudget) snapshot() (retainedReaderUsage, <-chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.usage, b.changed
}

func (b *retainedReaderBudget) notifyLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}

func (b *retainedReaderBudget) removeWaiterLocked(entry *list.Element) {
	b.queue.Remove(entry)
	b.usage.queued--
	b.notifyLocked()
}

func (b *retainedReaderBudget) reserve(ctx context.Context, need retainedResources, wait bool) (*retainedReaderReservation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for r, amount := range need {
		if amount > b.limit[r] {
			return nil, &retainedBudgetExhaustion{retainedResource(r), b.limit[r], b.usage.inUse[r], amount}
		}
	}

	var entry *list.Element
	for {
		if err := ctx.Err(); err != nil {
			if entry != nil {
				b.removeWaiterLocked(entry)
			}
			return nil, err
		}
		var blocked retainedBudgetExhaustion
		for r, amount := range need {
			used := b.usage.inUse[r]
			if amount > b.limit[r]-used {
				blocked = retainedBudgetExhaustion{retainedResource(r), b.limit[r], used, amount}
				break
			}
		}
		if blocked.need == 0 && b.queue.Front() == entry {
			for r, amount := range need {
				b.usage.inUse[r] += amount
				b.usage.peak[r] = max(b.usage.peak[r], b.usage.inUse[r])
			}
			if entry != nil {
				b.removeWaiterLocked(entry)
			} else {
				b.notifyLocked()
			}
			return &retainedReaderReservation{budget: b, charge: need}, nil
		}
		if !wait {
			if blocked.need != 0 {
				return nil, &blocked
			}
			return nil, fmt.Errorf("%w: earlier reservation is queued", errRetainedBudgetExhausted)
		}
		if entry == nil {
			entry = b.queue.PushBack(nil)
			b.usage.queued++
			b.notifyLocked()
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		b.mu.Lock()
	}
}

type retainedReaderReservation struct {
	budget   *retainedReaderBudget
	charge   retainedResources
	open     uint64 // guarded by budget.mu
	released bool
}

func (r *retainedReaderReservation) release() error {
	b := r.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if r.released {
		return nil
	}
	if r.open != 0 {
		return fmt.Errorf("retain charge with %d open fixture descriptors", r.open)
	}
	for resource, amount := range r.charge {
		b.usage.inUse[resource] -= amount
	}
	r.released = true
	b.notifyLocked()
	return nil
}

func (r *retainedReaderReservation) acquireDescriptor(ctx context.Context, privateFDs uint64) error {
	b := r.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.released {
		return errors.New("retained reader reservation is released")
	}
	reserved := r.charge[retainedDescriptors]
	limit := reserved - min(reserved, privateFDs)
	if r.open >= limit {
		return &retainedBudgetExhaustion{retainedDescriptors, limit, r.open, 1}
	}
	r.open++
	b.usage.open++
	b.usage.peakOpen = max(b.usage.peakOpen, b.usage.open)
	return nil
}

func (r *retainedReaderReservation) releaseDescriptor() {
	b := r.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	r.open--
	b.usage.open--
}

// Track harness-owned opens separately from the allowance for synchronous
// storage helpers. The returned closer releases its slot after Close, even
// when Close reports an error. Callers retain the worker's vector throughout.
func openRetainedHandle[T io.Closer](ctx context.Context, r *retainedReaderReservation, privateFDs uint64,
	open func() (T, error)) (file T, closeFile func() error, err error) {
	if err := ctx.Err(); err != nil {
		return file, nil, err
	}
	if r != nil {
		if err := r.acquireDescriptor(ctx, privateFDs); err != nil {
			return file, nil, err
		}
	}
	file, err = open()
	if err != nil {
		if r != nil {
			r.releaseDescriptor()
		}
		return file, nil, err
	}
	closeFile = sync.OnceValue(func() error {
		if r != nil {
			defer r.releaseDescriptor()
		}
		return file.Close()
	})
	if err := ctx.Err(); err != nil {
		var zero T
		return zero, nil, errors.Join(err, closeFile())
	}
	return file, closeFile, nil
}
