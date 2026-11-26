package idempotency

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestCleanupBoundAndExactBoundary(t *testing.T) {
	b := openTestBackend(nil)
	clock := newTestClock()
	c := coordinatorFor(t, b, clock)
	expiring := []Request{
		batchRequest(testScope, "one", row),
		batchRequest(testScope, "two", row),
		batchRequest(testScope, "three", row),
	}
	for _, req := range expiring {
		submit(c, t.Context(), req).check(t, OutcomeCommitted, nil)
	}
	clock.set(initialTime.Add(testWindow - time.Nanosecond))
	live := batchRequest(testScope, "live", row)
	submit(c, t.Context(), live).check(t, OutcomeCommitted, nil)
	checkCleanup(t, c, 2, 0)
	clock.set(initialTime.Add(testWindow))
	checkCleanup(t, c, 2, 2)
	checkCleanup(t, c, 2, 1)
	checkCleanup(t, c, 2, 0)

	b = openTestBackend(b.image())
	c = coordinatorFor(t, b, clock)
	for _, req := range expiring {
		if _, exists := b.record(req.Key); exists {
			t.Fatal("cleanup deletion was lost on restart")
		}
	}
	submit(c, t.Context(), live).check(t, OutcomeReplayed, nil)
	if got := len(b.batches()); got != len(expiring)+1 {
		t.Fatalf("cleanup changed published batch count to %d", got)
	}
}

func checkCleanup(t *testing.T, c *Coordinator, limit, want int) {
	t.Helper()
	if n, err := c.Cleanup(t.Context(), limit); n != want || err != nil {
		t.Fatalf("Cleanup = (%d, %v), want (%d, nil)", n, err, want)
	}
}

func TestCleanupCannotDeleteRenewedKey(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := openTestBackend(nil)
		clock := newTestClock()
		c := coordinatorFor(t, b, clock)
		req := batchRequest(testScope, "renewed", row)
		submit(c, t.Context(), req).check(t, OutcomeCommitted, nil)
		clock.set(initialTime.Add(testWindow))

		selected := make(chan struct{})
		apply := make(chan struct{})
		defer close(apply)
		b.beforeCleanup = func() {
			close(selected)
			<-apply
		}
		type cleanupResult struct {
			n   int
			err error
		}
		result := make(chan cleanupResult, 1)
		go func() {
			n, err := c.Cleanup(t.Context(), 1)
			result <- cleanupResult{n, err}
		}()
		<-selected
		renewed := batchRequest(testScope, req.Key.Key, row+row)
		// Always unblock cleanup on a failed assertion.
		s := submit(c, t.Context(), renewed)
		apply <- struct{}{}
		r := <-result
		s.check(t, OutcomeCommitted, nil)
		if r.n != 0 || r.err != nil {
			t.Fatalf("stale cleanup = (%d, %v), want (0, nil)", r.n, r.err)
		}
		submit(c, t.Context(), renewed).check(t, OutcomeReplayed, nil)
		submit(c, t.Context(), req).check(t, OutcomeRejected, ErrConflict)
	})
}

type cleanupBackend struct {
	Backend
	delete func(context.Context, time.Time, int) (int, error)
}

func (b cleanupBackend) DeleteExpired(ctx context.Context, cutoff time.Time, limit int) (int, error) {
	return b.delete(ctx, cutoff, limit)
}

func TestCleanupValidationAndPartialFailure(t *testing.T) {
	b := cleanupBackend{
		Backend: openTestBackend(nil),
		delete: func(_ context.Context, cutoff time.Time, limit int) (int, error) {
			if !cutoff.Equal(initialTime) || limit != 5 {
				t.Fatalf("backend cleanup got cutoff %v and limit %d", cutoff, limit)
			}
			return 2, errInjected
		},
	}
	c := coordinatorFor(t, b, newTestClock())
	for _, limit := range []int{-1, 0, MaxCleanupBatch + 1} {
		if n, err := c.Cleanup(t.Context(), limit); n != 0 || !errors.Is(err, ErrCleanupLimit) {
			t.Fatalf("invalid limit %d: got (%d, %v)", limit, n, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if n, err := c.Cleanup(ctx, 5); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled cleanup = (%d, %v)", n, err)
	}
	if n, err := c.Cleanup(t.Context(), 5); n != 2 || !errors.Is(err, errInjected) {
		t.Fatalf("partial cleanup = (%d, %v)", n, err)
	}
}
