package idempotency

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

var (
	initialTime = time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	testScope   = Scope{TenantID: "tenant", StreamID: "stream", PrincipalID: "principal"}
	testSecret  = [SecretBytes]byte{1}
)

const testWindow = time.Hour

type testClock struct {
	unixNano atomic.Int64
}

func newTestClock() *testClock {
	c := &testClock{}
	c.set(initialTime)
	return c
}

func (c *testClock) set(t time.Time) { c.unixNano.Store(t.UnixNano()) }
func (c *testClock) now() time.Time  { return time.Unix(0, c.unixNano.Load()).UTC() }

func coordinatorFor(t *testing.T, backend Backend, clock *testClock) *Coordinator {
	t.Helper()
	c, err := New(backend, Config{Clock: clock.now, RetryWindow: testWindow, FingerprintSecret: testSecret})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func batchRequest(scope Scope, key, body string) Request {
	b := []byte(body)
	return Request{Key: ScopedKey{Scope: scope, Key: key}, Original: b, Batch: b}
}

const row = "{\"ts\":\"2026-01-02T03:04:05Z\",\"line\":\"A\"}\n"

type submission struct {
	outcome Outcome
	err     error
}

func submit(c *Coordinator, ctx context.Context, req Request) submission {
	outcome, err := c.Submit(ctx, req)
	return submission{outcome, err}
}

func (s submission) check(t *testing.T, outcome Outcome, err error) {
	t.Helper()
	if s.outcome != outcome || !errors.Is(s.err, err) {
		t.Fatalf("submission = (%v, %v), want (%v, %v)", s.outcome, s.err, outcome, err)
	}
}

func TestConcurrentSameKeyCommitsOneBatch(t *testing.T) {
	for _, sharedCoordinator := range []bool{true, false} {
		name := "one coordinator"
		if !sharedCoordinator {
			name = "two coordinators"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := openTestBackend(nil)
				clock := newTestClock()
				first := coordinatorFor(t, b, clock)
				other := first
				if !sharedCoordinator {
					other = coordinatorFor(t, b, clock)
				}
				entered := make(chan struct{})
				startCommit := make(chan struct{})
				var calls atomic.Int32
				b.beforeCommit = func(_ context.Context, _ CommitRequest) commitFault {
					if calls.Add(1) == 1 {
						close(entered)
						<-startCommit
					}
					return noFault
				}
				req := batchRequest(testScope, "concurrent", row+row)
				results := make(chan submission, 32)
				go func() { results <- submit(first, t.Context(), req) }()
				<-entered
				for range cap(results) - 1 {
					go func() { results <- submit(other, t.Context(), req) }()
				}
				synctest.Wait()
				if sharedCoordinator && calls.Load() != 1 {
					t.Errorf("calls before releasing first attempt = %d, want 1", calls.Load())
				}
				close(startCommit)
				commits := 0
				for range cap(results) {
					s := <-results
					if s.outcome == OutcomeCommitted {
						s.check(t, OutcomeCommitted, nil)
						commits++
					} else {
						s.check(t, OutcomeReplayed, nil)
					}
				}
				batches := b.batches()
				if commits != 1 || len(batches) != 1 || !bytes.Equal(batches[0], req.Batch) {
					t.Fatalf("committed outcomes = %d, published batches = %q", commits, batches)
				}
			})
		})
	}
}

func TestSeparateKeysAndScopesProceedWhileFirstKeyIsBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := openTestBackend(nil)
		c := coordinatorFor(t, b, newTestClock())
		leader := batchRequest(testScope, "same", row)
		entered := make(chan struct{})
		release := make(chan struct{})
		defer close(release)
		b.beforeCommit = func(_ context.Context, req CommitRequest) commitFault {
			if req.Key == leader.Key {
				close(entered)
				<-release
			}
			return noFault
		}
		done := make(chan submission, 1)
		go func() { done <- submit(c, t.Context(), leader) }()
		<-entered
		submit(c, t.Context(), batchRequest(testScope, "different-key", row)).check(t, OutcomeCommitted, nil)
		for _, scope := range []Scope{
			{TenantID: "other", StreamID: testScope.StreamID, PrincipalID: testScope.PrincipalID},
			{TenantID: testScope.TenantID, StreamID: "other", PrincipalID: testScope.PrincipalID},
			{TenantID: testScope.TenantID, StreamID: testScope.StreamID, PrincipalID: "other"},
		} {
			req := batchRequest(scope, leader.Key.Key, row+"\n")
			submit(c, t.Context(), req).check(t, OutcomeCommitted, nil)
			submit(c, t.Context(), req).check(t, OutcomeReplayed, nil)
		}
		if got := len(b.batches()); got != 4 {
			t.Fatalf("independent commits = %d, want 4", got)
		}
	})
}

func TestWaitingRequestCanCancelWithoutDispatchOrGateLeak(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := openTestBackend(nil)
		c := coordinatorFor(t, b, newTestClock())
		entered := make(chan struct{})
		release := make(chan struct{})
		var calls atomic.Int32
		b.beforeCommit = func(context.Context, CommitRequest) commitFault {
			if calls.Add(1) == 1 {
				close(entered)
				<-release
			}
			return noFault
		}
		req := batchRequest(testScope, "pending", row)
		leader := make(chan submission, 1)
		go func() { leader <- submit(c, t.Context(), req) }()
		<-entered

		ctx, cancel := context.WithCancel(t.Context())
		waiter := make(chan submission, 1)
		go func() { waiter <- submit(c, ctx, req) }()
		synctest.Wait()
		cancel()
		s := <-waiter
		s.check(t, OutcomeUncommitted, context.Canceled)
		if calls.Load() != 1 {
			t.Errorf("canceled waiter reached backend")
		}
		close(release)
		(<-leader).check(t, OutcomeCommitted, nil)
		submit(c, t.Context(), req).check(t, OutcomeReplayed, nil)
		if n := len(c.gate.entries); n != 0 {
			t.Fatalf("gate retained %d entries", n)
		}
	})
}

func TestConflictUsesExactOriginalBytes(t *testing.T) {
	for _, body := range []string{" " + row, row[:len(row)-1], row + row, bytesChanged(row)} {
		b := openTestBackend(nil)
		c := coordinatorFor(t, b, newTestClock())
		req := batchRequest(testScope, "key", row)
		submit(c, t.Context(), req).check(t, OutcomeCommitted, nil)
		changed := req
		changed.Original = []byte(body)
		submit(c, t.Context(), changed).check(t, OutcomeRejected, ErrConflict)
		submit(c, t.Context(), req).check(t, OutcomeReplayed, nil)
		if len(b.batches()) != 1 {
			t.Fatal("a conflict published a second batch")
		}
	}
}

func bytesChanged(input string) string {
	return string(bytes.ReplaceAll([]byte(input), []byte(`"A"`), []byte(`"\u0041"`)))
}

func TestLostResponseAndRestart(t *testing.T) {
	clock := newTestClock()
	b := openTestBackend(nil)
	b.beforeCommit = func(context.Context, CommitRequest) commitFault { return lostCommitReply }
	ctx, disconnect := context.WithCancel(t.Context())
	defer disconnect()
	b.afterCommit = disconnect

	req := batchRequest(testScope, "disconnect", row)
	s := submit(coordinatorFor(t, b, clock), ctx, req)
	s.check(t, OutcomeUnknown, ErrUnknown)
	if !errors.Is(s.err, context.Canceled) || !errors.Is(s.err, errInjected) {
		t.Fatalf("lost response causes = %v", s.err)
	}
	submit(coordinatorFor(t, b, clock), t.Context(), req).check(t, OutcomeReplayed, nil)

	// Recover only the committed image and a newly constructed coordinator.
	b = openTestBackend(b.image())
	restarted := coordinatorFor(t, b, clock)
	submit(restarted, t.Context(), req).check(t, OutcomeReplayed, nil)
	submit(restarted, t.Context(), batchRequest(testScope, req.Key.Key, row+row)).check(t, OutcomeRejected, ErrConflict)
	if batches := b.batches(); len(batches) != 1 || string(batches[0]) != row {
		t.Fatalf("recovered batches = %q, want one original batch", batches)
	}
}

func TestAuthoritativeResolution(t *testing.T) {
	tests := []struct {
		name    string
		fault   commitFault
		outcome Outcome
		err     error
		count   int
	}{
		{"confirmed rollback", rollback, OutcomeUncommitted, ErrRolledBack, 0},
		{"resolved rollback", uncertainRollback, OutcomeUncommitted, ErrRolledBack, 0},
		{"resolved commit", lostCommitReply, OutcomeCommitted, nil, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := openTestBackend(nil)
			var attempts int
			b.beforeCommit = func(context.Context, CommitRequest) commitFault {
				attempts++
				if attempts == 1 {
					return tt.fault
				}
				return noFault
			}
			c := coordinatorFor(t, b, newTestClock())
			req := batchRequest(testScope, "attempt", row)
			s := submit(c, t.Context(), req)
			s.check(t, tt.outcome, tt.err)
			if tt.err != nil && !errors.Is(s.err, errInjected) {
				t.Fatalf("backend cause absent from %v", s.err)
			}
			if n := len(b.batches()); n != tt.count {
				t.Fatalf("after first attempt: %d batches, want %d", n, tt.count)
			}
			if tt.count == 0 {
				submit(c, t.Context(), req).check(t, OutcomeCommitted, nil)
			}
			submit(c, t.Context(), req).check(t, OutcomeReplayed, nil)
			if n := len(b.batches()); n != 1 {
				t.Fatalf("after retry: %d batches, want 1", n)
			}
		})
	}
}

func TestUnknownOutcomeFencesKeyUntilRecovery(t *testing.T) {
	b := openTestBackend(nil)
	req := batchRequest(testScope, "fenced", row)
	b.beforeCommit = func(_ context.Context, r CommitRequest) commitFault {
		if r.Key == req.Key {
			return unresolved
		}
		return noFault
	}
	clock := newTestClock()
	c := coordinatorFor(t, b, clock)
	submit(c, t.Context(), req).check(t, OutcomeUnknown, ErrUnknown)
	submit(coordinatorFor(t, b, clock), t.Context(), req).check(t, OutcomeUnknown, ErrUnknown)
	submit(c, t.Context(), batchRequest(testScope, "independent", row)).check(t, OutcomeCommitted, nil)
	if got := len(b.batches()); got != 1 {
		t.Fatalf("published batches while fenced = %d, want 1", got)
	}

	// Restart recovers only committed state and definitively discards the
	// uncommitted attempt before admitting writes.
	b = openTestBackend(b.image())
	submit(coordinatorFor(t, b, clock), t.Context(), req).check(t, OutcomeCommitted, nil)
	if got := len(b.batches()); got != 2 {
		t.Fatalf("published batches after recovery = %d, want 2", got)
	}
}

func TestRedactedPayloadAndPolicyChange(t *testing.T) {
	b := openTestBackend(nil)
	clock := newTestClock()
	req := batchRequest(testScope, "redacted", row)
	req.Batch = bytes.ReplaceAll(req.Original, []byte(`"A"`), []byte(`"[redacted]"`))
	req.PolicyVersion = "v1"
	submit(coordinatorFor(t, b, clock), t.Context(), req).check(t, OutcomeCommitted, nil)

	b = openTestBackend(b.image())
	c := coordinatorFor(t, b, clock)
	retry := req
	retry.PolicyVersion = "v2"
	retry.Batch = bytes.ReplaceAll(req.Original, []byte(`"A"`), []byte(`"[removed]"`))
	submit(c, t.Context(), retry).check(t, OutcomeReplayed, nil)
	record, exists := b.record(req.Key)
	if !exists || record.PolicyVersion != "v1" {
		t.Fatalf("stored policy = %q, want v1", record.PolicyVersion)
	}
	if batches := b.batches(); len(batches) != 1 || !bytes.Equal(batches[0], req.Batch) {
		t.Fatalf("durable batches = %q, want only original prepared batch", batches)
	}
}

func TestExpiryStartsAtCommitAndDoesNotSlide(t *testing.T) {
	for _, changeBody := range []bool{false, true} {
		clock := newTestClock()
		b := openTestBackend(nil)
		c := coordinatorFor(t, b, clock)
		started := clock.now()
		committed := started.Add(2 * testWindow)
		b.beforeCommit = func(context.Context, CommitRequest) commitFault {
			if clock.now().Equal(started) {
				clock.set(committed)
			}
			return noFault
		}
		req := batchRequest(testScope, "expiry", row)
		submit(c, t.Context(), req).check(t, OutcomeCommitted, nil)
		record, exists := b.record(req.Key)
		deadline := committed.Add(testWindow)
		if !exists || !record.CommittedAt.Equal(committed) || !record.ExpiresAt.Equal(deadline) {
			t.Fatalf("commit time = %v, expiry = %v", record.CommittedAt, record.ExpiresAt)
		}
		clock.set(deadline.Add(-time.Nanosecond))
		submit(c, t.Context(), req).check(t, OutcomeReplayed, nil)
		boundary := req
		if changeBody {
			boundary = batchRequest(testScope, req.Key.Key, row+row)
			submit(c, t.Context(), boundary).check(t, OutcomeRejected, ErrConflict)
		}
		clock.set(deadline)
		submit(c, t.Context(), boundary).check(t, OutcomeCommitted, nil)
		renewed, exists := b.record(req.Key)
		if !exists || renewed.AttemptID == record.AttemptID || !renewed.ExpiresAt.Equal(deadline.Add(testWindow)) {
			t.Fatalf("new generation was not installed at the exact deadline: %+v", renewed)
		}
		if got := len(b.batches()); got != 2 {
			t.Fatalf("published batches at expiry = %d, want 2", got)
		}
	}
}

func TestConcurrentDifferentFingerprintConflicts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := openTestBackend(nil)
		c := coordinatorFor(t, b, newTestClock())
		first := batchRequest(testScope, "conflicting", row)
		second := batchRequest(testScope, first.Key.Key, row+row)
		entered := make(chan struct{})
		release := make(chan struct{})
		b.beforeCommit = func(_ context.Context, req CommitRequest) commitFault {
			if bytes.Equal(req.Batch, first.Batch) {
				close(entered)
				<-release
			}
			return noFault
		}
		winner, loser := make(chan submission, 1), make(chan submission, 1)
		go func() { winner <- submit(c, t.Context(), first) }()
		<-entered
		go func() { loser <- submit(c, t.Context(), second) }()
		synctest.Wait()
		close(release)
		(<-winner).check(t, OutcomeCommitted, nil)
		(<-loser).check(t, OutcomeRejected, ErrConflict)
		if batches := b.batches(); len(batches) != 1 || !bytes.Equal(batches[0], first.Batch) {
			t.Fatalf("published batches = %q, want only the first batch", batches)
		}
	})
}

func TestClassifyMalformedBackendDecisions(t *testing.T) {
	req := CommitRequest{
		AttemptID:   AttemptID{1},
		Fingerprint: Fingerprint{1},
		window:      testWindow,
	}
	record := req.RecordAt(initialTime)
	wrongAttempt := record
	wrongAttempt.AttemptID = AttemptID{2}
	badDeadline := record
	badDeadline.ExpiresAt = badDeadline.CommittedAt
	wrongFingerprint := record
	wrongFingerprint.Fingerprint = Fingerprint{2}
	for _, tt := range []struct {
		name   string
		result BackendResult
		cause  error
	}{
		{"unknown state", BackendResult{State: BackendState(255)}, nil},
		{"error with commit", BackendResult{State: BackendStored, Record: record}, errInjected},
		{"rollback with record", BackendResult{State: BackendRolledBack, Record: record}, nil},
		{"wrong attempt", BackendResult{State: BackendStored, Record: wrongAttempt}, nil},
		{"invalid expiry", BackendResult{State: BackendExisting, Record: badDeadline}, nil},
		{"mismatched fingerprint on commit", BackendResult{State: BackendStored, Record: wrongFingerprint}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			outcome, err := classify(req, tt.result, tt.cause)
			submission{outcome, err}.check(t, OutcomeUnknown, ErrBackendContract)
			if tt.cause != nil && !errors.Is(err, tt.cause) {
				t.Fatalf("missing underlying backend cause: %v", err)
			}
		})
	}
}
