//go:build linux || (darwin && cgo)

package fuzz

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"bog/internal/backup"
	"bog/internal/checkpoint"
)

type retainedReadBarrier struct {
	arrival chan struct{}
	resume  chan struct{}
}

func pauseRetainedReader(w retainedReaderWorker, read int) (retainedReaderWorker, retainedReadBarrier) {
	b := retainedReadBarrier{arrival: make(chan struct{}), resume: make(chan struct{})}
	original := w.beforeRead
	var reads atomic.Int64
	w.beforeRead = func(ctx context.Context) error {
		if reads.Add(1) == int64(read) {
			close(b.arrival)
			select {
			case <-b.resume:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if original != nil {
			return original(ctx)
		}
		return nil
	}
	return w, b
}

func (b retainedReadBarrier) await(t *testing.T, ctx context.Context, done <-chan error) {
	t.Helper()
	select {
	case <-b.arrival:
	case err := <-done:
		t.Fatalf("worker returned before read barrier: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

type retainedWorkerGroup struct {
	ctx    context.Context
	parent string
	wg     sync.WaitGroup
}

func newRetainedWorkerGroup(t *testing.T) *retainedWorkerGroup {
	ctx, cancel := context.WithTimeout(t.Context(), replayDeadline)
	g := &retainedWorkerGroup{ctx: ctx, parent: t.TempDir()}
	t.Cleanup(func() {
		cancel()
		g.wg.Wait()
	})
	return g
}

func (g *retainedWorkerGroup) start(run func() error) <-chan error {
	done := make(chan error, 1)
	g.wg.Go(func() { done <- run() })
	return done
}

func (g *retainedWorkerGroup) startReader(ctx context.Context, w retainedReaderWorker, budget *retainedReaderBudget, f retainedFixture) <-chan error {
	return g.start(func() error { return w.run(ctx, g.parent, budget, f, true) })
}

func awaitRetainedResult(t *testing.T, ctx context.Context, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return nil
	}
}

func awaitRetainedQueue(t *testing.T, ctx context.Context, b *retainedReaderBudget, count int) retainedReaderUsage {
	t.Helper()
	for {
		usage, changed := b.snapshot()
		if usage.queued == count {
			return usage
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatalf("waiting for %d queued workers with usage %+v: %v", count, usage, ctx.Err())
		}
	}
}

func workerResources(t *testing.T, kind retainedReaderKind) retainedResources {
	t.Helper()
	need, err := kind.resources()
	requireRetainedOK(t, err)
	return need
}

func addRetainedResources(a, b retainedResources) retainedResources {
	for i, n := range b {
		a[i] += n
	}
	return a
}

func assertRetainedWorkerBaseline(t *testing.T, parent string, b *retainedReaderBudget, baseline []string) {
	t.Helper()
	usage, _ := b.snapshot()
	if usage.inUse != (retainedResources{}) || usage.open != 0 || usage.queued != 0 {
		t.Fatalf("worker reservations did not return to baseline: %+v", usage)
	}
	for r, peak := range usage.peak {
		if peak > b.limit[r] {
			t.Fatalf("%s peak %d exceeds %d", retainedResource(r), peak, b.limit[r])
		}
	}
	if usage.peakOpen > usage.peak[retainedDescriptors] {
		t.Fatalf("descriptor peak escaped reservation: %+v", usage)
	}
	requireRetainedOK(t, checkRetainedReaderFDs(parent, baseline))
	entries, err := os.ReadDir(parent)
	requireRetainedOK(t, err)
	if len(entries) != 0 {
		t.Fatalf("worker fixtures were left behind: %v", entries)
	}
}

func requireRetainedExhaustion(t *testing.T, err error, resource retainedResource, limit, inUse, need uint64) {
	t.Helper()
	var detail *retainedBudgetExhaustion
	if !errors.Is(err, errRetainedBudgetExhausted) || !errors.As(err, &detail) ||
		*detail != (retainedBudgetExhaustion{resource, limit, inUse, need}) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, backup.ErrIntegrity) || errors.Is(err, checkpoint.ErrIntegrity) {
		t.Fatalf("wanted clean %s exhaustion (limit %d, used %d, need %d), got %v", resource, limit, inUse, need, err)
	}
}

func TestRetainedRecoveryAggregateBudget(t *testing.T) {
	f, err := seedRetainedFixture()
	requireRetainedOK(t, err)
	checkpointNeed := workerResources(t, retainedCheckpointReader)
	backupNeed := workerResources(t, retainedBackupReader)
	tailNeed := workerResources(t, retainedWALReader)
	active := addRetainedResources(checkpointNeed, backupNeed)

	for resource := range retainedResourceCount {
		t.Run(resource.String(), func(t *testing.T) {
			g := newRetainedWorkerGroup(t)
			baseline, err := retainedReaderFDs(g.parent)
			requireRetainedOK(t, err)
			limit := addRetainedResources(active, tailNeed)
			limit[resource] = active[resource]
			b := newRetainedReaderBudget(limit)

			// Each worker completes against this exact limit in isolation.
			for _, kind := range []retainedReaderKind{retainedCheckpointReader, retainedBackupReader, retainedWALReader} {
				requireRetainedOK(t, (retainedReaderWorker{kind: kind}).run(g.ctx, g.parent, b, f, false))
				assertRetainedWorkerBaseline(t, g.parent, b, baseline)
			}

			checkpointWorker, checkpointRead := pauseRetainedReader(retainedReaderWorker{kind: retainedCheckpointReader}, 1)
			backupWorker, backupRead := pauseRetainedReader(retainedReaderWorker{kind: retainedBackupReader}, 1)
			tailWorker, tailRead := pauseRetainedReader(retainedReaderWorker{kind: retainedWALReader}, 1)
			checkpointDone := g.startReader(g.ctx, checkpointWorker, b, f)
			backupDone := g.startReader(g.ctx, backupWorker, b, f)
			checkpointRead.await(t, g.ctx, checkpointDone)
			backupRead.await(t, g.ctx, backupDone)
			activeFDs, err := retainedReaderFDs(g.parent)
			requireRetainedOK(t, err)
			if uint64(len(activeFDs)) > active[retainedDescriptors] {
				t.Fatalf("active fixture descriptors %d exceed the reserved %d", len(activeFDs), active[retainedDescriptors])
			}

			err = (retainedReaderWorker{kind: retainedWALReader}).run(g.ctx, g.parent, b, f, false)
			requireRetainedExhaustion(t, err, resource, limit[resource], active[resource], tailNeed[resource])
			tailDone := g.startReader(g.ctx, tailWorker, b, f)
			usage := awaitRetainedQueue(t, g.ctx, b, 1)
			if usage.inUse != active || usage.open != 6 || usage.peak[resource] != limit[resource] {
				t.Fatalf("two live workers and one queued: %+v", usage)
			}
			requireRetainedOK(t, checkRetainedReaderFDs(g.parent, activeFDs))
			entries, err := os.ReadDir(g.parent)
			requireRetainedOK(t, err)
			if len(entries) != 2 {
				t.Fatalf("queued or rejected worker created scratch space: %v", entries)
			}

			close(checkpointRead.resume)
			requireRetainedOK(t, awaitRetainedResult(t, g.ctx, checkpointDone))
			tailRead.await(t, g.ctx, tailDone)
			usage, _ = b.snapshot()
			if usage.inUse != addRetainedResources(backupNeed, tailNeed) || usage.open != 4 || usage.queued != 0 {
				t.Fatalf("WAL did not take the released charge: %+v", usage)
			}
			close(backupRead.resume)
			close(tailRead.resume)
			requireRetainedOK(t, awaitRetainedResult(t, g.ctx, backupDone))
			requireRetainedOK(t, awaitRetainedResult(t, g.ctx, tailDone))
			assertRetainedWorkerBaseline(t, g.parent, b, baseline)
		})
	}
}

func TestRetainedRecoveryAggregateCancellation(t *testing.T) {
	f, err := seedRetainedFixture()
	requireRetainedOK(t, err)
	queueWorker := retainedReaderWorker{kind: retainedWALReader}
	queueNeed := workerResources(t, queueWorker.kind)
	for _, kind := range []retainedReaderKind{retainedCheckpointReader, retainedBackupReader, retainedWALReader} {
		t.Run(string(kind), func(t *testing.T) {
			g := newRetainedWorkerGroup(t)
			baseline, err := retainedReaderFDs(g.parent)
			requireRetainedOK(t, err)
			need := workerResources(t, kind)
			limit := addRetainedResources(need, queueNeed)
			limit[retainedMemory] = need[retainedMemory]
			b := newRetainedReaderBudget(limit)
			activeCtx, cancelActive := context.WithCancel(g.ctx)
			defer cancelActive()
			pauseAt := 1
			if kind == retainedWALReader {
				// Pass the ledger's initial byte comparison and cancel a
				// frame read while ReplayCapturedTail holds its reservation.
				pauseAt = 2
			}
			worker, read := pauseRetainedReader(retainedReaderWorker{kind: kind}, pauseAt)
			active := g.startReader(activeCtx, worker, b, f)
			read.await(t, g.ctx, active)
			activeFDs, err := retainedReaderFDs(g.parent)
			requireRetainedOK(t, err)
			before, _ := b.snapshot()

			queuedCtx, cancelQueued := context.WithCancel(g.ctx)
			defer cancelQueued()
			queued := g.startReader(queuedCtx, queueWorker, b, f)
			awaitRetainedQueue(t, g.ctx, b, 1)
			cancelQueued()
			err = awaitRetainedResult(t, g.ctx, queued)
			requireRetainedOK(t, checkRetainedReaderRejection(err, context.Canceled))
			if errors.Is(err, errRetainedBudgetExhausted) {
				t.Fatalf("queued cancellation reported as exhaustion: %v", err)
			}
			after, _ := b.snapshot()
			if after != before {
				t.Fatalf("queued cancellation changed active usage from %+v to %+v", before, after)
			}
			requireRetainedOK(t, checkRetainedReaderFDs(g.parent, activeFDs))

			cancelActive()
			err = awaitRetainedResult(t, g.ctx, active)
			requireRetainedOK(t, checkRetainedReaderRejection(err, context.Canceled))
			if errors.Is(err, errRetainedBudgetExhausted) {
				t.Fatalf("active cancellation reported as exhaustion: %v", err)
			}
			assertRetainedWorkerBaseline(t, g.parent, b, baseline)
			requireRetainedOK(t, queueWorker.run(g.ctx, g.parent, b, f, false))
			assertRetainedWorkerBaseline(t, g.parent, b, baseline)
		})
	}
}

func TestRetainedRecoveryAggregateOverlap(t *testing.T) {
	f, err := seedRetainedFixture()
	requireRetainedOK(t, err)
	g := newRetainedWorkerGroup(t)
	baseline, err := retainedReaderFDs(g.parent)
	requireRetainedOK(t, err)

	kinds := [...]retainedReaderKind{retainedCheckpointReader, retainedBackupReader, retainedWALReader}
	var total retainedResources
	for _, kind := range kinds {
		total = addRetainedResources(total, workerResources(t, kind))
	}
	b := newRetainedReaderBudget(total)
	var barriers [len(kinds)]retainedReadBarrier
	var results [len(kinds)]<-chan error
	for i, kind := range kinds {
		worker, barrier := pauseRetainedReader(retainedReaderWorker{kind: kind}, 1)
		barriers[i] = barrier
		results[i] = g.startReader(g.ctx, worker, b, f)
	}
	for i, barrier := range barriers {
		barrier.await(t, g.ctx, results[i])
	}
	usage, _ := b.snapshot()
	if usage.inUse != total || usage.peak != total || usage.open != 8 || usage.queued != 0 {
		t.Fatalf("all three workers did not overlap at the aggregate limit: %+v", usage)
	}
	for _, barrier := range barriers {
		close(barrier.resume)
	}
	for _, done := range results {
		requireRetainedOK(t, awaitRetainedResult(t, g.ctx, done))
	}
	assertRetainedWorkerBaseline(t, g.parent, b, baseline)
}

func TestRetainedRecoveryAggregateWorkerFailures(t *testing.T) {
	f, err := seedRetainedFixture()
	requireRetainedOK(t, err)
	cases := retainedReaderCases()
	fault := func(name string) retainedReaderCase {
		for _, tc := range cases {
			if tc.name == name {
				return tc
			}
		}
		t.Fatalf("missing retained test fault %q", name)
		return retainedReaderCase{}
	}
	workerFailure := errors.New("injected worker read failure")
	for _, tc := range []struct {
		name   string
		worker retainedReaderWorker
		want   error
	}{
		{"checkpoint corruption", retainedReaderWorker{kind: retainedCheckpointReader, fault: fault("truncated-manifest")}, checkpoint.ErrIntegrity},
		{"backup corruption", retainedReaderWorker{kind: retainedBackupReader, fault: fault("second-frame-checksum")}, backup.ErrIntegrity},
		{"WAL read failure", retainedReaderWorker{kind: retainedWALReader, beforeRead: func(context.Context) error { return workerFailure }}, workerFailure},
		{"backup close failure", retainedReaderWorker{kind: retainedBackupReader, fault: fault("suffix-close-error")}, errRetainedReaderClose},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newRetainedWorkerGroup(t)
			baseline, err := retainedReaderFDs(g.parent)
			requireRetainedOK(t, err)
			need := workerResources(t, tc.worker.kind)
			b := newRetainedReaderBudget(need)
			blocker, err := b.reserve(g.ctx, need, false)
			requireRetainedOK(t, err)
			defer func() { requireRetainedOK(t, blocker.release()) }()
			err = tc.worker.run(g.ctx, g.parent, b, f, false)
			requireRetainedExhaustion(t, err, retainedBytes, need[retainedBytes], need[retainedBytes], need[retainedBytes])
			requireRetainedOK(t, blocker.release())
			err = tc.worker.run(g.ctx, g.parent, b, f, false)
			if !errors.Is(err, tc.want) || errors.Is(err, errRetainedBudgetExhausted) {
				t.Fatalf("worker returned %v, want %v independently of budget exhaustion", err, tc.want)
			}
			assertRetainedWorkerBaseline(t, g.parent, b, baseline)
			requireRetainedOK(t, (retainedReaderWorker{kind: tc.worker.kind}).run(g.ctx, g.parent, b, f, false))
			assertRetainedWorkerBaseline(t, g.parent, b, baseline)
		})
	}
}

func TestRetainedRecoveryAggregatePartialOpen(t *testing.T) {
	f, err := seedRetainedFixture()
	requireRetainedOK(t, err)
	for _, leaveReaderOpen := range []bool{false, true} {
		t.Run(fmt.Sprintf("consumer-leak=%t", leaveReaderOpen), func(t *testing.T) {
			g := newRetainedWorkerGroup(t)
			baseline, err := retainedReaderFDs(g.parent)
			requireRetainedOK(t, err)
			need := workerResources(t, retainedBackupReader)
			b := newRetainedReaderBudget(need)
			err = runRetainedFixtureWorker(g.ctx, g.parent, b, need, false,
				func(r *retainedReaderReservation, path string) error {
					in := (retainedReaderCase{}).input(f)
					source := filepath.Join(path, "source")
					if err := writeRetainedImage(g.ctx, source, in.image); err != nil {
						return err
					}
					storage := checkpoint.NewBudget(checkpoint.Limits{checkpoint.LiveCaptures: 1})
					point := retainedRecoveryPoint(f.previous)
					var closedView *retainedReaderView
					opErr, cleanupErr := runRetainedReaderView(g.ctx, source, manifestFile, point, 0, storage, retainedViewOptions{reservation: r},
						func(_ context.Context, v *retainedReaderView) (err error) {
							closedView = v
							first, err := v.OpenManifest()
							if err != nil {
								return err
							}
							if !leaveReaderOpen {
								defer func() { err = errors.Join(err, first.Close()) }()
							}
							// The directory opens but fails the regular-file check.
							before, _ := b.snapshot()
							second, err := v.OpenSegment(".")
							if second != nil {
								return errors.Join(errors.New("partial open transferred a directory"), err, second.Close())
							}
							after, _ := b.snapshot()
							if after.open != before.open || after.peakOpen != before.open+1 || after.inUse != need {
								return fmt.Errorf("partial open changed live descriptors or charge: before %+v, after %+v", before, after)
							}
							return err
						})
					if closedView != nil {
						late, err := closedView.OpenManifest()
						if late != nil {
							return errors.Join(opErr, cleanupErr, err, late.Close(), errors.New("view opened a reader after cleanup"))
						}
						if !errors.Is(err, os.ErrClosed) {
							return errors.Join(opErr, cleanupErr, fmt.Errorf("opening a closed view returned %v", err))
						}
					}
					return errors.Join(opErr, cleanupErr, checkRetainedReaderBudget(storage))
				})
			if !errors.Is(err, os.ErrInvalid) || strings.Contains(err.Error(), "consumer left") != leaveReaderOpen {
				t.Fatalf("partial open or forced cleanup returned %v", err)
			}
			assertRetainedWorkerBaseline(t, g.parent, b, baseline)
		})
	}
}
