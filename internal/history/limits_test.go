package history

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"bog/internal/checkpoint"
)

func assertExhausted(t *testing.T, err error, resource checkpoint.Resource, limit uint64) {
	t.Helper()
	var rejected *checkpoint.ExhaustedError
	if !errors.As(err, &rejected) || rejected.Resource != resource ||
		rejected.InUse != limit || rejected.Limit != limit || rejected.Need != 1 {
		t.Fatalf("expected exhausted %s at %d, got %v", resource, limit, err)
	}
}

func TestBoundedReplacementAndCaptureDrainOnPinnedDirectory(t *testing.T) {
	ctx := t.Context()
	lines := []checkpoint.BlockRow{row(epoch, 0, "[.*]"), row(epoch, 1, "[.*]"), row(epoch.Add(time.Nanosecond), 2, "[.*]")}
	parent := privateDir(t)
	path, initial := segmentFixture(t, [][]checkpoint.BlockRow{lines})
	pinned, err := os.Open(path)
	requireOK(t, err)
	cleanup(t, pinned.Close)
	activePath := filepath.Join(parent, "active")
	requireOK(t, os.Rename(path, activePath))
	requireOK(t, os.Mkdir(path, 0700))
	budget := checkpoint.NewBudget(checkpoint.Limits{checkpoint.PinnedGenerations: 2, checkpoint.LiveCaptures: 1})
	cat := NewCatalog(budget)
	cleanup(t, cat.Close)
	old, err := cat.ReplacePinned(ctx, pinned)
	requireOK(t, err)
	capture, err := cat.AcquireCurrent()
	requireOK(t, err)
	cleanup(t, capture.Close)
	_, err = cat.AcquireCurrent()
	assertExhausted(t, err, checkpoint.LiveCaptures, 1)

	began, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	defer unblock()
	done := make(chan error, 1)
	go func() {
		var seen []checkpoint.BlockRow
		err := capture.Scan(ctx, Query{Start: epoch, End: epoch.Add(time.Nanosecond), Contains: "[.*]"},
			func(item checkpoint.BlockRow) error {
				seen = append(seen, item)
				if len(seen) == 1 {
					close(began)
					<-resume
				}
				return nil
			})
		if err == nil && !slices.Equal(seen, lines[:2]) {
			err = fmt.Errorf("unexpected retired snapshot: %+v", seen)
		}
		done <- err
	}()
	select {
	case <-began:
	case err := <-done:
		t.Fatalf("held scan completed early: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("scan did not start")
	}

	ticket, err := cat.ReserveReplacement(ctx)
	requireOK(t, err)
	defer ticket.Close()
	_, err = cat.ReserveReplacement(ctx)
	assertExhausted(t, err, checkpoint.PinnedGenerations, 2)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ticket.ReplacePinned(cancelled, pinned); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled replacement: %v", err)
	}
	next := initial
	nextRow := row(epoch.Add(-time.Nanosecond), 0, "backfill")
	next.Segments = []segmentMeta{installHistoryRun(t, activePath, 7, nextRow)}
	next.NextOrdinal = 1
	writeManifest(t, activePath, next)
	current, err := ticket.ReplacePinned(ctx, pinned)
	requireOK(t, err)
	if current.ID == old.ID {
		t.Fatal("replacement did not activate")
	}
	_, err = cat.Acquire(old.ID)
	assertExpired(t, err, old.ID)
	requireOK(t, capture.Close())
	if err := capture.Scan(ctx, all, nil); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("retired capture admitted a scan: %v", err)
	}
	_, err = cat.AcquireCurrent()
	assertExhausted(t, err, checkpoint.LiveCaptures, 1)
	_, err = cat.ReserveReplacement(ctx)
	assertExhausted(t, err, checkpoint.PinnedGenerations, 2)
	requireOK(t, cat.ReclaimPinned(ctx, pinned))
	assertSegment(t, activePath, initial.Segments[0].Name, true)
	select {
	case <-old.Drained:
		t.Fatal("closed capture invalidated an admitted scan")
	default:
	}

	unblock()
	select {
	case err := <-done:
		requireOK(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("held scan did not drain")
	}
	select {
	case <-old.Drained:
	default:
		t.Fatal("retired generation remained pinned after the scan completed")
	}
	requireOK(t, cat.ReclaimPinned(ctx, pinned))
	assertSegment(t, activePath, initial.Segments[0].Name, false)
	fresh, err := cat.AcquireCurrent()
	requireOK(t, err)
	cleanup(t, fresh.Close)
	if got := scanRows(t, fresh, all); !slices.Equal(got, []checkpoint.BlockRow{nextRow}) {
		t.Fatalf("new snapshot = %v", got)
	}
	retry, err := cat.ReserveReplacement(ctx)
	requireOK(t, err)
	retry.Close()
	requireOK(t, fresh.Close())
	requireOK(t, cat.Close())
	usage := budget.Usage()
	if usage.InUse != (checkpoint.Limits{}) ||
		usage.Peak[checkpoint.PinnedGenerations] != 2 || usage.Peak[checkpoint.LiveCaptures] != 1 {
		t.Fatalf("usage after shutdown = %+v", usage)
	}
}

func TestExpiredCaptureRetainsAnAdmittedScan(t *testing.T) {
	dir, _ := segmentFixture(t, [][]checkpoint.BlockRow{{row(epoch, 0, "old")}})
	synctest.Test(t, func(t *testing.T) {
		budget := checkpoint.NewBudget(checkpoint.Limits{checkpoint.PinnedGenerations: 1, checkpoint.LiveCaptures: 1})
		cat := NewCatalog(budget)
		defer func() { requireOK(t, cat.Close()) }()
		gen, err := cat.Replace(t.Context(), dir)
		requireOK(t, err)
		deadline := time.Now().Add(time.Minute)
		capture, err := cat.AcquireCurrentUntil(t.Context(), deadline)
		requireOK(t, err)
		defer func() { requireOK(t, capture.Close()) }()

		resume, done := make(chan struct{}), make(chan error, 1)
		var once sync.Once
		unblock := func() { once.Do(func() { close(resume) }) }
		defer unblock()
		go func() {
			done <- capture.Scan(t.Context(), all, func(checkpoint.BlockRow) error {
				<-resume
				return nil
			})
		}()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("expected scan to be admitted and blocked: %v", err)
		default:
		}
		time.Sleep(time.Until(deadline))
		assertExpired(t, capture.Scan(t.Context(), all, nil), gen.ID)
		requireOK(t, capture.Close())
		_, err = cat.AcquireCurrentUntil(t.Context(), deadline)
		assertExpired(t, err, gen.ID)
		_, err = cat.AcquireCurrent()
		assertExhausted(t, err, checkpoint.LiveCaptures, 1)
		unblock()
		requireOK(t, <-done)
		fresh, err := cat.AcquireCurrent()
		requireOK(t, err)
		requireOK(t, fresh.Close())
	})
}

func TestReplacementReservationRetryAfterValidationFailure(t *testing.T) {
	dir, m := segmentFixture(t, [][]checkpoint.BlockRow{{row(epoch, 0, "old")}})
	pinned, err := os.Open(dir)
	requireOK(t, err)
	cleanup(t, pinned.Close)
	budget := checkpoint.NewBudget(checkpoint.Limits{checkpoint.PinnedGenerations: 1})
	cat := NewCatalog(budget)
	cleanup(t, cat.Close)
	ticket, err := cat.ReserveReplacement(t.Context())
	requireOK(t, err)
	defer ticket.Close()
	name := filepath.Join(dir, m.Segments[0].Name)
	data, err := os.ReadFile(name)
	requireOK(t, err)
	requireOK(t, os.Remove(name))
	if _, err := ticket.ReplacePinned(t.Context(), pinned); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("missing committed segment: %v", err)
	}
	_, err = cat.ReserveReplacement(t.Context())
	assertExhausted(t, err, checkpoint.PinnedGenerations, 1)
	writeFile(t, name, data)
	_, err = ticket.ReplacePinned(t.Context(), pinned)
	requireOK(t, err)
	ticket.Close()
	if inUse := budget.Usage().InUse[checkpoint.PinnedGenerations]; inUse != 1 {
		t.Fatalf("closing used ticket released active generation: %d", inUse)
	}
}
