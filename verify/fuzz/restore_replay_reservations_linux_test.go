package fuzz

import (
	"cmp"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/store"
)

type cancelAtFrameBoundary struct {
	io.ReaderAt
	offset int64
	cancel func()
	ctx    context.Context
}

func (r cancelAtFrameBoundary) ReadAt(p []byte, off int64) (int, error) {
	if off == r.offset {
		r.cancel()
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.ReaderAt.ReadAt(p, off)
}

func TestRestoreReplayReservations(t *testing.T) {
	f := newFixture(t)
	restored := filepath.Join(t.TempDir(), "restored")
	writeFiles(t, restored, f.image)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	from, to := f.point.CoveredOffset, f.point.CommittedEnd
	suffixBytes := uint64(to - from)
	const baselineBytes = 1
	budget := checkpoint.NewBudget(checkpoint.Limits{checkpoint.ReplayTailBytes: baselineBytes + suffixBytes})
	existing, err := budget.Reserve(ctx, checkpoint.ReplayTailBytes, baselineBytes)
	must(t, err)
	defer existing.Release()
	assertReplayBytes := func(phase string, want uint64) {
		t.Helper()
		if got := budget.Usage().InUse[checkpoint.ReplayTailBytes]; got != want {
			t.Fatalf("%s: replay bytes in use = %d, want %d", phase, got, want)
		}
	}

	// Close the caller's root after verification. Replay owns the returned
	// reader independently and uses the point's absolute WAL offsets.
	point, reader := func() (backup.Point, backup.WALReader) {
		root, err := os.OpenRoot(restored)
		must(t, err)
		defer func() { must(t, root.Close()) }()
		point, reader, err := backup.VerifyPinnedRestore(ctx, root, func(start, end int64) error {
			if start != from || end != to {
				t.Fatalf("verified interval = [%d, %d), want [%d, %d)", start, end, from, to)
			}
			probe, err := budget.ReserveReplayTail(ctx, start, end)
			if err == nil {
				probe.Release()
			}
			return err
		})
		if reader != nil {
			t.Cleanup(func() { must(t, reader.Close()) })
		}
		must(t, err)
		return point, reader
	}()
	if point != f.point || reader == nil {
		t.Fatalf("verified point = %+v, reader present = %t, want %+v with a reader", point, reader != nil, f.point)
	}
	assertReplayBytes("after verification", baselineBytes)
	replayTail := func(ctx context.Context, input io.ReaderAt) (*checkpoint.ReplayedTail, error) {
		tail, err := budget.ReplayCapturedTail(ctx, input, point.CoveredOffset, point.CommittedEnd, point.NextOrdinal)
		if tail != nil {
			for _, batch := range tail.Batches {
				if batch.Reservation != nil {
					t.Cleanup(batch.Reservation.Release)
				}
			}
		}
		return tail, err
	}

	var rows []backup.Row
	for _, row := range f.rows {
		if row.Ordinal >= f.point.NextOrdinal {
			rows = append(rows, row)
		}
	}
	slices.SortFunc(rows, func(a, b backup.Row) int { return cmp.Compare(a.Ordinal, b.Ordinal) })
	if len(rows) != 3 {
		t.Fatalf("fixture has %d tail rows, want 3", len(rows))
	}
	secondStart := from + int64(len(frame(t, rows[:2])))

	replayCtx, cancelReplay := context.WithCancel(ctx)
	defer cancelReplay()
	reachedBoundary := false
	// Reading the second frame requires the scanner to have decoded and
	// assigned the first frame its own reservation.
	input := cancelAtFrameBoundary{ReaderAt: reader, offset: secondStart, ctx: replayCtx, cancel: func() {
		reachedBoundary = true
		assertReplayBytes("at the second frame", baselineBytes+suffixBytes)
		cancelReplay()
	}}
	partial, err := replayTail(replayCtx, input)
	if !reachedBoundary || partial != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled replay: reached second frame = %t, result = %+v, error = %v, want cancellation with no result",
			reachedBoundary, partial, err)
	}
	assertReplayBytes("after cancellation", baselineBytes)
	assertFiles(t, restored, f.image)

	replay, err := replayTail(ctx, reader)
	must(t, err)
	if replay == nil || len(replay.Batches) != 2 || replay.NextOrdinal != rows[2].Ordinal+1 {
		t.Fatalf("retry replay = %+v, want two frames and next ordinal %d", replay, rows[2].Ordinal+1)
	}
	for i, want := range []struct {
		start, end int64
		rows       []backup.Row
	}{
		{from, secondStart, rows[:2]},
		{secondStart, to, rows[2:]},
	} {
		batch := replay.Batches[i]
		if batch.Start != want.start || batch.End != want.end || batch.FirstOrdinal != want.rows[0].Ordinal ||
			batch.Reservation == nil || !slices.EqualFunc(batch.Rows, want.rows, func(got store.Row, want backup.Row) bool {
			return got == want.Row
		}) {
			t.Fatalf("retry frame %d = %+v, want [%d, %d) with rows %+v", i, batch, want.start, want.end, want.rows)
		}
	}
	assertReplayBytes("after retry", baselineBytes+suffixBytes)

	held := replay.Batches[0].Reservation.Retain()
	if held == nil {
		t.Fatal("could not retain the first frame reservation")
	}
	defer held.Release()
	assertReplayBytes("after retaining the first frame", baselineBytes+suffixBytes)
	for _, batch := range replay.Batches {
		batch.Reservation.Release()
	}
	assertReplayBytes("after caller releases both frames", baselineBytes+uint64(secondStart-from))
	held.Release()
	assertReplayBytes("after the last frame reference releases", baselineBytes)
	existing.Release()
	assertReplayBytes("after the baseline reservation releases", 0)
	assertFiles(t, restored, f.image)
}
