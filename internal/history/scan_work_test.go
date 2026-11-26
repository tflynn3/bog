package history

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

func TestScanWithWorkBudgetAndLookahead(t *testing.T) {
	const literal = "hit [.*]"
	miss := strings.Repeat("x", readPageSize) + " HIT [.*]"
	end := epoch.Add(3 * time.Nanosecond)
	dir, _ := segmentFixture(t, [][]checkpoint.BlockRow{
		{
			row(epoch.Add(-1), 0, literal),
			row(epoch, 2, literal),
			row(epoch, 4, literal),
			row(epoch.Add(1), 5, miss),
			row(epoch.Add(2), 7, literal),
			row(end, 8, literal),
		},
		{row(epoch, 1, literal), row(epoch, 3, literal), row(end, 9, literal)},
		{row(end.Add(1), 6, literal)},
	})
	ctx := t.Context()
	var cat Catalog
	cleanup(t, cat.Close)
	_, err := cat.Replace(ctx, dir)
	requireOK(t, err)
	r, err := cat.AcquireCurrent()
	requireOK(t, err)
	cleanup(t, r.Close)
	q := Query{Start: epoch, End: end, Contains: literal, After: &Position{epoch, 2}}

	// Setup charges all three segments, seeks of 3 and 2 comparisons, and
	// five candidate headers, including three excluded by the resume anchor.
	const setup = 3 + 3 + 2 + 5
	// Each of the first two lines is followed by another examined header.
	const afterPage = setup + 2*(len(literal)+1)
	lookahead := uint64(afterPage + len(miss) + 1 + len(literal))
	stop := errors.New("lookahead found")
	for _, tc := range []struct {
		name   string
		budget uint64
		stop   bool
		want   []uint64
		err    error
	}{
		{"segment", 0, false, nil, store.ErrQueryWorkExhausted},
		{"seek", 3, false, nil, store.ErrQueryWorkExhausted},
		{"header before anchor", 4, false, nil, store.ErrQueryWorkExhausted},
		{"first eligible line", uint64(setup + len(literal) - 1), false, nil, store.ErrQueryWorkExhausted},
		{"sparse nonmatch bytes", uint64(afterPage + len(miss) - 1), true, []uint64{3, 4}, store.ErrQueryWorkExhausted},
		{"lookahead bytes", lookahead - 1, true, []uint64{3, 4}, store.ErrQueryWorkExhausted},
		{"lookahead found", lookahead, true, []uint64{3, 4, 7}, stop},
		{"upper bound header", lookahead, false, []uint64{3, 4, 7}, store.ErrQueryWorkExhausted},
		{"complete", lookahead + 1, false, []uint64{3, 4, 7}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for range 2 {
				work := store.NewQueryWork(ctx, tc.budget)
				var got []uint64
				err := r.ScanWithWork(ctx, q, func(item checkpoint.BlockRow) error {
					got = append(got, item.Ordinal)
					if tc.stop && len(got) == 3 {
						return stop
					}
					return nil
				}, work)
				if !errors.Is(err, tc.err) || !slices.Equal(got, tc.want) {
					t.Fatalf("ordinals %v, error %v, want %v, %v", got, err, tc.want, tc.err)
				}
				if tc.err == nil || tc.err == stop {
					if err := work.Charge(1); !errors.Is(err, store.ErrQueryWorkExhausted) {
						t.Fatalf("exact budget left a unit after scan: %v", err)
					}
				}
			}
		})
	}

	for _, q := range []Query{
		all, q,
		{Start: epoch, End: end, Contains: "HIT [.*]"},
		{Start: epoch, End: end, Contains: "never present"},
		{Start: epoch, End: epoch},
		{Start: epoch, End: end, After: &Position{Time: end}},
	} {
		want := scanRows(t, r, q)
		var got []checkpoint.BlockRow
		requireOK(t, r.ScanWithWork(ctx, q, func(item checkpoint.BlockRow) error {
			got = append(got, item)
			return nil
		}, store.NewQueryWork(ctx, 1<<20)))
		if !slices.Equal(got, want) {
			t.Fatalf("metered scan disagrees with Scan for query %+v", q)
		}
	}
}

func TestScanWithWorkChargesLineBeforeOverflowRead(t *testing.T) {
	const first = "first"
	long := strings.Repeat("x", 2*readPageSize)
	// The second header fits in the first page, but its line requires
	// another read. Removing the file distinguishes the two outcomes.
	exact := uint64(1 + 2 + 2 + len(first) + len(long))
	for _, tc := range []struct {
		name   string
		budget uint64
		want   error
	}{
		{"cannot afford read", exact - 1, store.ErrQueryWorkExhausted},
		{"can afford read", exact, ErrIntegrity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			dir, m := segmentFixture(t, [][]checkpoint.BlockRow{{row(epoch, 0, first), row(epoch, 1, long)}})
			var cat Catalog
			cleanup(t, cat.Close)
			_, err := cat.Replace(ctx, dir)
			requireOK(t, err)
			r, err := cat.AcquireCurrent()
			requireOK(t, err)
			cleanup(t, r.Close)

			seen := 0
			err = r.ScanWithWork(ctx, all, func(item checkpoint.BlockRow) error {
				seen++
				if item.Ordinal != 0 {
					t.Fatalf("unexpected row %d", item.Ordinal)
				}
				return os.Remove(filepath.Join(dir, m.Segments[0].Name))
			}, store.NewQueryWork(ctx, tc.budget))
			if seen != 1 || !errors.Is(err, tc.want) {
				t.Fatalf("scan visited %d rows and returned %v, want one row and %v", seen, err, tc.want)
			}
		})
	}
}

func TestScanWithWorkReleasesRetiredCaptureOnFailure(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		cancelScan, cancelWorkContext bool
		want                          error
	}{
		{"scan cancellation", true, false, context.Canceled},
		{"work cancellation", false, true, context.Canceled},
		{"budget exhaustion", false, false, store.ErrQueryWorkExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			meterCtx, cancelWork := context.WithCancel(t.Context())
			defer cancelWork()
			oldDir, _ := segmentFixture(t, [][]checkpoint.BlockRow{{row(epoch, 0, "first"), row(epoch, 1, "second")}})
			newDir, _ := segmentFixture(t, [][]checkpoint.BlockRow{{row(epoch.Add(-1), 0, "backfill")}})
			budget := checkpoint.NewBudget(checkpoint.Limits{checkpoint.PinnedGenerations: 2, checkpoint.LiveCaptures: 1})
			cat := NewCatalog(budget)
			cleanup(t, cat.Close)
			old, err := cat.Replace(t.Context(), oldDir)
			requireOK(t, err)
			r, err := cat.AcquireCurrent()
			requireOK(t, err)
			cleanup(t, r.Close)

			const firstRowWork = 1 + 2 + 1 + len("first") // segment, seek, header, bytes
			seen := 0
			err = r.ScanWithWork(ctx, all, func(item checkpoint.BlockRow) error {
				seen++
				if seen != 1 || item.Ordinal != 0 {
					t.Fatalf("unexpected row %d during failed scan", item.Ordinal)
				}
				_, err := cat.Replace(t.Context(), newDir)
				requireOK(t, err)
				requireOK(t, r.Close())
				select {
				case <-old.Drained:
					t.Fatal("retired generation drained while scan was active")
				default:
				}
				if tc.cancelScan {
					cancel()
				}
				if tc.cancelWorkContext {
					cancelWork()
				}
				return nil
			}, store.NewQueryWork(meterCtx, uint64(firstRowWork)))
			if !errors.Is(err, tc.want) || seen != 1 {
				t.Fatalf("scan visited %d rows and returned %v, want one row and %v", seen, err, tc.want)
			}
			select {
			case <-old.Drained:
			default:
				t.Fatal("retired generation remained pinned after scan failure")
			}
			if got := budget.Usage().InUse; got != (checkpoint.Limits{checkpoint.PinnedGenerations: 1}) {
				t.Fatalf("usage after scan failure = %v", got)
			}
			if err := r.ScanWithWork(t.Context(), all, nil, nil); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("new scan on closed capture returned %v", err)
			}
		})
	}
}
