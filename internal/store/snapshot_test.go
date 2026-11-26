package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestQueryPageCursorPair(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	end := start.Add(time.Second)
	row := Row{start, "duplicate"}
	var s Store
	s.Publish([]Row{row, row})

	first, count, after, ok := s.QueryPage(start, end, "duplicate", 1, nil, nil)
	if !ok || count != 2 || !slices.Equal(first, []Row{row}) || after == nil || *after != (Position{start, 0}) {
		t.Fatalf("first page: %v, %d, %v, %v", first, count, after, ok)
	}
	missing := Position{start, 99}
	wrongTime := Position{end, 0}
	for _, tc := range []struct {
		name     string
		snapshot *int
		after    *Position
	}{
		{"existing position alone", nil, after},
		{"nonexistent position alone", nil, &missing},
		{"snapshot alone", &count, nil},
		{"ordinal outside snapshot", &count, &missing},
		{"timestamp mismatch", &count, &wrongTime},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, gotCount, next, err := s.QueryPageWithWork(start, end, "duplicate", 1, tc.snapshot, tc.after, NewQueryWork(context.Background(), 100))
			if !errors.Is(err, ErrInvalidQueryCursor) || rows != nil || gotCount != 0 || next != nil {
				t.Fatalf("metered query: %v, %d, %v, %v", rows, gotCount, next, err)
			}
			rows, gotCount, next, ok := s.QueryPage(start, end, "duplicate", 1, tc.snapshot, tc.after)
			if ok || rows != nil || gotCount != 0 || next != nil {
				t.Fatalf("legacy query: %v, %d, %v, %v", rows, gotCount, next, ok)
			}
		})
	}
	s.Publish([]Row{row})
	rows, gotCount, next, ok := s.QueryPage(start, end, "duplicate", 1, &count, after)
	if !ok || gotCount != count || !slices.Equal(rows, []Row{row}) || next != nil {
		t.Fatalf("final page: %v, %d, %v, %v", rows, gotCount, next, ok)
	}
}

// Model the snapshot directly from the append log, keeping ordinal identity
// so equal rows still require distinct, correctly positioned cursors.
func snapshotOrdinals(log []Row, start, end time.Time, literal string) []int {
	var ordinals []int
	for ordinal, row := range log {
		if !row.TS.Before(start) && row.TS.Before(end) && strings.Contains(row.Line, literal) {
			ordinals = append(ordinals, ordinal)
		}
	}
	sort.SliceStable(ordinals, func(i, j int) bool {
		return log[ordinals[i]].TS.Before(log[ordinals[j]].TS)
	})
	return ordinals
}

func checkSnapshotPage(t *testing.T, log []Row, want []int, offset, limit int, rows []Row, count int, next *Position, err error) {
	t.Helper()
	end := min(offset+limit, len(want))
	if err != nil || count != len(log) || len(rows) != end-offset {
		t.Fatalf("offset=%d limit=%d: got %d rows, snapshot %d, cursor %v, err %v; want %d rows, snapshot %d",
			offset, limit, len(rows), count, next, err, end-offset, len(log))
	}
	for i, ordinal := range want[offset:end] {
		if rows[i] != log[ordinal] {
			t.Fatalf("offset=%d limit=%d: row %d = %v, want ordinal %d (%v)",
				offset, limit, i, rows[i], ordinal, log[ordinal])
		}
	}
	if end == len(want) {
		if next != nil {
			t.Fatalf("offset=%d limit=%d: unexpected final-page cursor %v", offset, limit, next)
		}
		return
	}
	ordinal := want[end-1]
	if next == nil || next.Ordinal != ordinal || !next.Time.Equal(log[ordinal].TS) {
		t.Fatalf("offset=%d limit=%d: cursor %v, want ordinal %d at %v", offset, limit, next, ordinal, log[ordinal].TS)
	}
}

func TestQueryPageBackfillWithinBudget(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	end := start.Add(10 * time.Second)
	at := func(second int, line string) Row { return Row{start.Add(time.Duration(second) * time.Second), line} }
	for _, layout := range []string{"separate explicit", "merged explicit", "merged with recovered prefix", "implicit suffix", "historical recovered run"} {
		for _, literal := range []string{"", "hit", "needle"} {
			t.Run(layout+"/"+literal, func(t *testing.T) {
				var s Store
				original := []Row{at(0, "hit needle first"), at(2, "hit needle duplicate"), at(2, "hit needle duplicate"), at(4, "hit needle last")}
				backfill := make([]Row, 1<<16)
				for i := range backfill {
					backfill[i] = at(1+2*(i%4), "hit needle newer")
				}
				if layout == "implicit suffix" {
					for i := range backfill {
						backfill[i].TS = at(8, "").TS
					}
				}
				if layout == "merged with recovered prefix" {
					prefix := []Row{at(3, "hit needle recovered")}
					builder := NewRecoveryBuilder()
					builder.Add(prefix)
					builder.Install(&s)
					s.Publish(original)
					original = append(prefix, original...)
				} else {
					s.Publish(original)
				}
				want := snapshotOrdinals(original, start, end, literal)
				rows, count, next, err := s.QueryPageWithWork(start, end, literal, 1, nil, nil, nil)
				checkSnapshotPage(t, original, want, 0, 1, rows, count, next, err)
				if layout == "historical recovered run" {
					builder := NewRecoveryBuilder()
					builder.Add(original)
					builder.Add(backfill)
					builder.Install(&s)
				} else {
					if layout == "separate explicit" {
						s.Publish(original[:1])
					}
					s.Publish(backfill)
				}
				for offset := 1; offset < len(want); offset++ {
					after := next
					work := NewQueryWork(context.Background(), 256)
					var gotCount int
					rows, gotCount, next, err = s.QueryPageWithWork(start, end, literal, 1, &count, after, work)
					checkSnapshotPage(t, original, want, offset, 1, rows, gotCount, next, err)
					if offset == 1 {
						underBudget := NewQueryWork(context.Background(), 256-work.remaining-1)
						partial, partialCount, partialNext, err := s.QueryPageWithWork(start, end, literal, 1, &count, after, underBudget)
						if !errors.Is(err, ErrQueryWorkExhausted) || partial != nil || partialCount != 0 || partialNext != nil {
							t.Fatalf("under budget: %v, %d, %v, %v", partial, partialCount, partialNext, err)
						}
					}
				}
			})
		}
	}
}

func TestSnapshotVisibilitySearchCancellation(t *testing.T) {
	run := indexRun{ordinals: make([]int, 128), length: 128}
	for i := range run.ordinals {
		run.ordinals[i] = (i + 1) % run.length
	}
	run.buildVisibility()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The first rank group contains no visible ordinal. Cancel on the first
	// node after its probes so the metadata traversal must propagate the error.
	work := NewQueryWork(&cancelDuringWork{ctx, cancel, visibilityBlockSize + 1}, 128)
	if _, err := run.nextVisible(0, 1, work); !errors.Is(err, context.Canceled) {
		t.Fatalf("visibility search: got %v, want context.Canceled", err)
	}
}

func TestQueryPageAppendLogOracle(t *testing.T) {
	epoch := time.Unix(1000, 0).UTC()
	start, end := epoch.Add(3*time.Second), epoch.Add(19*time.Second)
	for _, seed := range []int64{23, 43, 25981} {
		for _, literal := range []string{"", "hit", "needle", "absent"} {
			t.Run(fmt.Sprintf("seed=%d/literal=%q", seed, literal), func(t *testing.T) {
				rng := rand.New(rand.NewSource(seed))
				makeBatch := func(n int) []Row {
					batch := make([]Row, n)
					for i := range batch {
						ts := epoch.Add(time.Duration(rng.Intn(24)) * time.Second)
						line := []string{"other", "hit", "needle", "hit needle"}[rng.Intn(4)]
						batch[i] = Row{ts, line}
					}
					return batch
				}
				var s Store
				original := makeBatch(2*blockSize + 13)
				builder := NewRecoveryBuilder()
				builder.Add(original[:blockSize-1])
				builder.Install(&s)
				for batch := blockSize - 1; batch < len(original); batch += 16 {
					s.Publish(original[batch:min(batch+16, len(original))])
				}
				want := snapshotOrdinals(original, start, end, literal)
				var snapshot *int
				var next *Position
				for offset := 0; ; {
					limit := 1 + rng.Intn(17)
					rows, count, cursor, err := s.QueryPageWithWork(start, end, literal, limit, snapshot, next, NewQueryWork(context.Background(), 4096))
					checkSnapshotPage(t, original, want, offset, limit, rows, count, cursor, err)
					if cursor == nil {
						break
					}
					snapshot, next = &count, cursor
					offset += len(rows)
					s.Publish(makeBatch(1 + rng.Intn(32)))
				}
			})
		}
	}
}

func TestRecoveredPrefixConcurrentPublication(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	end := start.Add(10 * time.Second)
	prefix := make([]Row, blockSize)
	for i := range prefix {
		prefix[i] = Row{start.Add(time.Duration(i) * time.Millisecond), "needle"}
	}
	builder := NewRecoveryBuilder()
	// Reserve capacity explicitly so the test exercises sharing in both slices.
	builder.rows = make([]Row, 0, 3*blockSize)
	builder.blocks = make([]*blockFilter, 0, 3)
	builder.Add(prefix)
	var stores [3]Store
	for _, s := range []*Store{&stores[0], &stores[1], &stores[2]} {
		builder.Install(s)
		if cap(s.rows) != len(s.rows) || cap(s.blocks) != len(s.blocks) {
			t.Error("installed slice retains spare capacity")
		}
	}
	snapshotRows, count, next, err := stores[0].QueryPageWithWork(start, end, "needle", 1, nil, nil, nil)
	want := snapshotOrdinals(prefix, start, end, "needle")
	checkSnapshotPage(t, prefix, want, 0, 1, snapshotRows, count, next, err)

	var writers sync.WaitGroup
	defer writers.Wait()
	begin := make(chan struct{})
	for i, line := range []string{"first needle", "second needle", "builder needle"} {
		batch := make([]Row, blockSize)
		for j := range batch {
			batch[j] = Row{start.Add(time.Duration(j%7) * time.Millisecond), line}
		}
		writers.Go(func() {
			<-begin
			for j := 0; j < 8; j++ {
				if i == 2 {
					builder.Add(batch)
				} else {
					stores[i].Publish(batch)
				}
			}
		})
	}
	close(begin)
	for offset := 1; offset < len(want); offset++ {
		var gotCount int
		snapshotRows, gotCount, next, err = stores[0].QueryPageWithWork(start, end, "needle", 1, &count, next, NewQueryWork(context.Background(), 256))
		checkSnapshotPage(t, prefix, want, offset, 1, snapshotRows, gotCount, next, err)
	}

	writers.Wait()
	builder.Install(&stores[2])
	for i, literal := range []string{"first needle", "second needle", "builder needle"} {
		rows := stores[i].Query(start, end, literal, 8*blockSize+1)
		if len(rows) != 8*blockSize {
			t.Errorf("store %d: got %d rows tagged %q, want %d", i, len(rows), literal, 8*blockSize)
		}
	}
}
