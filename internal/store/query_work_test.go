package store

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestQueryWorkCharge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	work := NewQueryWork(ctx, math.MaxUint64)
	for _, tc := range []struct {
		charges []uint64
		want    error
	}{
		{[]uint64{math.MaxUint64, 1}, ErrQueryWorkExhausted},
		{[]uint64{math.MaxUint64 - 1, 1}, nil},
		{nil, nil},
		{[]uint64{1}, ErrQueryWorkExhausted},
	} {
		if err := work.Charge(tc.charges...); !errors.Is(err, tc.want) {
			t.Fatalf("charge %v: got %v, want %v", tc.charges, err, tc.want)
		}
	}
	cancel()
	if err := work.Charge(1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled charge: %v", err)
	}
}

func TestQueryWorkBoundaries(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	end := start.Add(time.Minute)
	row := func(offset int, line string) Row { return Row{start.Add(time.Duration(offset)), line} }
	direct := new(Store)
	direct.Publish([]Row{row(1, "hit"), row(2, "abc"), row(3, "hit")})
	merged := new(Store)
	recovery := NewRecoveryBuilder()
	recovery.Add([]Row{row(1, "hit"), row(3, "hit")})
	recovery.Install(merged)
	merged.Publish([]Row{row(2, "hit")})
	large := func(n int, line string, backfill, multipleRuns, dense, tailMatch bool) *Store {
		t.Helper()
		s := new(Store)
		rows := make([]Row, n)
		for i := range rows {
			rows[i] = row(i, line)
		}
		if dense {
			rows[0].Line, rows[1].Line = "needle", "needle"
		}
		if tailMatch {
			rows[n-1].Line = "needle"
		}
		if backfill {
			rows[n-2].TS, rows[n-3].TS = rows[n-3].TS, rows[n-2].TS
		}
		if multipleRuns {
			r := NewRecoveryBuilder()
			r.Add(rows[:n/2])
			r.Install(s)
			s.Publish(rows[n/2:])
		} else {
			s.Publish(rows)
		}
		return s
	}
	for _, tc := range []struct {
		name, literal string
		s             *Store
		page          bool
		budget        uint64
		wantLines     int
		wantNext      bool
	}{
		{"direct", "hit", direct, false, 3 + 4, 1, false},
		{"direct lookahead through nonmatch", "hit", direct, true, 3 + 3*4, 1, true},
		{"merged", "hit", merged, false, 5 + 2*4, 1, false},
		{"merged lookahead", "hit", merged, true, 5 + 3*4, 1, true},
		{"explicit direct fallback", "needle", large(513, "xxxx", true, false, false, false), false, 1 + 10 + 10 + 513*5, 0, false},
		{"direct Bloom pruning", "needle", large(1024, "xxxx", false, false, false, false), true, 1 + 11 + 11 + 512*5 + 4, 0, false},
		{"block candidates after Bloom", "needle", large(32768, "xxxx", true, false, false, true), true, 1 + 16 + 16 + 512*5 + 256 + 127*5 + 7, 1, false},
		{"bounded merge", "needle", large(32768, "xxxx", false, true, true, false), true, 2*(1+15+15) + 2*7, 1, true},
		{"per-run Bloom fallback", "needle", large(32768, "xxxx", false, true, false, true), true, 2*(1+15+15+15) + 512*5 + 256 + 127*5 + 7, 1, false},
		// Each block contains all three grams, so Bloom selection gives up
		// after 65 blocks and the merge must revisit the bounded scan's rows.
		{"merge after Bloom false positives", "abcdef", large(32768, "abcd bcde cdef", true, true, false, false), true, 2*(1+15+15) + 512*15 + 65 + 32768*15, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, budget := range []uint64{tc.budget - 1, tc.budget} {
				var rows []Row
				var next *Position
				var err error
				work := NewQueryWork(context.Background(), budget)
				if tc.page {
					rows, _, next, err = tc.s.QueryPageWithWork(start, end, tc.literal, 1, nil, nil, work)
				} else {
					rows, err = tc.s.QueryWithWork(start, end, tc.literal, 1, work)
				}
				if budget < tc.budget {
					if !errors.Is(err, ErrQueryWorkExhausted) || rows != nil || next != nil {
						t.Fatalf("under budget: rows=%v next=%v err=%v", rows, next, err)
					}
					continue
				}
				if err != nil || len(rows) != tc.wantLines || (next != nil) != tc.wantNext {
					t.Fatalf("exact budget: rows=%v next=%v err=%v", rows, next, err)
				}
				// Metering preserves the exact results and cursor of the legacy callers.
				var want []Row
				var wantNext *Position
				if tc.page {
					var ok bool
					want, _, wantNext, ok = tc.s.QueryPage(start, end, tc.literal, 1, nil, nil)
					if !ok {
						t.Fatal("legacy first page was rejected")
					}
				} else {
					want = tc.s.Query(start, end, tc.literal, 1)
				}
				if !reflect.DeepEqual(rows, want) || !reflect.DeepEqual(next, wantNext) {
					t.Fatalf("legacy parity: got %v %v, want %v %v", rows, next, want, wantNext)
				}
			}
		})
	}
}

// cancelDuringWork makes cancellation happen at a particular accounting call.
type cancelDuringWork struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func (c *cancelDuringWork) Err() error {
	c.checks--
	if c.checks == 0 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestQueryWorkCancellationAndCursor(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	end := start.Add(time.Second)
	s := new(Store)
	s.Publish([]Row{{start, "hit"}, {start, "hit"}, {start, "hit"}})
	snapshot, after := 3, Position{start, 0}
	for _, tc := range []struct {
		name   string
		budget uint64
		checks int
		want   error
	}{
		{"cancel before capture", 100, 1, context.Canceled},
		{"cancel in setup", 100, 3, context.Canceled},
		{"cancel during lookahead", 100, 7, context.Canceled},
		{"reject anchor cost", 3, 0, ErrQueryWorkExhausted},
		{"exact continuation", 4 + 3 + 2 + 2*4, 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			work := NewQueryWork(&cancelDuringWork{ctx, cancel, tc.checks}, tc.budget)
			rows, count, next, err := s.QueryPageWithWork(start, end, "hit", 1, &snapshot, &after, work)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if err != nil {
				if rows != nil || count != 0 || next != nil {
					t.Fatalf("partial result: %v %d %v", rows, count, next)
				}
			} else if len(rows) != 1 || count != snapshot || next == nil || next.Ordinal != 1 {
				t.Fatalf("continuation: %v %d %v", rows, count, next)
			}
		})
	}
}
