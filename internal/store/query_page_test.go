package store

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func TestQuerySnapshotPageResumeAfterAnyReturnedRow(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	end := start.Add(10 * time.Second)
	last := start.Add(time.Second)
	needle := "needle"
	for _, tc := range []struct {
		name string
		load func(*Store)
		want []int
	}{
		{
			name: "implicit",
			load: func(s *Store) {
				s.Publish([]Row{{start, needle}, {start, needle}, {last, needle}})
			},
			want: []int{0, 1, 2},
		},
		{
			name: "sorted merge",
			load: func(s *Store) {
				recovery := NewRecoveryBuilder()
				recovery.Add([]Row{{last, needle}, {start, needle}})
				recovery.Install(s)
				s.Publish([]Row{{start, needle}, {start, "miss"}})
			},
			want: []int{1, 2, 0},
		},
		{
			name: "sparse Bloom candidates",
			load: func(s *Store) {
				rows := make([]Row, 1<<15)
				for i := range rows {
					rows[i] = Row{last, "miss"}
				}
				rows[7].TS = start
				rows[1000] = Row{last, needle}
				rows[2000] = Row{start, needle}
				rows[3000] = Row{start, needle}
				s.Publish(rows)
			},
			want: []int{2000, 3000, 1000},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := new(Store)
			tc.load(s)
			first, err := s.QuerySnapshotPage(start, end, needle, 2, nil, nil)
			if err != nil || !first.HasMore || !slices.Equal(first.Ordinals, tc.want[:2]) || len(first.Rows) != 2 {
				t.Fatalf("first page = %+v, err = %v", first, err)
			}
			for i, row := range first.Rows {
				if row != s.rows[first.Ordinals[i]] {
					t.Fatalf("row %d = %+v at ordinal %d", i, row, first.Ordinals[i])
				}
			}
			s.Publish([]Row{{start, needle}, {last, needle}})
			// A response that fit only its first record must be able to
			// resume after that record even though the store returned two.
			anchor := &PageAnchor{Count: first.Count, Ordinal: first.Ordinals[0]}
			terminal, err := s.QuerySnapshotPage(start, end, needle, 2, anchor, nil)
			if err != nil || terminal.HasMore || terminal.Count != first.Count || !slices.Equal(terminal.Ordinals, tc.want[1:]) {
				t.Fatalf("terminal page = %+v, err = %v", terminal, err)
			}
			for i, row := range terminal.Rows {
				if row != s.rows[terminal.Ordinals[i]] {
					t.Fatalf("row %d = %+v at ordinal %d", i, row, terminal.Ordinals[i])
				}
			}
		})
	}
}

func TestQuerySnapshotPageAnchorAndLookaheadBudget(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	end := start.Add(time.Second)
	s := new(Store)
	s.Publish([]Row{{start, "hit"}, {start, "miss"}, {start, "hit"}, {start, "hit"}, {end, "hit"}})
	for _, anchor := range []PageAnchor{{0, 0}, {6, 0}, {5, -1}, {5, 5}, {5, 1}, {5, 4}} {
		page, err := s.QuerySnapshotPage(start, end, "hit", 1, &anchor, nil)
		if !errors.Is(err, ErrInvalidQueryCursor) || page.Count != 0 || len(page.Rows) != 0 {
			t.Fatalf("anchor %+v: page = %+v, err = %v", anchor, page, err)
		}
	}

	const (
		anchorCost = 1 + uint64(len("hit"))
		seekCost   = 1 + 1 // one run and one implicit-rank lookup
		matchCost  = 1 + uint64(len("miss")) + 1 + uint64(len("hit"))
		lookahead  = 1 + uint64(len("hit"))
		minimum    = anchorCost + seekCost + matchCost + lookahead
	)
	anchor := &PageAnchor{Count: 5, Ordinal: 0}
	for _, budget := range []uint64{anchorCost - 1, anchorCost + seekCost - 1, minimum - 1, minimum} {
		work := NewQueryWork(t.Context(), budget)
		page, err := s.QuerySnapshotPage(start, end, "hit", 1, anchor, work)
		if budget < minimum {
			if !errors.Is(err, ErrQueryWorkExhausted) || page.Count != 0 || len(page.Rows) != 0 {
				t.Fatalf("budget %d: page = %+v, err = %v", budget, page, err)
			}
		} else if err != nil || page.Count != 5 || !slices.Equal(page.Ordinals, []int{2}) || !page.HasMore || work.remaining != 0 {
			t.Fatalf("budget %d: page = %+v, err = %v, remaining = %d", budget, page, err, work.remaining)
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	page, err := s.QuerySnapshotPage(start, end, "hit", 1, anchor, NewQueryWork(ctx, minimum))
	if !errors.Is(err, context.Canceled) || page.Count != 0 || len(page.Rows) != 0 {
		t.Fatalf("canceled query: page = %+v, err = %v", page, err)
	}
}

func TestSnapshotVisibilitySkipsEmptyGroupsWithinBudget(t *testing.T) {
	const length = 8192
	run := indexRun{ordinals: make([]int, length), length: length}
	for i := range run.ordinals {
		run.ordinals[i] = (i + 1) % length
	}
	run.buildVisibility()
	// Start on a group boundary, then descend the rightmost qualifying
	// subtree. A scan of the remaining gap would require 8191 probes.
	const budget = 1 + 1 + 9 + 8 + visibilityBlockSize
	work := NewQueryWork(t.Context(), budget)
	rank, err := run.nextVisible(0, 1, work)
	if err != nil || rank != length-1 || work.remaining != 0 {
		t.Fatalf("rank=%d err=%v remaining=%d", rank, err, work.remaining)
	}
	work = NewQueryWork(t.Context(), budget-1)
	if _, err := run.nextVisible(0, 1, work); !errors.Is(err, ErrQueryWorkExhausted) {
		t.Fatalf("search under budget: %v", err)
	}
}

func TestQuerySnapshotPageReusesMergeLookahead(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	s := new(Store)
	recovery := NewRecoveryBuilder()
	recovered := make([]Row, 32)
	recovered[0] = Row{start, "hit"}
	for i := 1; i < len(recovered); i++ {
		recovered[i] = Row{start.Add(time.Second), "miss"}
	}
	recovery.Add(recovered)
	recovery.Install(s)
	s.Publish([]Row{{start.Add(time.Millisecond), "hit"}})
	const budget = 1 + 6 + 1 + 1 + 2*4 // two runs, two seeks and two matches
	work := NewQueryWork(t.Context(), budget)
	page, err := s.QuerySnapshotPage(start, start.Add(time.Minute), "hit", 1, nil, work)
	if err != nil || !page.HasMore || !slices.Equal(page.Ordinals, []int{0}) || work.remaining != 0 {
		t.Fatalf("page=%+v err=%v remaining=%d", page, err, work.remaining)
	}
}
