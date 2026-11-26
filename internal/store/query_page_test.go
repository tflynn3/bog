package store

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

func TestSnapshotPageOrdinals(t *testing.T) {
	epoch := time.Unix(0, 0)
	at := func(n int) time.Time { return epoch.Add(time.Duration(n) * time.Second) }
	s := new(Store)
	s.Publish([]Row{{at(2), "hit"}, {at(1), "hit"}, {at(1), "miss"}, {at(1), "hit"}})
	first, err := s.QuerySnapshotPageWithWork(at(0), at(3), "hit", 2, nil, nil, nil)
	if err != nil || first.HighWater != 4 || !first.More || !slices.Equal(first.Ordinals, []int{1, 3}) ||
		!slices.Equal(first.Rows, []Row{{at(1), "hit"}, {at(1), "hit"}}) {
		t.Fatalf("first page = %+v, %v", first, err)
	}
	s.Publish([]Row{{at(0), "hit"}, {at(1), "hit"}, {at(2), "hit"}})

	// Resuming from the first returned row models byte truncation. Its
	// successor remains ordinal 3 even after the run has merged.
	next, err := s.QuerySnapshotPageWithWork(at(0), at(3), "hit", 2, &first.HighWater, &first.Ordinals[0], nil)
	if err != nil || next.HighWater != first.HighWater || next.More || !slices.Equal(next.Ordinals, []int{3, 0}) ||
		!slices.Equal(next.Rows, []Row{{at(1), "hit"}, {at(2), "hit"}}) {
		t.Fatalf("continuation = %+v, %v", next, err)
	}

	terminal, err := s.QuerySnapshotPageWithWork(at(0), at(3), "hit", 2, &first.HighWater, &next.Ordinals[1], nil)
	if err != nil || terminal.HighWater != first.HighWater || terminal.More || len(terminal.Rows) != 0 || len(terminal.Ordinals) != 0 {
		t.Fatalf("empty continuation = %+v, %v", terminal, err)
	}
}

func TestSnapshotPageAnchorValidation(t *testing.T) {
	epoch := time.Unix(0, 0)
	start, end := epoch.Add(time.Second), epoch.Add(3*time.Second)
	s := new(Store)
	s.Publish([]Row{{epoch, "hit"}, {start, "hit"}, {start, "miss"}, {end, "hit"}})
	for _, tc := range []struct {
		name    string
		count   *int
		ordinal *int
		want    error
	}{
		{"missing snapshot", nil, new(1), ErrInvalidQueryCursor},
		{"missing anchor", new(4), nil, ErrInvalidQueryCursor},
		{"negative snapshot", new(-1), new(1), ErrInvalidQueryCursor},
		{"future snapshot", new(5), new(1), ErrInvalidQueryCursor},
		{"negative ordinal", new(4), new(-1), ErrInvalidQueryCursor},
		{"ordinal outside snapshot", new(1), new(1), ErrInvalidQueryCursor},
		{"before start", new(4), new(0), ErrInvalidQueryCursor},
		{"filter mismatch", new(4), new(2), ErrInvalidQueryCursor},
		{"at exclusive end", new(4), new(3), ErrInvalidQueryCursor},
		{"at inclusive start", new(4), new(1), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			page, err := s.QuerySnapshotPageWithWork(start, end, "hit", 1, tc.count, tc.ordinal, nil)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %+v, %v; want %v", page, err, tc.want)
			}
			if err != nil && (page.Rows != nil || page.Ordinals != nil || page.More || page.HighWater != 0) {
				t.Fatalf("partial result on error: %+v", page)
			}
		})
	}
}

func TestVisibilitySuccessorAgainstOrdinalScan(t *testing.T) {
	const base = 91
	rng := rand.New(rand.NewPCG(11, 52))
	for _, length := range []int{1, 15, 16, 17, 40, 255, 256, 257, 8193} {
		r := indexRun{base: base, length: length, ordinals: rng.Perm(length)}
		for i := range r.ordinals {
			r.ordinals[i] += base
		}
		r.buildVisibility()
		for _, visible := range []int{1, 2, length / 2, length - 1, length} {
			count := base + max(0, visible)
			want := length
			for rank := length; rank >= 0; rank-- {
				if rank < length && r.ordinals[rank] < count {
					want = rank
				}
				got, err := r.nextVisible(rank, count, nil)
				if err != nil || got != want {
					t.Fatalf("length %d count %d rank %d: got %d, %v; want %d", length, count, rank, got, err, want)
				}
			}
		}
	}
}

func TestSnapshotPageVisibilityAndLookaheadBudget(t *testing.T) {
	stamp := time.Unix(0, 0)
	row := func(i int) Row { return Row{stamp.Add(time.Duration(i) * time.Second), "x"} }
	s := new(Store)
	s.Publish([]Row{row(0), row(2), row(4)})
	s.Publish([]Row{row(1), row(3), row(5)})
	// Anchor (2), run (1), seek (3), initial visibility seek (2),
	// returned row (2), visibility seek (2), lookahead row (2).
	for budget := uint64(0); budget <= 14; budget++ {
		work := NewQueryWork(t.Context(), budget)
		got, err := s.QuerySnapshotPageWithWork(stamp, stamp.Add(time.Minute), "x", 1, new(3), new(0), work)
		if budget < 14 {
			if !errors.Is(err, ErrQueryWorkExhausted) || got.Rows != nil || got.Ordinals != nil {
				t.Fatalf("budget %d: got %+v, %v", budget, got, err)
			}
			continue
		}
		if err != nil || !got.More || !slices.Equal(got.Ordinals, []int{1}) || work.remaining != 0 {
			t.Fatalf("exact budget: got %+v, %v; %d units left", got, err, work.remaining)
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	meter := NewQueryWork(&cancelDuringWork{Context: ctx, cancel: cancel, checks: 5}, 14)
	got, err := s.QuerySnapshotPageWithWork(stamp, stamp.Add(time.Minute), "x", 1, new(3), new(0), meter)
	if !errors.Is(err, context.Canceled) || got.Rows != nil || got.Ordinals != nil {
		t.Fatalf("cancelled visibility: got %+v, %v", got, err)
	}
}
