package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestQuerySnapshotPagePositions(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	end := start.Add(10 * time.Second)

	for _, layout := range []string{"implicit", "explicit", "merged", "bloom"} {
		t.Run(layout, func(t *testing.T) {
			var log []Row
			var s Store
			switch layout {
			case "implicit":
				log = []Row{{start, "needle"}, {start, "needle"}, {start.Add(time.Second), "needle"}}
				s.Publish(log)
			case "explicit", "merged":
				log = []Row{{start.Add(time.Second), "needle"}, {start, "needle"}, {start, "needle"}, {start, "miss"}}
				if layout == "merged" {
					b := NewRecoveryBuilder()
					b.Add(log[:2])
					b.Install(&s)
					s.Publish(log[2:])
				} else {
					s.Publish(log)
				}
			case "bloom":
				// A matching row is retained by the bounded scan before it
				// switches to block selection. Its ordinal must appear once.
				log = make([]Row, 1<<15)
				for i := range log {
					log[i] = Row{start.Add(time.Duration(i^1) * time.Nanosecond), "miss"}
				}
				for _, ordinal := range []int{0, len(log) - 2, len(log) - 1} {
					log[ordinal].Line = "needle"
				}
				s.Publish(log)
			}
			want := snapshotOrdinals(log, start, end, "needle")
			var cursor *SnapshotCursor
			for offset := 0; offset < len(want); offset += 2 {
				page, err := s.QuerySnapshotPage(start, end, "needle", 2, cursor, nil)
				last := min(len(want), offset+2)
				if err != nil || page.Count != len(log) || page.More != (last < len(want)) ||
					!slices.Equal(page.Ordinals, want[offset:last]) || len(page.Rows) != last-offset {
					t.Fatalf("offset %d: page %+v, error %v, want ordinals %v", offset, page, err, want[offset:last])
				}
				for i, ordinal := range page.Ordinals {
					if page.Rows[i] != log[ordinal] {
						t.Fatalf("row %d = %v, want %v at ordinal %d", i, page.Rows[i], log[ordinal], ordinal)
					}
				}
				// Deliberately continue from the first emitted row when a byte
				// limit could have omitted the second one.
				if offset == 0 && len(page.Ordinals) > 1 {
					next, err := s.QuerySnapshotPage(start, end, "needle", 1,
						&SnapshotCursor{page.Count, page.Ordinals[0]}, nil)
					if err != nil || !slices.Equal(next.Ordinals, want[1:2]) {
						t.Fatalf("resume after first emitted row: %+v, %v", next, err)
					}
				}
				cursor = &SnapshotCursor{page.Count, page.Ordinals[len(page.Ordinals)-1]}
				s.Publish([]Row{{start, "needle late"}})
			}
		})
	}
}

func TestSnapshotPageWorkPhases(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	end := start.Add(time.Second)
	hit := Row{start, "hit"}
	var s Store
	s.Publish([]Row{hit, hit, hit})

	for _, tc := range []struct {
		name   string
		cursor SnapshotCursor
		budget uint64
		want   error
	}{
		{"count outside published prefix", SnapshotCursor{4, 0}, 99, ErrInvalidQueryCursor},
		{"ordinal outside snapshot", SnapshotCursor{2, 2}, 99, ErrInvalidQueryCursor},
		{"negative ordinal", SnapshotCursor{3, -1}, 99, ErrInvalidQueryCursor},
		{"anchor validation", SnapshotCursor{3, 0}, 3, ErrQueryWorkExhausted},
		{"run visit", SnapshotCursor{3, 0}, 4, ErrQueryWorkExhausted},
		{"implicit seek", SnapshotCursor{3, 0}, 5, ErrQueryWorkExhausted},
		{"candidate", SnapshotCursor{3, 0}, 9, ErrQueryWorkExhausted},
		{"lookahead", SnapshotCursor{3, 0}, 13, ErrQueryWorkExhausted},
		{"exact", SnapshotCursor{3, 0}, 14, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			page, err := s.QuerySnapshotPage(start, end, "hit", 1, &tc.cursor, NewQueryWork(t.Context(), tc.budget))
			if !errors.Is(err, tc.want) {
				t.Fatalf("error %v, want %v", err, tc.want)
			}
			if err != nil {
				if page.Count != 0 || page.More || page.Rows != nil || page.Ordinals != nil {
					t.Fatalf("partial page %+v", page)
				}
			} else if page.Count != 3 || !page.More || !slices.Equal(page.Ordinals, []int{1}) {
				t.Fatalf("got %+v", page)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.QuerySnapshotPage(start, end, "", 1, nil, NewQueryWork(ctx, 100)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled snapshot: %v", err)
	}
}

func TestVisibilitySeekAgainstLinearProbe(t *testing.T) {
	rng := rand.New(rand.NewPCG(52, 1))
	for _, length := range []int{1, 15, 16, 17, 50, 257, 1025} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			run := indexRun{length: length, ordinals: rng.Perm(length)}
			run.buildVisibility()
			for _, count := range []int{0, 1, 2, length / 4, length / 2, length} {
				want := length
				for i := length; i >= 0; i-- {
					if i < length && run.ordinals[i] < count {
						want = i
					}
					got, err := run.nextVisible(i, count, nil)
					if err != nil || got != want {
						t.Fatalf("offset %d, count %d: got %d, %v, want %d", i, count, got, err, want)
					}
				}
			}
		})
	}
}

func TestSnapshotAnchorVerifiesFilter(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	end := start.Add(time.Second)
	var s Store
	s.Publish([]Row{{start, strings.Repeat("miss", 8)}, {start, "hit"}})
	cursor := SnapshotCursor{Count: 2, Ordinal: 0}
	if _, err := s.QuerySnapshotPage(start, end, "hit", 1, &cursor, NewQueryWork(t.Context(), 33)); !errors.Is(err, ErrInvalidQueryCursor) {
		t.Fatalf("nonmatching anchor: %v", err)
	}
}

func TestSnapshotVisibilityWorkBoundary(t *testing.T) {
	const length = 64
	run := indexRun{length: length, ordinals: make([]int, length)}
	for i := range run.ordinals {
		run.ordinals[i] = (i + 16) % length
	}
	run.buildVisibility()
	for _, tc := range []struct {
		name   string
		offset int
		units  uint64
	}{
		{"adjacent block", 47, 3},
		{"skip invisible subtree", 15, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := run.nextVisible(tc.offset, 1, NewQueryWork(t.Context(), tc.units-1)); !errors.Is(err, ErrQueryWorkExhausted) {
				t.Fatalf("one unit short: %v", err)
			}
			rank, err := run.nextVisible(tc.offset, 1, NewQueryWork(t.Context(), tc.units))
			if err != nil || rank != 48 {
				t.Fatalf("exact budget: rank %d, err %v", rank, err)
			}
		})
	}
}
