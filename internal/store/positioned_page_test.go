package store

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	querymodel "bog/verify/query-model"
)

func TestPositionedPageWalk(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	tie := start.Add(time.Second)
	end := start.Add(3 * time.Second)
	const first = uint64(math.MaxUint64 - 20)
	localTie := tie.In(time.FixedZone("west", -7*60*60))
	original := []Row{
		{tie, "hit duplicate"},
		{tie, "hit duplicate"},
		{end, "hit excluded end"},
		{localTie, "HIT wrong case"},
		{start, "hit backfill at start"},
		{localTie, "hit duplicate"},
		{start.Add(2 * time.Second), "hit last"},
		{start.Add(-time.Second), "hit excluded before start"},
	}
	var s Store
	var log querymodel.Log
	replay := NewRecoveryBuilder()
	replay.Add(original[:3])
	replay.Install(&s)
	recordPositionedRows(&log, original[:3])
	s.Publish(original[3:])
	recordPositionedRows(&log, original[3:])
	count := s.PublishedCount()
	later := []Row{{tie, "hit later tie"}, {start, "hit later backfill"}, {start.Add(2 * time.Second), "hit later time"}}
	s.Publish(later)
	recordPositionedRows(&log, later)
	if got := s.PublishedCount(); count != len(original) || got != len(original)+len(later) {
		t.Fatalf("published counts before and after append = %d and %d", count, got)
	}

	for _, tc := range []struct {
		name, literal string
		after         *TailPosition
		prefix        int
	}{
		{"first page of fixed prefix", "hit", nil, count},
		{"checkpoint tie", "hit", &TailPosition{localTie, first - 1}, count},
		{"between duplicates", "hit", &TailPosition{localTie, first}, count},
		{"nonmatching lower bound", "hit", &TailPosition{tie, first + 3}, count},
		{"ordinal beyond prefix at tie", "hit", &TailPosition{tie, math.MaxUint64}, count},
		{"large ordinal before range", "hit", &TailPosition{start.Add(-time.Second), math.MaxUint64}, count},
		{"bound at exclusive end", "hit", &TailPosition{end, 0}, count},
		{"empty literal", "", nil, count},
		{"absent literal", "absent", nil, count},
		{"empty prefix", "", &TailPosition{tie, first - 1}, 0},
		{"fresh prefix", "hit", nil, s.PublishedCount()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filter := querymodel.Filter{Start: start, End: end, Contains: tc.literal}
			want := modelPositionedRows(t, &log, tc.prefix, first, filter, tc.after)
			after := tc.after
			limits := []int{2, 1, 3}
			for page, offset := 0, 0; ; page++ {
				limit := limits[page%len(limits)]
				last := min(offset+limit, len(want))
				got, more, err := s.QueryPositionedPage(start, end, tc.literal, limit, tc.prefix, first, after, NewQueryWork(t.Context(), 1000))
				checkPositionedPage(t, got, more, err, want[offset:last], last < len(want))
				if !more {
					break
				}
				after = &got[len(got)-1].Position
				offset = last
			}
		})
	}

	// A caller can trim a page to the first returned row and resume from it.
	filter := querymodel.Filter{Start: start, End: end, Contains: "hit"}
	want := modelPositionedRows(t, &log, count, first, filter, nil)
	got, more, err := s.QueryPositionedPage(start, end, "hit", 3, count, first, nil, nil)
	checkPositionedPage(t, got, more, err, want[:3], true)
	rest, more, err := s.QueryPositionedPage(start, end, "hit", 10, count, first, &got[0].Position, nil)
	checkPositionedPage(t, rest, more, err, want[1:], false)
}

func TestPositionedPageTraversal(t *testing.T) {
	start := time.Unix(2000, 0).UTC()
	end := start.Add(time.Second)
	const count = 36864
	for _, tc := range []struct {
		name                  string
		shuffled, multiple    bool
		literal, nonmatchLine string
	}{
		{"implicit direct Bloom", false, false, "needle", "xxxx"},
		{"explicit block candidates", true, false, "needle", "xxxx"},
		{"multiple implicit Bloom", false, true, "needle", "xxxx"},
		{"multiple explicit block candidates", true, true, "needle", "xxxx"},
		{"merge after Bloom false positives", true, true, "abcdef", "abcd bcde cdef"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := make([]Row, count)
			for i := range rows {
				tick := i
				if tc.multiple {
					tick %= count / 2 // Overlap the recovered and live runs.
				}
				rows[i] = Row{start.Add(time.Duration(tick)), tc.nonmatchLine}
			}
			if tc.shuffled {
				rows[20].TS, rows[21].TS = rows[21].TS, rows[20].TS
			}
			for _, i := range []int{0, 40, count - 1} {
				rows[i].Line = tc.literal
			}
			var s Store
			if tc.multiple {
				replay := NewRecoveryBuilder()
				replay.Add(rows[:count/2])
				replay.Install(&s)
				s.Publish(rows[count/2:])
			} else {
				s.Publish(rows)
			}
			s.Publish([]Row{{start, tc.literal}, {end.Add(-time.Nanosecond), tc.literal}})

			const first = 80000
			var log querymodel.Log
			recordPositionedRows(&log, rows)
			filter := querymodel.Filter{Start: start, End: end, Contains: tc.literal}
			after := TailPosition{start, first - 1}
			want := modelPositionedRows(t, &log, count, first, filter, &after)
			got, more, err := s.QueryPositionedPage(start, end, tc.literal, 2, count, first, &after, NewQueryWork(t.Context(), 1<<20))
			checkPositionedPage(t, got, more, err, want[:2], true)
			got, more, err = s.QueryPositionedPage(start, end, tc.literal, 1, count, first, &got[1].Position, NewQueryWork(t.Context(), 1<<20))
			checkPositionedPage(t, got, more, err, want[2:], false)
		})
	}
}

func TestPositionedPageWork(t *testing.T) {
	start := time.Unix(3000, 0).UTC()
	end := start.Add(time.Second)
	for _, last := range []string{"hit", "abc"} {
		t.Run(last, func(t *testing.T) {
			var s Store
			s.Publish([]Row{{start, "hit"}, {start, "abc"}, {start, last}})
			// One run and a two-unit seek, then four units per visited row.
			for _, tc := range []struct {
				name   string
				budget uint64
				cancel int
				err    error
			}{
				{"before scan", 0, 0, ErrQueryWorkExhausted},
				{"during lookahead", 14, 0, ErrQueryWorkExhausted},
				{"initial cancellation", 15, 1, context.Canceled},
				{"lookahead cancellation", 15, 6, context.Canceled},
				{"exact budget", 15, 0, nil},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					work := NewQueryWork(&cancelDuringWork{ctx, cancel, tc.cancel}, tc.budget)
					got, more, err := s.QueryPositionedPage(start, end, "hit", 1, 3, 10, nil, work)
					if tc.err != nil {
						if !errors.Is(err, tc.err) || got != nil || more {
							t.Fatalf("failed page = %v, %v, %v, want nil, false, %v", got, more, err, tc.err)
						}
						return
					}
					want := []PositionedRow{{Row{start, "hit"}, TailPosition{start, 10}}}
					checkPositionedPage(t, got, more, err, want, last == "hit")
				})
			}
		})
	}
}

func TestPositionedPageBounds(t *testing.T) {
	start := time.Unix(4000, 0).UTC()
	end := start.Add(time.Second)
	var s Store
	s.Publish([]Row{{start, "hit"}, {start, "hit"}})
	for _, tc := range []struct {
		name         string
		count, limit int
		first        uint64
	}{
		{"negative count", -1, 1, 0},
		{"unpublished count", 3, 1, 0},
		{"zero limit", 2, 0, 0},
		{"negative limit", 2, -1, 0},
		{"ordinal overflow", 2, 1, math.MaxUint64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, more, err := s.QueryPositionedPage(start, end, "", tc.limit, tc.count, tc.first, nil, nil)
			if err == nil || got != nil || more {
				t.Fatalf("invalid page = %v, %v, %v", got, more, err)
			}
		})
	}
	got, more, err := s.QueryPositionedPage(start, end, "", 1, 2, math.MaxUint64-1, &TailPosition{start, math.MaxUint64 - 1}, nil)
	checkPositionedPage(t, got, more, err, []PositionedRow{{Row{start, "hit"}, TailPosition{start, math.MaxUint64}}}, false)
	got, more, err = s.QueryPositionedPage(start, start, "", 1, 2, 0, nil, nil)
	checkPositionedPage(t, got, more, err, nil, false)
}

func TestPositionedPageConcurrentPublish(t *testing.T) {
	start := time.Unix(5000, 0).UTC()
	end := start.Add(time.Second)
	const first = 1500
	rows := make([]Row, 257)
	for i := range rows {
		rows[i] = Row{start.Add(time.Duration(i % 7)), "hit"}
	}
	builder := NewRecoveryBuilder()
	builder.Add(rows)
	var s Store
	builder.Install(&s)
	count := s.PublishedCount()
	var log querymodel.Log
	recordPositionedRows(&log, rows)
	filter := querymodel.Filter{Start: start, End: end, Contains: "hit"}
	want := modelPositionedRows(t, &log, count, first, filter, nil)

	var writer sync.WaitGroup
	writer.Go(func() {
		for i := 0; i < 256; i++ {
			s.Publish([]Row{{start.Add(time.Duration(i % 11)), "hit live"}, {start, "hit live backfill"}})
		}
	})
	defer writer.Wait()

	var after *TailPosition
	for offset := 0; offset < len(want); {
		last := min(offset+3, len(want))
		got, more, err := s.QueryPositionedPage(start, end, "hit", 3, count, first, after, NewQueryWork(t.Context(), 1<<16))
		checkPositionedPage(t, got, more, err, want[offset:last], last < len(want))
		after = &got[len(got)-1].Position
		offset = last
	}
}

func recordPositionedRows(log *querymodel.Log, rows []Row) {
	input := make([]querymodel.Row, len(rows))
	for i, row := range rows {
		input[i] = querymodel.Row{Time: row.TS, Line: row.Line}
	}
	log.Append(input)
}

func modelPositionedRows(t *testing.T, log *querymodel.Log, count int, first uint64, filter querymodel.Filter, after *TailPosition) []PositionedRow {
	t.Helper()
	entries, err := log.Match(count, filter)
	if err != nil {
		t.Fatal(err)
	}
	var want []PositionedRow
	for _, entry := range entries {
		key := TailPosition{entry.Time, first + uint64(entry.Ordinal)}
		if after != nil && (key.Time.Before(after.Time) || (key.Time.Equal(after.Time) && key.Ordinal <= after.Ordinal)) {
			continue
		}
		want = append(want, PositionedRow{Row{entry.Time, entry.Line}, key})
	}
	return want
}

func checkPositionedPage(t *testing.T, rows []PositionedRow, more bool, err error, want []PositionedRow, wantMore bool) {
	t.Helper()
	if err != nil || len(rows) != len(want) || more != wantMore {
		t.Fatalf("page = (%v, %v, %v), want (%v, %v, nil)", rows, more, err, want, wantMore)
	}
	for i, got := range rows {
		if !got.Row.TS.Equal(want[i].Row.TS) || got.Row.Line != want[i].Row.Line || got.Position != want[i].Position {
			t.Fatalf("row %d = %v, want %v", i, got, want[i])
		}
	}
}
