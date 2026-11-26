package querymodel_test

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	model "bog/verify/query-model"
)

func TestLogMatch(t *testing.T) {
	west := time.FixedZone("west", -7*60*60)
	tie := epoch.Add(time.Nanosecond)
	duplicate := model.Row{Time: tie, Line: "contains .*"}
	batch := []model.Row{
		{Time: tie.In(west), Line: "contains .* offset"},
		{Time: epoch.Add(-time.Nanosecond), Line: "before .*"},
		{Time: epoch, Line: "lower .*"},
		duplicate,
		duplicate,
		{Time: epoch.Add(2 * time.Nanosecond), Line: "upper .*"},
		{Time: epoch, Line: "Contains"},
	}
	var log model.Log
	pin := log.Append(batch)
	batch[2].Line = "changed"
	log.Append([]model.Row{{Time: epoch, Line: "late .*"}})
	bounds := model.Filter{Start: epoch.In(west), End: epoch.Add(2 * time.Nanosecond).In(west), Contains: ".*"}
	for _, tt := range []struct {
		name      string
		highWater int
		literal   string
		ordinals  []int
	}{
		{name: "pinned", highWater: pin, literal: ".*", ordinals: []int{2, 0, 3, 4}},
		{name: "fresh", highWater: log.HighWater(), literal: ".*", ordinals: []int{2, 7, 0, 3, 4}},
		{name: "case", highWater: pin, literal: "Contains", ordinals: []int{6}},
		{name: "all literals", highWater: pin, ordinals: []int{2, 6, 0, 3, 4}},
		{name: "empty prefix", highWater: 0, literal: ".*"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			filter := bounds
			filter.Contains = tt.literal
			got, err := log.Match(tt.highWater, filter)
			if err != nil {
				t.Fatal(err)
			}
			var ordinals []int
			for _, entry := range got {
				ordinals = append(ordinals, entry.Ordinal)
				if entry.Time.Location() != time.UTC || !strings.Contains(entry.Line, tt.literal) {
					t.Fatalf("unexpected match %+v", entry)
				}
			}
			if !slices.Equal(ordinals, tt.ordinals) {
				t.Fatalf("ordinals = %v, want %v", ordinals, tt.ordinals)
			}
		})
	}

	for _, pin := range []int{-1, log.HighWater() + 1} {
		if _, err := log.Match(pin, bounds); err == nil {
			t.Fatalf("accepted invalid high-water mark %d", pin)
		}
	}
}

func TestSeededBatches(t *testing.T) {
	const seed, count = -17, 513
	ordered := model.SeededBatches(seed, epoch, count, true)
	shuffled := model.SeededBatches(seed, epoch, count, false)
	if !reflect.DeepEqual(shuffled, model.SeededBatches(seed, epoch, count, false)) {
		t.Fatalf("seed=%d: fixture changed across calls", seed)
	}
	if reflect.DeepEqual(shuffled, model.SeededBatches(seed+1, epoch, count, false)) {
		t.Fatalf("seed=%d: changing the seed left the fixture unchanged", seed)
	}

	var orderedRows, shuffledRows []model.Row
	if len(ordered) != len(shuffled) {
		t.Fatalf("seed=%d: batch counts differ (%d, %d)", seed, len(ordered), len(shuffled))
	}
	for i, batch := range ordered {
		if len(batch) == 0 || len(batch) > 64 {
			t.Fatalf("seed=%d: unexpected batch size %d", seed, len(batch))
		}
		if len(batch) != len(shuffled[i]) {
			t.Fatalf("seed=%d: batch %d sizes differ (%d, %d)", seed, i, len(batch), len(shuffled[i]))
		}
		orderedRows = append(orderedRows, batch...)
		shuffledRows = append(shuffledRows, shuffled[i]...)
	}
	if len(orderedRows) != count || len(shuffledRows) != count {
		t.Fatalf("seed=%d: wrong row counts (%d, %d)", seed, len(orderedRows), len(shuffledRows))
	}

	multiset := make(map[model.Row]int)
	for i, row := range orderedRows {
		multiset[row]++
		if !row.Time.Equal(epoch.Add(time.Duration(i/4) * time.Millisecond)) {
			t.Fatalf("seed=%d: out-of-order timestamp at row %d: %s", seed, i, row.Time)
		}
		if i%4 == 1 && row != orderedRows[i-1] {
			t.Fatalf("seed=%d: missing exact duplicate at row %d", seed, i)
		}
		if i%4 == 2 && row.Time.Location() == time.UTC {
			t.Fatalf("seed=%d: missing alternate offset at row %d", seed, i)
		}
	}
	if slices.IsSortedFunc(shuffledRows, func(a, b model.Row) int { return a.Time.Compare(b.Time) }) {
		t.Fatalf("seed=%d: shuffled fixture is in timestamp order", seed)
	}
	for _, row := range shuffledRows {
		multiset[row]--
	}
	for row, delta := range multiset {
		if delta != 0 {
			t.Fatalf("seed=%d: multiplicity changed by %d for %+v", seed, delta, row)
		}
	}
	if got := model.SeededBatches(seed, epoch, 0, true); len(got) != 0 {
		t.Fatalf("seed=%d: got %d batches for an empty fixture", seed, len(got))
	}
}

func TestVerifyWalkRejectsBadPages(t *testing.T) {
	const seed = 43
	row := model.Row{Time: epoch, Line: "duplicate"}
	var log model.Log
	pin := log.Append([]model.Row{row, row, row})
	filter := model.Filter{Start: epoch, End: epoch.Add(time.Second)}
	queryErr := errors.New("query failed")
	for _, tt := range []struct {
		name, want string
		mutate     func(*model.Page) error
	}{
		{name: "adapter error", want: "query failed", mutate: func(*model.Page) error { return queryErr }},
		{name: "snapshot drift", want: "high-water mark", mutate: func(p *model.Page) error { p.HighWater++; return nil }},
		{name: "omitted duplicate", want: "got 0 rows", mutate: func(p *model.Page) error { p.Rows = nil; return nil }},
		{name: "changed row", want: "append ordinal 1", mutate: func(p *model.Page) error { p.Rows[0].Line = "Duplicate"; return nil }},
		{name: "premature end", want: "missing continuation", mutate: func(p *model.Page) error { p.Next = nil; return nil }},
		{name: "wrong ordinal at tie", want: "got continuation", mutate: func(p *model.Page) error { p.Next.Ordinal = 0; return nil }},
		{name: "wrong cursor instant", want: "got continuation", mutate: func(p *model.Page) error { p.Next.Time = epoch.Add(time.Nanosecond); return nil }},
		{name: "cursor on full terminal page", want: "terminal page has cursor"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			err := model.VerifyWalk(seed, &log, pin, filter, []int{1}, func(req model.Request) (model.Page, error) {
				calls++
				page := model.Page{Rows: []model.Row{row}, HighWater: pin,
					Next: &model.Position{Time: epoch, Ordinal: calls - 1}}
				if calls == 2 && tt.mutate != nil {
					err := tt.mutate(&page)
					return page, err
				}
				return page, nil
			})
			if err == nil {
				t.Fatal("accepted a bad page")
			}
			for _, fragment := range []string{tt.want, "seed=43", "request sequence", "limit=1 cursor=nil", "high_water=3 after=", "Ordinal:0"} {
				if !strings.Contains(err.Error(), fragment) {
					t.Errorf("error missing %q: %v", fragment, err)
				}
			}
			if tt.name == "adapter error" && !errors.Is(err, queryErr) {
				t.Errorf("error does not wrap adapter failure: %v", err)
			}
			wantCalls := 2
			if tt.mutate == nil {
				wantCalls = 3
			}
			if calls != wantCalls {
				t.Errorf("made %d requests, want %d", calls, wantCalls)
			}
		})
	}
}

func TestVerifyWalkRejectsInvalidSetup(t *testing.T) {
	for _, tt := range []struct {
		pin    int
		limits []int
	}{
		{pin: 0},
		{pin: 0, limits: []int{1, 0}},
		{pin: 0, limits: []int{-1}},
		{pin: 1, limits: []int{1}},
	} {
		err := model.VerifyWalk(43, new(model.Log), tt.pin, model.Filter{}, tt.limits, func(model.Request) (model.Page, error) {
			t.Fatal("queried with invalid setup")
			return model.Page{}, nil
		})
		if err == nil || !strings.Contains(err.Error(), "request sequence (0 requests)") {
			t.Errorf("pin=%d limits=%v: got %v, want setup error", tt.pin, tt.limits, err)
		}
	}
}
