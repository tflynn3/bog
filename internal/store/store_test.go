package store

import (
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDecodeRejectsMalformedPrefix(t *testing.T) {
	for _, s := range []string{`{"ts":"2025-01-01T00:00:00Z","line":"ok"}` + "\n" + `{"ts":"bad","line":"x"}`, `{"ts":"2025-01-01T00:00:00Z","ts":"2025-01-01T00:00:00Z","line":"x"}`, `{"ts":"2025-01-01T00:00:00Z","line":"x"}junk`, "\xff"} {
		if _, err := Decode([]byte(s)); err == nil {
			t.Fatalf("accepted %q", s)
		}
	}
}

func TestReservedQueryEnd(t *testing.T) {
	for _, ts := range []string{"9999-12-31T23:59:59.999999999Z", "9999-12-31T22:59:59.999999999-01:00"} {
		if _, err := ParseTime(ts); err != nil {
			t.Fatalf("query bound %s: %v", ts, err)
		}
		if _, err := Decode([]byte(`{"ts":"` + ts + `","line":"x"}`)); err == nil {
			t.Fatalf("accepted reserved timestamp %s", ts)
		}
	}
	if _, err := Decode([]byte(`{"ts":"9999-12-31T23:59:59.999999998Z","line":"x"}`)); err != nil {
		t.Fatal(err)
	}
}
func TestQuerySnapshotAndTies(t *testing.T) {
	var s Store
	now := time.Now().UTC()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			s.Publish([]Row{{now, "a"}, {now, "b"}})
		}
	}()
	for i := 0; i < 100; i++ {
		rows := s.Query(now, now.Add(time.Second), "", 1000)
		if len(rows)%2 != 0 {
			t.Fatal("partial batch")
		}
		for j := 0; j < len(rows); j += 2 {
			if rows[j].Line != "a" || rows[j+1].Line != "b" {
				t.Fatal("tie order")
			}
		}
	}
	wg.Wait()
}

func TestRunIndexParity(t *testing.T) {
	var s Store
	parse := func(v string) time.Time {
		t.Helper()
		x, err := ParseTime(v)
		if err != nil {
			t.Fatal(err)
		}
		return x
	}
	epoch := parse("1970-01-01T00:00:00Z")
	min := parse("0001-01-01T00:00:00Z")
	max := parse("9999-12-31T23:59:59.999999998Z")

	batches := [][]Row{
		{{epoch.Add(-time.Minute), "edge"}, {epoch, "tie first"}, {max, "max"}, {min, "min"}},
		{{epoch.Add(-time.Nanosecond), "late"}, {epoch, "tie second"}, {epoch.Add(32 * time.Minute), "far"}},
	}
	var all []Row
	for _, batch := range batches {
		s.Publish(batch)
		all = append(all, batch...)
	}
	cases := []struct {
		start, end time.Time
		contains   string
		limit      int
	}{
		{epoch.Add(-time.Minute), epoch, "", 10},
		{epoch.Add(-time.Nanosecond), epoch.Add(time.Nanosecond), "", 10},
		{epoch, epoch.Add(32 * time.Minute), "", 1},
		{epoch, epoch.Add(32*time.Minute + time.Nanosecond), "", 10},
		{min, max.Add(time.Nanosecond), "", 10},
		{min, min.Add(time.Minute), "min", 10},
		{max.Add(-time.Second), max.Add(time.Nanosecond), "max", 10},
		{epoch, epoch, "", 10},
		{epoch.Add(-time.Minute), epoch.Add(time.Minute), "tie", 1},
	}
	for _, tc := range cases {
		var want []Row
		for _, row := range all {
			if !row.TS.Before(tc.start) && row.TS.Before(tc.end) && strings.Contains(row.Line, tc.contains) {
				want = append(want, row)
			}
		}
		sort.SliceStable(want, func(i, j int) bool { return want[i].TS.Before(want[j].TS) })
		if len(want) > tc.limit {
			want = want[:tc.limit]
		}
		got := s.Query(tc.start, tc.end, tc.contains, tc.limit)
		if !slices.Equal(got, want) {
			t.Errorf("%s to %s: got %v, want %v", tc.start, tc.end, got, want)
		}
	}
}

func TestPageSnapshotConcurrentBackfill(t *testing.T) {
	s := &Store{}
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	initial := make([]Row, 200)
	for i := range initial {
		initial[i] = Row{TS: start.Add(time.Second), Line: "original"}
	}
	s.Publish(initial)
	rows, count, next, ok := s.QueryPage(start, start.Add(time.Minute), "", 1, nil, nil)
	if !ok || len(rows) != 1 || next == nil {
		t.Fatal("first page")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			s.Publish([]Row{{TS: start, Line: "backfill"}, {TS: start.Add(time.Second), Line: "late"}})
		}
	}()
	seen := len(rows)
	for next != nil {
		var page []Row
		page, _, next, ok = s.QueryPage(start, start.Add(time.Minute), "", 7, &count, next)
		if !ok {
			t.Fatal("invalid snapshot")
		}
		for _, row := range page {
			if row.Line != "original" {
				t.Fatalf("late row: %q", row.Line)
			}
		}
		seen += len(page)
	}
	<-done
	if seen != len(initial) {
		t.Fatalf("got %d rows", seen)
	}
}

func TestSeededRunPaginationParity(t *testing.T) {
	rng := rand.New(rand.NewSource(9817))
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	min := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	max := time.Date(9999, 12, 31, 23, 59, 59, 999999998, time.UTC)
	var s Store
	var all []Row
	for b := 0; b < 93; b++ {
		batch := make([]Row, 1+rng.Intn(13))
		for i := range batch {
			stamp := epoch.Add(time.Duration(rng.Intn(4000)-2000)*time.Second + time.Duration(rng.Intn(3))*time.Nanosecond)
			if b == 0 && i == 0 {
				stamp = min
			}
			if b == 1 && i == 0 {
				stamp = max
			}
			text := fmt.Sprintf("row-%d-%d", b, i)
			if rng.Intn(20) == 0 {
				text += " needle"
			}
			batch[i] = Row{stamp, text}
		}
		s.Publish(batch)
		all = append(all, batch...)
	}
	for _, bounds := range [][2]time.Time{{min, max.Add(time.Nanosecond)}, {epoch.Add(-16 * time.Minute), epoch.Add(16 * time.Minute)}, {epoch.Add(-16 * time.Minute), epoch.Add(16*time.Minute + time.Nanosecond)}, {max.Add(-time.Second), max.Add(time.Nanosecond)}} {
		for _, filter := range []string{"", "needle", "absent"} {
			var want []Row
			for _, r := range all {
				if !r.TS.Before(bounds[0]) && r.TS.Before(bounds[1]) && strings.Contains(r.Line, filter) {
					want = append(want, r)
				}
			}
			sort.SliceStable(want, func(i, j int) bool { return want[i].TS.Before(want[j].TS) })
			got := s.Query(bounds[0], bounds[1], filter, 17)
			n := len(want)
			if n > 17 {
				n = 17
			}
			if !slices.Equal(got, want[:n]) {
				t.Fatal("plain query mismatch")
			}
			var pages []Row
			var count int
			var cursor *Position
			var snap *int
			for {
				page, c, next, ok := s.QueryPage(bounds[0], bounds[1], filter, 7, snap, cursor)
				if !ok {
					t.Fatal("invalid page")
				}
				pages = append(pages, page...)
				count = c
				snap = &count
				cursor = next
				if cursor == nil {
					break
				}
			}
			if !slices.Equal(pages, want) {
				t.Fatalf("paged mismatch for %q: got %d want %d", filter, len(pages), len(want))
			}
		}
	}
}
