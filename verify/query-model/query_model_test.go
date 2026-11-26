// Package querymodel checks snapshot walks against an independent append log.
package querymodel

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"bog/internal/store"
)

type entry struct {
	time    time.Time
	line    string
	ordinal int
}

// appendLog is the reference data, independent of the store and its indexes.
type appendLog []entry

func (log *appendLog) append(rows ...entry) {
	for _, row := range rows {
		row.ordinal = len(*log)
		*log = append(*log, row)
	}
}

// matching freezes a prefix at the high-water mark and orders matching entries
// by instant and original append ordinal, without consulting the store.
func (log appendLog) matching(high int, start, end time.Time, literal string) []entry {
	var result []entry
	for _, row := range log[:high] {
		if !row.time.Before(start) && row.time.Before(end) && strings.Contains(row.line, literal) {
			result = append(result, row)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].time.Equal(result[j].time) {
			return result[i].ordinal < result[j].ordinal
		}
		return result[i].time.Before(result[j].time)
	})
	return result
}

type fixture struct {
	name       string
	seed       int64
	batches    [][]entry
	late       []entry
	start, end time.Time
	literal    string
}

func publish(s *store.Store, rows []entry) {
	batch := make([]store.Row, len(rows))
	for i, row := range rows {
		batch[i] = store.Row{TS: row.time, Line: row.line}
	}
	if len(batch) != 0 {
		s.Publish(batch)
	}
}

func walk(t *testing.T, f fixture, limits []int) {
	t.Helper()
	var s store.Store
	var log appendLog
	for _, batch := range f.batches {
		log.append(batch...)
		publish(&s, batch)
	}
	high := len(log)
	want := log.matching(high, f.start, f.end, f.literal)
	var snapshot *int
	var after *store.Position
	offset := 0
	var requests []string
	// Every non-final page must consume at least one expected row.
	for step := 0; step <= len(want)+1; step++ {
		limit := limits[step%len(limits)]
		requests = append(requests, fmt.Sprintf("{limit:%d snapshot:%v after:%v}", limit, value(snapshot), position(after)))
		fail := func(format string, args ...any) {
			t.Helper()
			t.Fatalf("seed=%d requests=%v: %s", f.seed, requests, fmt.Sprintf(format, args...))
		}
		rows, count, next, ok := s.QueryPage(f.start, f.end, f.literal, limit, snapshot, after)
		if !ok {
			fail("store rejected valid continuation")
		}
		if count != high {
			fail("snapshot count=%d, want %d", count, high)
		}
		end := min(offset+limit, len(want))
		expected := make([]store.Row, 0, end-offset)
		for _, row := range want[offset:end] {
			expected = append(expected, store.Row{TS: row.time, Line: row.line})
		}
		// Time.Equal compares instants even if the source used another zone.
		if len(rows) != len(expected) {
			fail("page rows=%v, want %v", rows, expected)
		}
		for i := range rows {
			if !rows[i].TS.Equal(expected[i].TS) || rows[i].Line != expected[i].Line {
				fail("page row %d=%v, want %v", i, rows[i], expected[i])
			}
		}
		if end == len(want) {
			if next != nil {
				fail("final continuation=%v, want nil", next)
			}
			return
		}
		last := want[end-1]
		expectedPosition := store.Position{Time: last.time, Ordinal: last.ordinal}
		if next == nil || !next.Time.Equal(expectedPosition.Time) || next.Ordinal != expectedPosition.Ordinal {
			fail("continuation=%v, want %v", next, expectedPosition)
		}
		if step == 0 {
			// These rows may sort before, at, or after the cursor, but are outside
			// the frozen prefix and must never appear in this walk.
			log.append(f.late...)
			publish(&s, f.late)
		}
		offset = end
		snapshot = &count
		after = next
	}
	t.Fatalf("seed=%d requests=%v: walk exceeded bound", f.seed, requests)
}

func value(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}
func position(p *store.Position) any {
	if p == nil {
		return nil
	}
	return *p
}

func TestSnapshotPageWalks(t *testing.T) {
	epoch := time.Date(2025, 2, 3, 4, 5, 6, 0, time.UTC)
	at := func(ns int) time.Time { return epoch.Add(time.Duration(ns)) }
	// Distinct offsets represent the same instant and exercise UTC ordering.
	zone := time.FixedZone("west", -7*60*60)
	tie := at(2).In(zone)
	fixed := fixture{
		name: "ties-and-boundaries", seed: 43, start: at(1), end: at(5), literal: "needle",
		batches: [][]entry{
			{{time: at(0), line: "needle outside"}, {time: at(1), line: "needle start"}, {time: at(2), line: "needle duplicate"}, {time: tie, line: "needle duplicate"}},
			{{time: at(4), line: "needle last"}, {time: at(5), line: "needle end"}, {time: at(2), line: "needle tie"}, {time: at(3), line: "other"}},
		},
		late: []entry{{time: at(1), line: "needle late before"}, {time: at(2), line: "needle late tie"}, {time: at(4), line: "needle late after"}},
	}
	fixtures := []fixture{fixed}
	empty := fixed
	empty.name, empty.literal = "empty-matches", "not present"
	fixtures = append(fixtures, empty)
	emptyLog := fixed
	emptyLog.name, emptyLog.batches, emptyLog.late = "empty-log", nil, nil
	fixtures = append(fixtures, emptyLog)
	for _, seed := range []int64{1, 17, 91, 2025} {
		rng := rand.New(rand.NewSource(seed))
		f := fixture{name: fmt.Sprintf("runs-%d", seed), seed: seed, start: at(3), end: at(25), literal: "needle"}
		// Each batch is a separate publication: alternate ordered runs and backfill.
		for b := 0; b < 6; b++ {
			var batch []entry
			for i := 0; i < 12; i++ {
				n := rng.Intn(30)
				line := "other"
				if rng.Intn(3) != 0 {
					line = fmt.Sprintf("needle %d", rng.Intn(4))
				}
				batch = append(batch, entry{time: at(n), line: line})
			}
			if b%2 == 0 {
				sort.SliceStable(batch, func(i, j int) bool { return batch[i].time.Before(batch[j].time) })
			}
			f.batches = append(f.batches, batch)
		}
		f.late = []entry{{time: at(3), line: "needle backfill"}, {time: at(20), line: "needle late"}}
		fixtures = append(fixtures, f)
	}
	for _, f := range fixtures {
		for _, limits := range [][]int{{1}, {2}, {7}, {1000}, {1, 4, 2, 9}} {
			t.Run(fmt.Sprintf("%s/limits-%v", f.name, limits), func(t *testing.T) { walk(t, f, limits) })
		}
	}
}

// Guard the reference model's duplicate and frozen-prefix behavior independently
// of Store.QueryPage, so it can also be used by checkpoint and retention tests.
func TestReferenceModel(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	var log appendLog
	log.append(entry{time: now.Add(time.Nanosecond), line: "x"}, entry{time: now, line: "x"}, entry{time: now, line: "x"})
	high := len(log)
	log.append(entry{time: now, line: "x"})
	got := log.matching(high, now, now.Add(2*time.Nanosecond), "x")
	var ordinals []int
	for _, row := range got {
		ordinals = append(ordinals, row.ordinal)
	}
	if !reflect.DeepEqual(ordinals, []int{1, 2, 0}) {
		t.Fatalf("ordinals=%v", ordinals)
	}
}
