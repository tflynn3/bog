package store

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	model "bog/verify/query-model"
)

func TestPositionedSnapshotKeepsScanWorkAcrossAppends(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Second)
	rows := []Row{
		{start.Add(5), "hit tie"},
		{start.Add(2), "miss"},
		{start.Add(5), "hit tie"},
		{start.Add(1), "hit first"},
		{start.Add(8), "hit last"},
	}
	builder := NewRecoveryBuilder()
	builder.Add(rows[:2])
	s := new(Store)
	builder.Install(s)
	for _, row := range rows[2:] {
		s.Publish([]Row{row})
	}
	view := s.CapturePositionedSnapshot()
	const firstOrdinal = 240
	var log model.Log
	for _, row := range rows {
		log.Append([]model.Row{{Time: row.TS, Line: row.Line}})
	}
	oracle, err := log.Match(log.HighWater(), model.Filter{Start: start, End: end, Contains: "hit"})
	if err != nil {
		t.Fatal(err)
	}

	query := func(after *TailPosition, limit int, budget uint64) ([]PositionedRow, bool, error) {
		return view.Page(start, end, "hit", limit, firstOrdinal, after, NewQueryWork(t.Context(), budget))
	}
	type scan struct {
		anchor *TailPosition
		limit  int
		budget uint64
		rows   []PositionedRow
		more   bool
	}
	var scans []scan
	for _, limit := range []int{1, 2, 3} {
		var anchor *TailPosition
		for offset := 0; offset < len(oracle); {
			var budget uint64
			for budget = 1; budget < 1024; budget++ {
				page, more, err := query(anchor, limit, budget)
				if errors.Is(err, ErrQueryWorkExhausted) {
					if len(page) != 0 || more {
						t.Fatal("budget error leaked a partial page")
					}
					continue
				}
				next := min(offset+limit, len(oracle))
				if err != nil || len(page) != next-offset || more != (next < len(oracle)) {
					t.Fatalf("position %d, limit %d: page=%v more=%v err=%v", offset, limit, page, more, err)
				}
				for i, entry := range oracle[offset:next] {
					want := PositionedRow{Row{entry.Time, entry.Line}, TailPosition{entry.Time, firstOrdinal + uint64(entry.Ordinal)}}
					if !reflect.DeepEqual(page[i], want) {
						t.Fatalf("position %d: got %+v, want %+v", offset+i, page[i], want)
					}
				}
				scans = append(scans, scan{anchor, limit, budget, page, more})
				offset, anchor = next, &page[len(page)-1].Position
				break
			}
			if budget == 1024 {
				t.Fatal("scan never completed")
			}
		}
	}

	var writers sync.WaitGroup
	writers.Go(func() {
		for i := range 1024 {
			s.Publish([]Row{{start.Add(time.Duration(i % 13)), "hit backfill"}})
		}
	})
	defer writers.Wait()
	check := func() {
		for _, scan := range scans {
			rows, more, err := query(scan.anchor, scan.limit, scan.budget)
			if err != nil || !reflect.DeepEqual(rows, scan.rows) || more != scan.more {
				t.Fatalf("replay changed at budget %d: rows=%v more=%v err=%v", scan.budget, rows, more, err)
			}
			if rows, more, err := query(scan.anchor, scan.limit, scan.budget-1); !errors.Is(err, ErrQueryWorkExhausted) || len(rows) != 0 || more {
				t.Fatalf("minimum work changed: rows=%v more=%v err=%v", rows, more, err)
			}
		}
	}
	check()
	writers.Wait()
	check()
	if got := s.PublishedCount(); got != len(rows)+1024 {
		t.Fatalf("fresh published count = %d", got)
	}
}
