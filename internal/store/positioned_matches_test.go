package store

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	querymodel "bog/verify/query-model"
)

func TestCapturePositionedMatchesAcrossRuns(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(n int, line string) Row {
		return Row{TS: start.Add(time.Duration(n) * time.Second), Line: line}
	}
	rows := []Row{
		at(5, "hit tie"),
		at(2, "miss"),
		at(5, "hit tie"),
		at(1, "hit first"),
		at(8, "hit last"),
		at(4, "HIT"),
		at(10, "hit end"),
		at(0, "hit before start"),
	}
	builder := NewRecoveryBuilder()
	builder.Add(rows[:2])
	s := new(Store)
	builder.Install(s)
	s.Publish(rows[2:5])
	s.Publish(rows[5:])
	var ledger querymodel.Log
	recordPositionedRows(&ledger, rows)
	low, high := start.Add(time.Second), start.Add(10*time.Second)

	for _, literal := range []string{"", "hit", "missing"} {
		t.Run(literal, func(t *testing.T) {
			filter := querymodel.Filter{Start: low, End: high, Contains: literal}
			want := modelPositionedRows(t, &ledger, ledger.HighWater(), 0, filter, nil)
			var view *PositionedMatches
			for budget := uint64(0); budget < 1024; budget++ {
				var err error
				view, err = s.CapturePositionedMatches(low, high, literal, max(1, len(want)), NewQueryWork(t.Context(), budget))
				if errors.Is(err, ErrQueryWorkExhausted) {
					if view != nil {
						t.Fatal("partial capture on exhausted work")
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				break
			}
			if view == nil {
				t.Fatal("capture never completed")
			}
			if view.Len() != len(want) {
				t.Fatalf("captured %d rows, want %d", view.Len(), len(want))
			}
			for i := range want {
				if got := view.At(i); !reflect.DeepEqual(got, want[i]) {
					t.Fatalf("match %d = %+v, want %+v", i, got, want[i])
				}
			}
			if len(want) > 1 {
				if view, err := s.CapturePositionedMatches(low, high, literal, len(want)-1, nil); view != nil || !errors.Is(err, ErrQueryWorkExhausted) {
					t.Fatalf("past match bound: capture=%v, error=%v", view, err)
				}
			}
		})
	}
}

func TestCapturePositionedMatchesMeterAndConcurrentReplay(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Second)
	s := new(Store)
	s.Publish([]Row{{start, "one"}, {start, "two"}, {start, "three"}})
	for _, tc := range []struct {
		literal string
		minimum uint64
	}{
		{"", 4},              // one run and three in-range candidates
		{"t", 4 + 3 + 3 + 5}, // every candidate must be inspected for the literal
	} {
		if view, err := s.CapturePositionedMatches(start, end, tc.literal, 3, NewQueryWork(t.Context(), tc.minimum-1)); view != nil || !errors.Is(err, ErrQueryWorkExhausted) {
			t.Fatalf("literal %q below minimum: capture=%v error=%v", tc.literal, view, err)
		}
		if _, err := s.CapturePositionedMatches(start, end, tc.literal, 3, NewQueryWork(t.Context(), tc.minimum)); err != nil {
			t.Fatalf("literal %q at minimum: %v", tc.literal, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if view, err := s.CapturePositionedMatches(start, end, "", 3, NewQueryWork(ctx, 4)); view != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled capture: capture=%v error=%v", view, err)
	}

	view, err := s.CapturePositionedMatches(start, end, "", 3, NewQueryWork(t.Context(), 4))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"one", "two", "three"}
	var group sync.WaitGroup
	group.Go(func() {
		for range 256 {
			s.Publish([]Row{{start.Add(-time.Second), "older backfill"}, {start, "new tie"}})
		}
	})
	for range 4 {
		group.Go(func() {
			for range 256 {
				for i, line := range want {
					row := view.At(i)
					if row.Row.Line != line || row.Position.Ordinal != uint64(i) || !row.Position.Time.Equal(start) {
						t.Errorf("match %d after backfill: %+v", i, row)
						return
					}
				}
			}
		})
	}
	group.Wait()
	if view.Len() != len(want) || s.PublishedCount() != len(want)+512 {
		t.Fatalf("old capture length=%d fresh count=%d", view.Len(), s.PublishedCount())
	}
}
