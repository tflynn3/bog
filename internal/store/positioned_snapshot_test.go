package store

import (
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	model "bog/verify/query-model"
)

func TestPositionedSnapshotWalkDuringPublish(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	end := start.Add(time.Second)
	filter := model.Filter{Start: start, End: end, Contains: "needle"}
	const firstOrdinal = 53

	for _, ordered := range []bool{true, false} {
		name := "backfill"
		if ordered {
			name = "ordered"
		}
		t.Run(name, func(t *testing.T) {
			s := new(Store)
			var oracle model.Log
			for i, batch := range model.SeededBatches(71, start, 413, ordered) {
				rows := make([]Row, len(batch))
				for j, row := range batch {
					rows[j] = Row{TS: row.Time, Line: row.Line}
				}
				oracle.Append(batch)
				if i == 0 {
					builder := NewRecoveryBuilder()
					builder.Add(rows)
					builder.Install(s)
				} else {
					s.Publish(rows)
				}
			}
			want, err := oracle.Match(oracle.HighWater(), filter)
			if err != nil || len(want) < 2 {
				t.Fatalf("oracle: %d matches, %v", len(want), err)
			}
			snapshot, err := s.CapturePositionedSnapshot(firstOrdinal)
			if err != nil {
				t.Fatal(err)
			}

			type trial struct {
				after *TailPosition
				rows  []PositionedRow
				more  bool
				used  uint64
				limit int
			}
			const generous = 1 << 20
			var trials []trial
			var after *TailPosition
			for offset := 0; ; {
				limit := [...]int{1, 7, 2, 13}[len(trials)%4]
				work := NewQueryWork(t.Context(), generous)
				rows, more, err := snapshot.Page(start, end, filter.Contains, limit, after, work)
				if err != nil || len(rows) != min(limit, len(want)-offset) || len(rows) == 0 {
					t.Fatalf("page at %d: %v, more=%t, error=%v", offset, rows, more, err)
				}
				for _, row := range rows {
					entry := want[offset]
					if !row.Row.TS.Equal(entry.Time) || row.Row.Line != entry.Line ||
						row.Position != (TailPosition{entry.Time, firstOrdinal + uint64(entry.Ordinal)}) {
						t.Fatalf("row %d = %+v, want %+v", offset, row, entry)
					}
					offset++
				}
				trials = append(trials, trial{after, rows, more, generous - work.Remaining(), limit})
				if more != (offset < len(want)) {
					t.Fatalf("More at %d of %d = %v", offset, len(want), more)
				}
				if !more {
					break
				}
				after = &rows[len(rows)-1].Position
			}

			var writers sync.WaitGroup
			writers.Go(func() {
				for i := range 250 {
					s.Publish([]Row{{TS: start.Add(time.Duration(i)), Line: "needle backfill"}})
				}
			})
			defer writers.Wait()
			for range 2 {
				for _, page := range trials {
					work := NewQueryWork(t.Context(), page.used)
					rows, more, err := snapshot.Page(start, end, filter.Contains, page.limit, page.after, work)
					if err != nil || work.Remaining() != 0 || more != page.more || !reflect.DeepEqual(rows, page.rows) {
						t.Fatalf("capture changed after append: got %v %t %v, remaining %d", rows, more, err, work.Remaining())
					}
					rows, more, err = snapshot.Page(start, end, filter.Contains, page.limit, page.after, NewQueryWork(t.Context(), page.used-1))
					if !errors.Is(err, ErrQueryWorkExhausted) || rows != nil || more {
						t.Fatalf("partial result below budget: %v %t %v", rows, more, err)
					}
				}
				writers.Wait()
			}
		})
	}
}

func TestPositionedSnapshotOrdinalBounds(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	end := start.Add(time.Second)
	s := new(Store)
	s.Publish([]Row{{TS: start, Line: "first"}, {TS: start, Line: "second"}})
	if _, err := s.CapturePositionedSnapshot(math.MaxUint64); err == nil {
		t.Fatal("accepted overflowing ordinal range")
	}
	snapshot, err := s.CapturePositionedSnapshot(math.MaxUint64 - 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		after uint64
		count int
	}{
		{0, 2},
		{math.MaxUint64 - 1, 1},
		{math.MaxUint64, 0},
	} {
		rows, more, err := snapshot.Page(start, end, "", 2, &TailPosition{start, tc.after}, nil)
		if err != nil || more || len(rows) != tc.count {
			t.Errorf("after %d: rows=%v, more=%t, err=%v", tc.after, rows, more, err)
		}
	}
	if rows, more, err := snapshot.Page(start, end, "", 0, nil, nil); err == nil || rows != nil || more {
		t.Fatalf("accepted zero limit: %v, %t, %v", rows, more, err)
	}
}
