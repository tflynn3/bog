package store

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"
)

var errTestQueryMemory = errors.New("test query memory exhausted")

// Record successful reservation and release events in int slots. A caller
// can reject a particular attempt or cancel just after allocating permission.
type matchMemory struct {
	t       *testing.T
	used    uint64
	peak    uint64
	limit   uint64
	attempt int
	reject  int
	events  []int
	onGrow  func()
}

func (m *matchMemory) ReserveQueryBytes(n uint64) error {
	m.attempt++
	if m.attempt == m.reject || n > m.limit-m.used {
		return errTestQueryMemory
	}
	m.used += n
	m.peak = max(m.peak, m.used)
	m.events = append(m.events, int(n/queryIntBytes))
	if m.onGrow != nil {
		m.onGrow()
	}
	return nil
}

func (m *matchMemory) ReleaseQueryBytes(n uint64) {
	if n > m.used {
		m.t.Fatalf("released %d bytes with only %d reserved", n, m.used)
	}
	m.used -= n
	m.events = append(m.events, -int(n/queryIntBytes))
}

func TestPositionedMatchMemoryGrowth(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	end := start.Add(time.Second)
	var s Store
	s.Publish(slices.Repeat([]Row{{start, "a"}}, 5))

	for _, tc := range []struct {
		name     string
		literal  string
		max      int
		work     uint64
		slots    uint64
		events   []int
		retained uint64
		err      error
	}{
		{"zero matches", "b", 5, 12, 0, nil, 0, nil},
		{"inclusive growth peak", "", 10, 6, 9, []int{1, 2, -1, 4, -2, 5, -4}, 5, nil},
		{"one byte below growth peak", "", 10, 6, 9, []int{1, 2, -1, 4, -2, -4}, 0, errTestQueryMemory},
		{"work stops after growth", "", 10, 5, 9, []int{1, 2, -1, 4, -2, -4}, 0, ErrQueryWorkExhausted},
		{"lookahead exceeds match bound", "", 4, 6, 9, []int{1, 2, -1, 4, -2, -4}, 0, ErrQueryWorkExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			memory := &matchMemory{t: t, limit: tc.slots * queryIntBytes}
			if tc.err == errTestQueryMemory {
				memory.limit--
			}
			matches, err := s.CapturePositionedMatchesWithMemory(start, end, tc.literal, tc.max, NewQueryWork(t.Context(), tc.work), memory)
			if !errors.Is(err, tc.err) || (matches == nil) != (err != nil) {
				t.Fatalf("capture=%v error=%v, want error=%v", matches, err, tc.err)
			}
			if memory.used != tc.retained*queryIntBytes || !slices.Equal(memory.events, tc.events) {
				t.Fatalf("retained=%d events=%v, want retained=%d events=%v", memory.used, memory.events, tc.retained*queryIntBytes, tc.events)
			}
			if matches == nil {
				return
			}
			if matches.Len() != int(tc.retained) || uint64(cap(matches.ordinals))*queryIntBytes != memory.used {
				t.Fatalf("match length=%d capacity=%d charge=%d", matches.Len(), cap(matches.ordinals), memory.used)
			}
			for i := range matches.Len() {
				if got := matches.At(i); got.Position.Ordinal != uint64(i) || got.Row.Line != "a" {
					t.Fatalf("match %d = %+v", i, got)
				}
			}
			memory.ReleaseQueryBytes(memory.used)
		})
	}
}

func TestPositionedMatchMemoryCancellation(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	var s Store
	s.Publish(slices.Repeat([]Row{{start, "a"}}, 5))
	for _, stage := range []int{1, 3, 4} {
		ctx, cancel := context.WithCancel(t.Context())
		memory := &matchMemory{t: t, limit: 9 * queryIntBytes}
		memory.onGrow = func() {
			if memory.attempt == stage {
				cancel()
			}
		}
		matches, err := s.CapturePositionedMatchesWithMemory(start, start.Add(time.Second), "", 5, NewQueryWork(ctx, 6), memory)
		cancel()
		if matches != nil || !errors.Is(err, context.Canceled) || memory.used != 0 {
			t.Fatalf("cancel on growth %d: capture=%v error=%v remaining charge=%d", stage, matches, err, memory.used)
		}
	}
}

func TestPositionedMatchMemoryScanPaths(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	end := start.Add(time.Second)
	const count = 36864
	for _, tc := range []struct {
		name         string
		shuffled     bool
		multi        bool
		dense        bool
		filler       string
		minPeakSlots uint64
	}{
		{"implicit direct and Bloom", false, false, false, "xxxx", 24},
		{"implicit merge and Bloom", false, true, false, "xxxx", 24},
		{"explicit block candidates", true, false, false, "xxxx", 49},
		{"explicit ordered scan resets before block scan", true, true, true, "xxxx", 2000},
		{"reject block selection and resume merge", true, true, false, "need eedl edle", 192},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := make([]Row, count)
			for i := range rows {
				tick := i
				if tc.multi {
					tick %= count / 2
				}
				line := tc.filler
				if i%4096 == 0 || (tc.dense && i < 768) {
					line = "needle"
				}
				rows[i] = Row{TS: start.Add(time.Duration(tick)), Line: line}
			}
			if tc.shuffled {
				rows[0].TS, rows[1].TS = rows[1].TS, rows[0].TS
			}
			var s Store
			if tc.multi {
				builder := NewRecoveryBuilder()
				builder.Add(rows[:count/2])
				builder.Install(&s)
				s.Publish(rows[count/2:])
			} else {
				s.Publish(rows)
			}
			want, more, err := s.QueryPositionedPage(start, end, "needle", count, count, 0, nil, nil)
			if err != nil || more || len(want) == 0 {
				t.Fatalf("fixture query: rows=%d more=%t err=%v", len(want), more, err)
			}
			const memoryLimit = 1 << 20
			memory := &matchMemory{t: t, limit: memoryLimit}
			open := func(m *matchMemory) (*PositionedMatches, error) {
				return s.CapturePositionedMatchesWithMemory(start, end, "needle", count, NewQueryWork(t.Context(), 1<<20), m)
			}
			got, err := open(memory)
			if err != nil || got.Len() != len(want) {
				t.Fatalf("metered query: %v %v, want %d matches", got, err, len(want))
			}
			for i, row := range want {
				if !reflect.DeepEqual(got.At(i), row) {
					t.Fatalf("offset %d got %+v, want %+v", i, got.At(i), row)
				}
			}
			if memory.used != uint64(cap(got.ordinals))*queryIntBytes || memory.peak < tc.minPeakSlots*queryIntBytes {
				t.Fatalf("peak=%d want at least %d, retained=%d ordinal capacity=%d",
					memory.peak, tc.minPeakSlots*queryIntBytes, memory.used, cap(got.ordinals))
			}
			// Fail each allocation in turn, including those in Bloom scratch,
			// while earlier allocations from the same opening are still live.
			for i := 1; i <= memory.attempt; i++ {
				rejected := &matchMemory{t: t, limit: memoryLimit, reject: i}
				partial, err := open(rejected)
				if partial != nil || !errors.Is(err, errTestQueryMemory) || rejected.used != 0 {
					t.Fatalf("allocation %d: capture=%v err=%v remaining charge=%d", i, partial, err, rejected.used)
				}
			}
		})
	}
}
