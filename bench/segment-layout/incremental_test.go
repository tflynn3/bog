package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

func TestSuccessiveFixture(t *testing.T) {
	for _, batches := range []int{1, 8, 64} {
		for _, entropy := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/entropy=%t", batches, entropy), func(t *testing.T) {
				t.Parallel()
				input := fixture(batches, entropy, seed)
				changes := input.successiveAppends()
				repeat := input.successiveAppends()
				original := slices.Clone(input.rows)
				cutoff := uint64(len(input.rows))
				for i, name := range []string{"forward", "clustered", "scattered"} {
					change := changes[i]
					if change.name != name || len(change.batch) != 4 || !slices.Equal(change.batch, repeat[i].batch) {
						t.Fatalf("unexpected change %+v", change)
					}
					for j, row := range change.batch {
						if row.Ordinal != cutoff {
							t.Errorf("%s/%d ordinal = %d, want %d", name, j, row.Ordinal, cutoff)
						}
						cutoff++
						if i == 0 && !row.Row.TS.After(input.anchors[batches-1].Row.TS) {
							t.Error("forward batch must follow ordinary history")
						}
						if i > 0 && row.Row != input.rows[input.backfills()[i-1].batch[j].Ordinal].Row {
							t.Errorf("%s/%d lost exact duplicate", name, j)
						}
					}
				}
				if !slices.Equal(input.rows, original) {
					t.Fatal("successive fixture mutated the initial append history")
				}
			})
		}
	}
}

func TestSegmentAccounting(t *testing.T) {
	t.Parallel()
	a := segmentRef{"a.segment", 120, 200, false}
	b := segmentRef{"b.segment", 140, 220, false}
	c := segmentRef{"c.segment", 170, 250, false}
	changed := segmentRef{"b.segment", 141, 220, false}
	for _, tc := range []struct {
		name          string
		before, after []segmentRef
		want          segmentBytes
		reject        bool
	}{
		{"initial", nil, []segmentRef{a, b}, segmentBytes{0, 260, 260, 0}, false},
		{"reuse all", []segmentRef{a, b}, []segmentRef{a, b}, segmentBytes{260, 0, 260, 0}, false},
		{"replace tail", []segmentRef{a, b}, []segmentRef{a, c}, segmentBytes{120, 170, 290, 140}, false},
		{"remove all", []segmentRef{a, b}, nil, segmentBytes{0, 0, 0, 260}, false},
		{"duplicate old", []segmentRef{a, a}, []segmentRef{a}, segmentBytes{}, true},
		{"duplicate new", []segmentRef{a}, []segmentRef{a, a}, segmentBytes{}, true},
		{"mutated identity", []segmentRef{b}, []segmentRef{changed}, segmentBytes{}, true},
		{"invalid sizes", nil, []segmentRef{{"a.segment", 500, 100, false}}, segmentBytes{}, true},
		{"escaping manifest", nil, []segmentRef{{"../a.segment", 120, 100, false}}, segmentBytes{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := accountSegments(snapshot{tc.before}, snapshot{tc.after})
			if (err != nil) != tc.reject || got != tc.want {
				t.Fatalf("accounted %+v (%v), want %+v, reject=%t", got, err, tc.want, tc.reject)
			}
		})
	}
}

func TestSuccessiveSnapshotsAndDecodeBound(t *testing.T) {
	for _, target := range segmentTargets {
		for c := rawCodec; c <= zstdCodec; c++ {
			l := layout{codec: c}
			t.Run(fmt.Sprintf("%s/%d", l.name(), target), func(t *testing.T) {
				t.Parallel()
				dir := t.TempDir()
				input := fixture(32, c == lz4Codec, seed)
				wide := query{"all", "", firstTime, endTime}
				var previous snapshot
				var history []checkpoint.BlockRow
				var initial decodeWorkingSet
				for i, phase := range append([]backfill{{"initial", input.rows}}, input.successiveAppends()...) {
					id := fmt.Sprintf("%02d", i)
					var pinned []checkpoint.BlockRow
					var path string
					write := func() error {
						var err error
						path, err = publishSegments(t.Context(), dir, id, previous, phase.batch, l, target)
						return err
					}
					var err error
					if i == 0 {
						err = write()
					} else {
						_, _, _, err = compactWithQuery(t.Context(), func() error {
							var err error
							pinned, err = querySnapshot(t.Context(), dir, previous, target, wide)
							return err
						}, write)
						if err == nil {
							err = checkSnapshotRows(pinned, reference(history, wide))
						}
					}
					if err != nil {
						t.Fatal(err)
					}
					current, err := readSnapshot(path)
					if err != nil {
						t.Fatal(err)
					}
					account, err := accountSegments(previous, current)
					if err != nil || account.RetainedBytes != account.ReusedBytes+account.RewrittenBytes ||
						account.RewrittenBytes <= 0 || (account.ReusedBytes == 0) != (i == 0) {
						t.Fatalf("%s: accounting %+v: %v", phase.name, account, err)
					}
					history = append(history, phase.batch...)
					rows, err := querySnapshot(t.Context(), dir, current, target, wide)
					if err != nil {
						t.Fatal(err)
					}
					if err := checkSnapshotRows(rows, reference(history, wide)); err != nil {
						t.Fatal(err)
					}
					stats, err := scanSnapshot(t.Context(), dir, current, target, nil)
					if err != nil {
						t.Fatal(err)
					}
					if stats.OversizedSegments != 1 || stats.PeakOrdinaryBytes <= 0 ||
						stats.PeakOrdinaryBytes > stats.OrdinaryBoundBytes || stats.PeakOversizedBytes <= 0 ||
						stats.PeakWorkingBytes != max(stats.PeakOversizedBytes, stats.PeakOrdinaryBytes) ||
						stats.PeakWorkingBytes > stats.WorkingBoundBytes {
						t.Fatalf("invalid memory accounting: %+v", stats)
					}
					if i == 0 {
						initial = stats
					} else if stats.WorkingBoundBytes != initial.WorkingBoundBytes || stats.PeakOversizedBytes != initial.PeakOversizedBytes {
						t.Fatalf("growing snapshot changed singleton bound: before %+v, after %+v", initial, stats)
					}
					previous = current
				}
			})
		}
	}
}

func TestDecodePeakIndependentOfSegmentCount(t *testing.T) {
	for _, target := range segmentTargets {
		t.Run(fmt.Sprint(target), func(t *testing.T) {
			dir := t.TempDir()
			rows := make([]checkpoint.BlockRow, (target-blockHeader)/blockRowWidth)
			for i := range rows {
				rows[i] = checkpoint.BlockRow{Row: store.Row{TS: epoch}, Ordinal: uint64(i)}
			}
			path, err := publishSegments(t.Context(), dir, "one", snapshot{}, rows, layout{codec: rawCodec}, target)
			if err != nil {
				t.Fatal(err)
			}
			one, err := readSnapshot(path)
			if err != nil || len(one.Segments) != 1 {
				t.Fatalf("expected one block: %v", err)
			}
			many := snapshot{Segments: slices.Repeat(one.Segments, 12)}
			small, err := collectDecode(t.Context(), dir, one, target)
			if err != nil {
				t.Fatal(err)
			}
			large, err := collectDecode(t.Context(), dir, many, target)
			if err != nil {
				t.Fatal(err)
			}
			if small.decodeWorkingSet != large.decodeWorkingSet || large.Allocations <= small.Allocations ||
				large.AllocationBytes <= small.AllocationBytes || small.PeakWorkingBytes < target ||
				large.PeakWorkingBytes > large.OrdinaryBoundBytes {
				t.Fatalf("one block %+v, twelve blocks %+v", small, large)
			}
		})
	}
}

func TestDecodeRejectsInvalidSegmentBoundary(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	rows := []checkpoint.BlockRow{
		{Row: store.Row{TS: epoch, Line: strings.Repeat("x", 1024)}},
		{Row: store.Row{TS: epoch, Line: strings.Repeat("y", 1024)}, Ordinal: 1},
	}
	l := layout{codec: zstdCodec}
	for _, tc := range []struct {
		name     string
		blocks   [][]checkpoint.BlockRow
		rawBytes int
	}{
		{"oversized-not-singleton", [][]checkpoint.BlockRow{rows}, rawBlockBytes(rows)},
		{"declared-raw-size-too-small", [][]checkpoint.BlockRow{rows[:1]}, rawBlockBytes(rows[:1]) - 1},
		{"more-than-one-block", [][]checkpoint.BlockRow{rows[:1], rows[1:]}, rawBlockBytes(rows)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := tc.name + ".segment"
			size, err := writeGeneration(filepath.Join(dir, name), tc.blocks, l)
			if err != nil {
				t.Fatal(err)
			}
			current := snapshot{[]segmentRef{{name, size, tc.rawBytes, false}}}
			_, err = scanSnapshot(t.Context(), dir, current, 1024, func([]checkpoint.BlockRow) {
				t.Error("invalid segment reached consumer")
			})
			if !errors.Is(err, errLayout) {
				t.Fatalf("expected layout error, got %v", err)
			}
		})
	}
}

func TestCompactionOverlap(t *testing.T) {
	t.Parallel()
	queryStart := epoch.Add(10 * time.Millisecond)
	queryEnd := epoch.Add(20 * time.Millisecond)
	for _, tc := range []struct {
		name                   string
		startMS, endMS, wantMS int
	}{
		{"writer finishes during dispatch", 1, 9, 0},
		{"writer finishes at query entry", 1, 10, 0},
		{"writer finishes during query", 1, 15, 5},
		{"writer runs entirely during query", 12, 16, 4},
		{"query finishes during write", 15, 25, 5},
		{"writer starts after query", 21, 25, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writerStart := epoch.Add(time.Duration(tc.startMS) * time.Millisecond)
			writerEnd := epoch.Add(time.Duration(tc.endMS) * time.Millisecond)
			want := time.Duration(tc.wantMS) * time.Millisecond
			if got := compactionOverlap(queryStart, queryEnd, writerStart, writerEnd); got != want {
				t.Fatalf("overlap = %s, want %s", got, want)
			}
		})
	}
}

func TestCompactionLatencyAggregation(t *testing.T) {
	t.Parallel()
	m := compactionLatency{}
	for _, tc := range []struct {
		elapsed, overlap time.Duration
		valid            bool
	}{{10, 9, true}, {2, 2, true}, {7, 0, false}, {0, 0, false}, {1, 2, false}, {9, 1, true}} {
		if err := m.add(tc.elapsed, tc.overlap); (err == nil) != tc.valid {
			t.Fatalf("add %v got %v", tc, err)
		}
	}
	if !slices.Equal(m.SamplesNS, []int64{10, 2, 9}) || !slices.Equal(m.OverlapNS, []int64{9, 2, 1}) || p95(m.SamplesNS) != 10 {
		t.Fatalf("invalid aggregate %+v", m)
	}
}
