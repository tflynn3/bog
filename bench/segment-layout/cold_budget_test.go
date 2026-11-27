package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"slices"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

func TestColdRewriteBudgetExactLimit(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		written, wal uint64
		limit        uint64
		over         bool
	}{
		{"zero", 0, 1, 0, false},
		{"no growth", 1, 0, 1024, true},
		{"at limit", 1024 * 300, 300, 1024, false},
		{"one byte over", 1024*300 + 1, 300, 1024, true},
		{"multiplication would overflow", math.MaxUint64, math.MaxUint64/2 + 1, 2, false},
		{"rounded ratio would match", math.MaxUint64, math.MaxUint64 - 1, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := overRewriteBudget(amplification{WrittenBytes: tc.written, AppendedWALBytes: tc.wal}, tc.limit); got != tc.over {
				t.Fatalf("over budget = %t, want %t", got, tc.over)
			}
		})
	}
}

func TestColdFixtureProbeAndBackfillSchedule(t *testing.T) {
	t.Parallel()
	for _, entropy := range []bool{false, true} {
		input := fixture(64, entropy, seed)
		if !slices.Equal(input.rows, fixture(64, entropy, seed).rows) {
			t.Fatal("fixture changes when repeated with the same seed")
		}
		clustered, scattered := input.backfills()[0], input.backfills()[1]
		for _, b := range []backfill{clustered, scattered} {
			meta, err := describeColdBackfill(b)
			if err != nil {
				t.Fatal(err)
			}
			if meta.Rows != 4 || !meta.Earliest.Equal(epoch) ||
				meta.Latest.After(epoch.Add(62*time.Second)) || meta.AppendedWALBytes <= 16 {
				t.Fatalf("bad fixture schedule: %+v", meta)
			}
			if b.name == "clustered" && meta.DistinctTimestamps != 1 ||
				b.name == "scattered" && (meta.DistinctTimestamps != 3 || !meta.Latest.Equal(epoch.Add(62*time.Second))) {
				t.Fatalf("bad timestamp distribution: %+v", meta)
			}
			rows := input.withBackfill(b)
			var sparse, dense int
			for _, q := range coldQueryPlan(coldMatrixSchema) {
				switch q.name {
				case "sparse":
					sparse = len(reference(rows, q))
				case "dense":
					dense = len(reference(rows, q))
				default:
					continue
				}
				if got := coldQueryRuns(coldMatrixSchema, q, 3); got != 3 {
					t.Fatalf("%s ran %d rounds", q.name, got)
				}
			}
			if sparse <= 0 || dense <= len(rows)/2 || dense <= sparse {
				t.Fatalf("unexpected cohort sizes: sparse %d, dense %d, rows %d", sparse, dense, len(rows))
			}
		}
	}
}

type cancelOnRead struct {
	io.ReaderAt
	cancel context.CancelFunc
	offset int64
}

func (r cancelOnRead) ReadAt(p []byte, offset int64) (int, error) {
	n, err := r.ReaderAt.ReadAt(p, offset)
	if offset == r.offset {
		r.cancel()
	}
	return n, err
}

func TestColdDecodeCanceledDuringRead(t *testing.T) {
	t.Parallel()
	rows := []checkpoint.BlockRow{{Row: store.Row{TS: epoch, Line: "match"}}}
	raw, err := checkpoint.EncodeBlock(rows)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range layouts {
		t.Run(l.name(), func(t *testing.T) {
			data := layoutFixture(t, l, raw)
			for _, offset := range []int{0, headerSize, headerSize + l.indexWidth()} {
				ctx, cancel := context.WithCancel(t.Context())
				r := cancelOnRead{ReaderAt: bytes.NewReader(data), offset: int64(offset), cancel: cancel}
				q := query{start: firstTime, end: endTime}
				var decoder blockDecoder
				got, err := decoder.scan(ctx, r, int64(len(data)), len(raw), l.indexWidth(), q,
					func([]byte, int) error { t.Error("canceled payload reached decoder visitor"); return nil })
				decoder.close()
				cancel()
				if !errors.Is(err, context.Canceled) || got != (payloadReads{}) {
					t.Fatalf("offset %d: counters %+v, error %v", offset, got, err)
				}
			}
		})
	}
}

func TestColdRewriteCounterParsing(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, data string
		ok         bool
	}{
		{"explicit zero", "rchar: 290\nwchar: 0\n", true},
		{"missing", "rchar: 290\n", false},
		{"no value", "wchar:\n", false},
		{"negative", "wchar: -1\n", false},
		{"identical duplicate", "wchar: 0\nwchar: 0\n", false},
		{"conflicting duplicate", "wchar: 0\nwchar: 10\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseProcessIO([]byte(tc.data), "wchar")
			if (err == nil) != tc.ok || got != 0 {
				t.Fatalf("got %d, %v", got, err)
			}
		})
	}
}

func TestColdTailPackingAccounting(t *testing.T) {
	t.Parallel()
	for _, size := range []int{128, blockTarget - blockHeader - blockRowWidth} {
		initial := []checkpoint.BlockRow{
			{Row: store.Row{TS: epoch, Line: string(bytes.Repeat([]byte("a"), blockTarget-blockHeader-blockRowWidth))}},
			{Row: store.Row{TS: epoch, Line: string(bytes.Repeat([]byte("b"), size))}, Ordinal: 1},
		}
		added := checkpoint.BlockRow{Row: store.Row{TS: epoch, Line: "late"}, Ordinal: 2}
		rows := append(slices.Clone(initial), added)
		l := layout{rawCodec, true}
		dir := t.TempDir()
		path, err := publishSegments(t.Context(), dir, "initial", snapshot{}, initial, l, blockTarget)
		if err != nil {
			t.Fatal(err)
		}
		before, err := readSnapshot(path)
		if err != nil {
			t.Fatal(err)
		}
		path, err = publishSegments(t.Context(), dir, "clustered", before, rows[len(initial):], l, blockTarget)
		if err != nil {
			t.Fatal(err)
		}
		after, err := readSnapshot(path)
		if err != nil {
			t.Fatal(err)
		}
		account, err := accountSegments(before, after)
		if err != nil {
			t.Fatal(err)
		}
		blocks, reused := coldPhaseBlocks(partition(initial), len(initial), rows)
		manifest, err := json.Marshal(after)
		if err != nil {
			t.Fatal(err)
		}
		pub := coldPhase{Phase: "clustered", PayloadSizes: coldPayloadSizes(after), ManifestBytes: int64(len(manifest) + 1), segmentBytes: account}
		inventory, err := coldPublicationInventory(pub, l, coldBlockShapes(blocks), before, reused)
		if err != nil || !slices.Equal(after.Segments, inventory.Segments) {
			t.Fatalf("tail %d: inventory %v, error %v", size, inventory, err)
		}
		if size == 128 && (reused != 1 || account.RetiredBytes != before.Segments[1].FileBytes) ||
			size != 128 && (reused != 2 || account.RetiredBytes != 0) {
			t.Fatalf("tail %d: reused %d, accounting %+v", size, reused, account)
		}
		pub.ManifestBytes++
		if _, err := coldPublicationInventory(pub, l, coldBlockShapes(blocks), before, reused); err == nil {
			t.Fatal("accepted incorrect manifest byte count")
		}
	}
}
