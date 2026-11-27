package main

import (
	"slices"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

func TestFixtureScalesAndBackfill(t *testing.T) {
	for _, scale := range []string{"small", "medium", "large"} {
		t.Run(scale, func(t *testing.T) {
			batches := scaleBatches[scale]
			input := fixture(batches, false, seed)
			if len(input.rows) != batches/4*328+5 || len(input.anchors) != batches {
				t.Fatalf("unexpected fixture cardinality at %s", scale)
			}
			small := fixture(8, false, seed)
			// The oversized row follows the halfway point, so ordinary rows
			// through batch three match the prefix at every standard scale.
			prefix := small.anchors[4].Ordinal
			if !slices.Equal(small.rows[:prefix], input.rows[:prefix]) {
				t.Fatal("seeded ordinary prefix differs across scales")
			}
			for _, b := range input.backfills() {
				augmented := input.withBackfill(b)
				if !slices.Equal(augmented[:len(input.rows)], input.rows) {
					t.Fatal("backfill modified the original append input")
				}
				for i, row := range augmented[len(input.rows):] {
					if row.Ordinal != uint64(len(input.rows)+i) || row.Row != input.rows[b.batch[i].Ordinal].Row ||
						!row.Row.TS.Before(input.anchors[batches-1].Row.TS) {
						t.Fatalf("%s: row %d is not a late duplicate with a new ordinal", b.name, i)
					}
				}
			}
		})
	}
}

func TestExactOracle(t *testing.T) {
	input := fixture(64, false, seed)
	cases := []struct {
		name, literal string
		end           time.Time
		initial       []uint64
		clustered     []uint64
	}{
		{"duplicate ties", rareLiteral, epoch.Add(time.Second), []uint64{5250, 5251}, []uint64{5250, 5251, 5253}},
		{"short UTF-8", "é", epoch.Add(time.Second), []uint64{1}, []uint64{1, 5254, 5255, 5256}},
		{"different case", "RÉSEAU", epoch.Add(time.Second), nil, nil},
		{"exclusive end", rareLiteral, epoch, nil, nil},
	}
	clustered := input.withBackfill(input.backfills()[0])
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := query{literal: tc.literal, start: firstTime, end: tc.end}
			assertOrdinals := func(rows []checkpoint.BlockRow, expected []uint64) {
				t.Helper()
				got := reference(rows, q)
				ordinals := make([]uint64, len(got))
				for i := range got {
					ordinals[i] = got[i].Ordinal
				}
				if !slices.Equal(ordinals, expected) {
					t.Errorf("got ordinals %v, want %v", ordinals, expected)
				}
			}
			assertOrdinals(input.rows, tc.initial)
			assertOrdinals(clustered, tc.clustered)
		})
	}
}

func TestSeededWorkloadsAndProductionBatches(t *testing.T) {
	for _, entropy := range []bool{false, true} {
		for _, seed := range []uint64{0, 44, ^uint64(0)} {
			input := fixture(64, entropy, seed)
			repeat := fixture(64, entropy, seed)
			if !slices.Equal(input.rows, repeat.rows) || !slices.Equal(input.anchors, repeat.anchors) ||
				slices.Equal(input.rows, fixture(64, entropy, seed^1).rows) {
				t.Fatalf("invalid fixture determinism with seed %d entropy %v", seed, entropy)
			}
			var flat []checkpoint.BlockRow
			maxBatches := 0
			for batch := range input.batches() {
				payload, rows, err := encodeBatch(batch)
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := store.Decode(payload)
				if err != nil || !slices.Equal(rows, decoded) {
					t.Fatalf("production frame changes fixture rows: %v", err)
				}
				if len(payload) == store.MaxBatch {
					maxBatches++
				}
				flat = append(flat, batch...)
			}
			if maxBatches != 1 || !slices.Equal(flat, input.rows) {
				t.Fatal("production batches do not preserve the fixture or maximum frame")
			}
			for i, backfill := range input.backfills() {
				if !slices.Equal(input.withBackfill(backfill), repeat.withBackfill(repeat.backfills()[i])) {
					t.Fatalf("nondeterministic %s backfill", backfill.name)
				}
			}
		}
	}
}
