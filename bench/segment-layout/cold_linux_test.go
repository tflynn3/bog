package main

import (
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

func TestColdComparisonReport(t *testing.T) {
	for _, profile := range []string{"mixed", "entropy"} {
		t.Run(profile, func(t *testing.T) {
			t.Parallel()
			parent := t.TempDir()
			cfg := config{cold: true, dir: parent, revision: "test", profile: profile, scale: "custom",
				batches: 4, runs: 2, fixtureSeed: seed, timeout: time.Minute}
			var out bytes.Buffer
			if err := run(t.Context(), cfg, &out); err != nil {
				t.Fatal(err)
			}
			if err := validateBenchmarkReport(&out); err != nil {
				t.Fatal(err)
			}
			assertEmpty(t, parent)
			if err := run(t.Context(), cfg, failedReportWriter{}); !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("report write error = %v", err)
			}
			assertEmpty(t, parent)
		})
	}
}

func TestColdExperimentalPruningAndOracle(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	rows := []checkpoint.BlockRow{
		{Row: store.Row{TS: epoch, Line: "cohort-sparse é"}, Ordinal: 0},
		{Row: store.Row{TS: epoch, Line: "é-only"}, Ordinal: 1},
		{Row: store.Row{TS: epoch.Add(time.Second), Line: "other"}, Ordinal: 2},
	}
	path, err := publishSegments(t.Context(), dir, "test", snapshot{}, rows, layout{rawCodec, true}, 85)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := readSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		blocks int
	}{
		{"sparse", 1}, {"absent", 0}, {"one-byte", 3}, {"two-byte", 3}, {"three-byte", 3}, {"all", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, q := range coldQueries() {
				if q.name != tc.name {
					continue
				}
				got, sample, err := scanColdSnapshot(t.Context(), dir, snap, q)
				if err != nil {
					t.Fatal(err)
				}
				if err := checkSnapshotRows(got, reference(rows, q)); err != nil {
					t.Fatal(err)
				}
				if sample.Payload.Blocks != tc.blocks {
					t.Fatalf("read %d blocks, want %d", sample.Payload.Blocks, tc.blocks)
				}
				return
			}
			t.Fatalf("unknown query %s", tc.name)
		})
	}

	cfg := config{timeout: time.Minute}
	req := coldRequest{Path: filepath.Join(dir, "test.json"), Query: "two-byte"}
	for _, tc := range []struct {
		name string
		want []checkpoint.BlockRow
	}{
		{"reordered ties", []checkpoint.BlockRow{rows[1], rows[0]}},
		{"omitted short match", rows[:1]},
		{"extra nonmatch", rows},
		{"altered line", []checkpoint.BlockRow{{Row: store.Row{TS: epoch, Line: "cohort-sparse è"}, Ordinal: 0}, rows[1]}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var report coldQueryReport
			err := sampleColdQuery(t.Context(), cfg, req, tc.want, &report)
			if err == nil || !strings.Contains(err.Error(), "oracle") || len(report.Samples) != 0 {
				t.Fatalf("sampleColdQuery = %v, recorded samples %d", err, len(report.Samples))
			}
		})
	}
}
