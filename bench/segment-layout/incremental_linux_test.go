package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"bog/internal/checkpoint"
)

func TestIncrementalReport(t *testing.T) {
	parent := t.TempDir()
	cfg, err := parseConfig([]string{"-incremental", "-batches", "8", "-runs", "2", "-dir", parent}, "test", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := run(t.Context(), cfg, &out); err != nil {
		t.Fatal(err)
	}
	var got incrementalReport
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != 4 || got.Mode != "incremental" || got.Revision != cfg.revision ||
		got.Fixture.Name != "mixed-successive-v1" || got.Fixture.Batches != cfg.batches || got.Fixture.Runs != cfg.runs ||
		got.Environment["os"] != "linux" || len(got.Layouts) != 3*len(segmentTargets) {
		t.Fatalf("incorrect incremental report header: %+v", got)
	}
	seen := make(map[int]map[string]bool)
	for _, item := range got.Layouts {
		if !slices.Contains(segmentTargets[:], item.SegmentBytes) || !slices.Contains([]string{"raw", "lz4", "zstd"}, item.Codec) {
			t.Fatalf("unexpected codec/target %s/%d", item.Codec, item.SegmentBytes)
		}
		if seen[item.SegmentBytes] == nil {
			seen[item.SegmentBytes] = make(map[string]bool)
		}
		if seen[item.SegmentBytes][item.Codec] || len(item.Publications) != 4 {
			t.Fatalf("duplicate layout or missing publications %+v", item)
		}
		seen[item.SegmentBytes][item.Codec] = true
		previous := publicationResult{}
		for i, pub := range item.Publications {
			if pub.Phase != []string{"initial", "forward", "clustered", "scattered"}[i] ||
				pub.Rows != previous.Rows+pub.AppendedRows || pub.ManifestBytes <= 0 || pub.Segments <= 0 ||
				pub.ReusedBytes+pub.RewrittenBytes != pub.RetainedBytes ||
				pub.ReusedBytes+pub.RetiredBytes != previous.RetainedBytes {
				t.Fatalf("%s/%d: invalid publication %+v", item.Codec, item.SegmentBytes, pub)
			}
			m := pub.Decode
			if m.Allocations == 0 || m.AllocationBytes == 0 || m.PeakRSSBytes == 0 ||
				m.OversizedSegments != 1 || m.PeakOrdinaryBytes <= 0 ||
				m.PeakOrdinaryBytes > m.OrdinaryBoundBytes || m.OrdinaryBoundBytes != decodeBound(item.SegmentBytes) ||
				m.PeakWorkingBytes > m.WorkingBoundBytes {
				t.Fatalf("invalid decode sample %+v", m)
			}
			q := pub.QueryCompaction
			if i == 0 {
				if q != nil || pub.AppendedRows != 661 || pub.RewrittenBytes != pub.RetainedBytes {
					t.Fatalf("invalid initial publication %+v", pub)
				}
			} else {
				if q == nil || q.Query != "sparse" || q.MatchingRows == 0 || pub.AppendedRows != 4 ||
					pub.ReusedBytes <= 0 || pub.RewrittenBytes <= 0 || len(q.SamplesNS) != cfg.runs ||
					len(q.OverlapNS) != cfg.runs || q.P95NS != p95(q.SamplesNS) {
					t.Fatalf("invalid compaction sample %+v", pub)
				}
				for j, ns := range q.OverlapNS {
					if ns <= 0 || ns > q.SamplesNS[j] {
						t.Fatalf("invalid overlap %d in sample %d", ns, q.SamplesNS[j])
					}
				}
			}
			previous = pub
		}
	}
	if err := validateQualificationJSON(bytes.NewReader(out.Bytes())); err != nil {
		t.Fatal(err)
	}
	if got.Baseline.Name != productionV1 {
		t.Fatalf("wrong baseline: %s", got.Baseline.Name)
	}
	input := fixture(cfg.batches, false, cfg.fixtureSeed)
	phases := append([]backfill{{"initial", input.rows}}, input.successiveAppends()...)
	var history []checkpoint.BlockRow
	for i, phase := range phases {
		history = append(history, phase.batch...)
		readRows := history
		if i > 0 {
			readRows = history[:len(history)-len(phase.batch)]
		}
		var expectedWAL uint64
		if i == 0 {
			for batch := range input.batches() {
				expectedWAL += fixtureFrameBytes(t, batch)
			}
		} else {
			expectedWAL = fixtureFrameBytes(t, phase.batch)
		}
		candidates := []publicationResult{got.Baseline.Publications[i]}
		for _, item := range got.Layouts {
			candidates = append(candidates, item.Publications[i])
		}
		for _, pub := range candidates {
			for j, literal := range []string{sparseLiteral, denseLiteral} {
				q := query{literal: literal, start: firstTime, end: endTime}
				scan := pub.Queries[j]
				if scan.MatchingRows != len(reference(readRows, q)) {
					t.Fatalf("%s: incorrect %s result count: %+v", phase.name, scan.Query, scan)
				}
				for _, sample := range scan.Samples {
					if sample.WriteAmplification.AppendedWALBytes != expectedWAL {
						t.Fatalf("%s: wrote %d WAL bytes, want %d", phase.name, sample.WriteAmplification.AppendedWALBytes, expectedWAL)
					}
				}
			}
		}
	}
	assertEmpty(t, parent)
	reportPath := filepath.Join(parent, "report.json")
	if err := os.WriteFile(reportPath, out.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run(t.Context(), config{validateReport: reportPath}, io.Discard); err != nil {
		t.Fatal(err)
	}
}

func fixtureFrameBytes(t *testing.T, rows []checkpoint.BlockRow) uint64 {
	t.Helper()
	payload, _, err := encodeBatch(rows)
	if err != nil {
		t.Fatal(err)
	}
	return uint64(len(payload) + 16)
}

func TestQualificationTrialCancellationCleansWorkDirectory(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := qualifyTrial(ctx, dir, qualificationRequest{Timeout: time.Second}, false)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled trial, got %v", err)
	}
	assertEmpty(t, dir)
}

func TestQualificationJoinsReaderAndWriterErrors(t *testing.T) {
	writerErr := errors.New("writer")
	readerErr := errors.New("reader")
	readerStarted := make(chan struct{})
	_, err := timeQualification(t.Context(), 1, func() error {
		select {
		case <-readerStarted:
			return writerErr
		case <-time.After(time.Second):
			return errors.New("reader did not start")
		}
	}, func() error {
		close(readerStarted)
		return readerErr
	})
	if !errors.Is(err, writerErr) || !errors.Is(err, readerErr) {
		t.Fatalf("expected both errors, got %v", err)
	}
}
