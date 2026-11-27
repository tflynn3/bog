package main

import (
	"bytes"
	"encoding/json"
	"io"
	"slices"
	"testing"
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
	assertEmpty(t, parent)
}
