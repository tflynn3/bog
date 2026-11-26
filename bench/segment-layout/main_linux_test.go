package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"bog/internal/checkpoint"
)

func TestSmokeLayouts(t *testing.T) {
	if maxBlock != checkpoint.MaxBlockSize {
		t.Fatal("benchmark block limit differs from checkpoint codec")
	}
	var out bytes.Buffer
	if err := run(t.TempDir(), "test", "mixed", 64, 1, &out); err != nil {
		t.Fatal(err)
	}
	var report struct {
		Layouts []layoutReport `json:"layouts"`
	}
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Layouts) != 6 {
		t.Fatalf("got %d layouts", len(report.Layouts))
	}
	for i, l := range report.Layouts {
		q := l.Queries
		if len(q) != len(queries()) {
			t.Fatalf("%s returned %d queries", l.Name, len(q))
		}
		if l.Name != layouts[i].name() || l.RewriteBytes <= 0 || q[0].BlocksRead != l.Blocks ||
			q[1].MatchingRows != 2 || q[2].MatchingRows != 0 || q[6].MatchingRows != 1 ||
			q[7].MatchingRows != 2 || q[8].MatchingRows != 0 {
			t.Fatalf("unexpected report for %s: %+v", l.Name, l)
		}
		for _, short := range q[3:6] {
			if short.BlocksRead != l.Blocks || short.MatchingRows == 0 {
				t.Errorf("%s pruned or lost a short literal: %+v", l.Name, short)
			}
		}
		if layouts[i].prune && (q[1].BlocksRead >= l.Blocks || q[2].BlocksRead != 0) {
			t.Errorf("%s did not prune rare/absent literals", l.Name)
		}
	}
}
