package history

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
	"bog/internal/wal"
)

func appendFrame(t *testing.T, log *wal.Log, data []byte) {
	t.Helper()
	rows, err := store.Decode(data)
	requireOK(t, err)
	requireOK(t, log.Append(data, rows))
}

func TestPublisherCompatibilityAndWALOracle(t *testing.T) {
	ctx := context.Background()
	dir := privateDir(t)
	l, err := wal.Open(dir, &store.Store{})
	requireOK(t, err)
	func() {
		defer func() { requireOK(t, l.Close()) }()
		prefix := `{"ts":"2026-01-01T00:00:00Z","line":"`
		suffix := `literal [a.b]"}`
		line := prefix + strings.Repeat("x", store.MaxBatch-len(prefix)-len(suffix)) + suffix
		appendFrame(t, l, []byte(line)) // Largest accepted WAL frame.
		appendFrame(t, l, []byte(`{"ts":"2026-01-01T01:00:00+01:00","line":"[a.b]"}
{"ts":"0001-01-01T00:00:00Z","line":"minimum"}
{"ts":"2026-01-01T00:00:00Z","line":"[a.b]"}
{"ts":"9999-12-31T23:59:59.999999998Z","line":"maximum"}
{"ts":"2026-01-01T00:00:00.000000001Z","line":"[A.B]"}`))
	}()

	// Publish with a partial, uncommitted successor. The WAL reader will
	// truncate this suffix on reopen, and both readers must agree on the rows.
	f, err := os.OpenFile(filepath.Join(dir, "bog.wal"), os.O_WRONLY|os.O_APPEND, 0)
	requireOK(t, err)
	_, writeErr := f.WriteString("torn successor")
	requireOK(t, errors.Join(writeErr, f.Close()))
	m, committed := publishFixture(t, dir)
	if len(m.Segments) < 2 {
		t.Fatal("fixture did not produce multiple v1 runs")
	}
	var cat Catalog
	cleanup(t, cat.Close)
	gen, err := cat.Replace(ctx, dir)
	requireOK(t, err)
	wantDigest, err := hex.DecodeString(m.SourceSHA256)
	requireOK(t, err)
	if gen.Boundary.CoveredOffset != committed || gen.Boundary.NextOrdinal != 6 ||
		!slices.Equal(gen.Boundary.SourceSHA256[:], wantDigest) {
		t.Fatalf("unexpected boundary: %+v", gen.Boundary)
	}
	walBytes, err := os.ReadFile(filepath.Join(dir, "bog.wal"))
	requireOK(t, err)
	if committed >= int64(len(walBytes)) || sha256.Sum256(walBytes[:committed]) != gen.Boundary.SourceSHA256 {
		t.Fatal("manifest did not bind the committed prefix before the uncommitted suffix")
	}
	r, err := cat.Acquire(gen.ID)
	requireOK(t, err)
	cleanup(t, r.Close)

	var sockStore store.Store
	recovered, err := wal.Open(dir, &sockStore)
	requireOK(t, err)
	cleanup(t, recovered.Close)
	for _, tt := range []struct {
		name     string
		q        Query
		ordinals []uint64
	}{
		{"all", all, []uint64{2, 0, 1, 3, 5, 4}},
		{"half-open", Query{Start: epoch, End: epoch.Add(time.Nanosecond)}, []uint64{0, 1, 3}},
		{"literal", Query{Start: minTime, End: maxTime, Contains: "[a.b]"}, []uint64{0, 1, 3}},
		{"case-sensitive", Query{Start: minTime, End: maxTime, Contains: "[A.B]"}, []uint64{5}},
		{"empty interval", Query{Start: epoch, End: epoch}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := scanRows(t, r, tt.q)
			gotRows := make([]store.Row, len(got))
			ordinals := make([]uint64, len(got))
			for i, item := range got {
				gotRows[i], ordinals[i] = item.Row, item.Ordinal
			}
			wantRows, err := sockStore.QueryWithWork(tt.q.Start, tt.q.End, tt.q.Contains, 20, nil)
			requireOK(t, err)
			if !slices.Equal(gotRows, wantRows) || !slices.Equal(ordinals, tt.ordinals) {
				t.Fatalf("recovered order: got ordinals %v, want %v; row counts %d, %d", ordinals, tt.ordinals, len(gotRows), len(wantRows))
			}
		})
	}
}

func TestPublishSuccessorInSameDirectory(t *testing.T) {
	ctx := context.Background()
	dir := privateDir(t)
	appendPayload := func(payload string) {
		l, err := wal.Open(dir, &store.Store{})
		requireOK(t, err)
		defer func() { requireOK(t, l.Close()) }()
		appendFrame(t, l, []byte(payload))
	}
	appendPayload(`{"ts":"2026-01-01T00:00:00Z","line":"tie"}`)
	firstManifest, firstEnd := publishFixture(t, dir)
	var c Catalog
	cleanup(t, c.Close)
	first, err := c.Replace(ctx, dir)
	requireOK(t, err)
	r, err := c.Acquire(first.ID)
	requireOK(t, err)
	cleanup(t, r.Close)
	appendPayload(`{"ts":"2025-01-01T00:00:00Z","line":"backfill"}
{"ts":"2026-01-01T00:00:00Z","line":"tie"}`)
	_, secondEnd := publishFixture(t, dir)
	next, err := c.Replace(ctx, dir)
	requireOK(t, err)
	if first.ID == next.ID || firstEnd >= secondEnd || first.Boundary.CoveredOffset != firstEnd ||
		next.Boundary.CoveredOffset != secondEnd || first.Boundary.NextOrdinal != 1 || next.Boundary.NextOrdinal != 3 {
		t.Fatalf("incorrect first or successor boundary: %+v %+v", first.Boundary, next.Boundary)
	}
	if got := scanRows(t, r, all); !slices.Equal(got, []checkpoint.BlockRow{row(epoch, 0, "tie")}) {
		t.Fatalf("old snapshot after publication: %v", got)
	}
	r2, err := c.Acquire(next.ID)
	requireOK(t, err)
	cleanup(t, r2.Close)
	want := []checkpoint.BlockRow{row(epoch.AddDate(-1, 0, 0), 1, "backfill"), row(epoch, 0, "tie"), row(epoch, 2, "tie")}
	if got := scanRows(t, r2, all); !slices.Equal(got, want) {
		t.Fatalf("successor snapshot: got %v, want %v", got, want)
	}
	requireOK(t, c.Reclaim(ctx, dir))
	for _, segment := range firstManifest.Segments {
		assertSegment(t, dir, segment.Name, true)
	}
	requireOK(t, r.Close())
	requireOK(t, c.Reclaim(ctx, dir))
	for _, segment := range firstManifest.Segments {
		assertSegment(t, dir, segment.Name, false)
	}
	if got := scanRows(t, r2, all); !slices.Equal(got, want) {
		t.Fatalf("successor changed after reclamation: got %v, want %v", got, want)
	}
}
