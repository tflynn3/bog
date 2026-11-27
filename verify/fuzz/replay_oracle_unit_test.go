package fuzz

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
	querymodel "bog/verify/query-model"
)

func replayMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestReplayLedger(t *testing.T) {
	var ledger committedBatchLedger
	empty, err := ledger.view(0, 0)
	replayMust(t, err)
	if string(empty.wal) != walMagic || len(empty.rows) != 0 || len(empty.tail()) != 0 {
		t.Fatalf("wrong empty view %+v", empty)
	}
	batches := replayHistory(3)
	first, err := prepareReplayBatch(batches[0])
	replayMust(t, err)
	replayMust(t, ledger.record(first, replayCommitted))

	ignored, err := prepareReplayBatch(batches[2])
	replayMust(t, err)
	replayMust(t, ledger.record(ignored, replayRolledBack))
	before, err := ledger.view(1, 1)
	replayMust(t, err)
	second, err := prepareReplayBatch(batches[1])
	replayMust(t, err)
	// Preparing a fixture captures its input before the caller reuses it.
	batches[1][0].Line = "caller overwrote input"
	replayMust(t, ledger.record(second, replayCommitted))
	after, err := ledger.view(1, 2)
	replayMust(t, err)
	if !bytes.Equal(after.prefix(), before.wal) || !bytes.Equal(after.tail(), replayFrame(second.payload)) {
		t.Fatal("unexpected committed frame boundaries")
	}
	ordinals := make([]int, 0, len(after.rows))
	for _, row := range after.rows {
		ordinals = append(ordinals, row.Ordinal)
	}
	if !slices.Equal(ordinals, []int{3, 0, 1, 2}) || after.rows[1].Line != after.rows[3].Line ||
		after.rows[2].Line != second.rows[0].Line {
		t.Fatalf("committed view lost backfill or duplicate ordering: %+v", after.rows)
	}
	before.wal[0] ^= 1
	before.rows[0].Line = "modified view"
	still, err := ledger.view(1, 1)
	replayMust(t, err)
	if !bytes.Equal(still.wal, after.prefix()) || still.rows[0].Line != first.rows[0].Line {
		t.Fatal("a snapshot retained mutable ledger state")
	}
	for _, cuts := range [][2]int{{-1, 1}, {1, 3}, {2, 1}} {
		if _, err := ledger.view(cuts[0], cuts[1]); err == nil {
			t.Fatalf("invalid cuts accepted: %v", cuts)
		}
	}
	if err := ledger.record(ignored, replayUncertain); !errors.Is(err, errReplayUncertain) {
		t.Fatalf("record uncertain: %v", err)
	}
	if _, err := ledger.view(1, 2); !errors.Is(err, errReplayUncertain) {
		t.Fatalf("view after uncertainty: %v", err)
	}
}

func TestReplayInputLimits(t *testing.T) {
	sample := replayHistory(0)[0][0]
	t.Run("single-batch", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			rows []store.Row
		}{
			{"empty", nil},
			{"row-count", slices.Repeat([]store.Row{sample}, replayMaxRows+1)},
			{"long-line", []store.Row{{TS: sample.TS, Line: strings.Repeat("x", replayMaxLineBytes+1)}}},
			{"encoded-bytes", slices.Repeat([]store.Row{{TS: sample.TS, Line: strings.Repeat("\x00", replayMaxLineBytes)}}, 8)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if _, err := prepareReplayBatch(tc.rows); !errors.Is(err, errReplayLimit) {
					t.Fatalf("prepare: %v", err)
				}
			})
		}
	})
	for _, tc := range []struct {
		name string
		rows []store.Row
		n    int
	}{
		{"batch-count", []store.Row{sample}, replayMaxBatches},
		{"total-rows", slices.Repeat([]store.Row{sample}, replayMaxRows/2), 2},
		{"total-encoded-bytes", slices.Repeat([]store.Row{{TS: sample.TS, Line: strings.Repeat("\x00", replayMaxLineBytes)}}, 3), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch, err := prepareReplayBatch(tc.rows)
			replayMust(t, err)
			var ledger committedBatchLedger
			for range tc.n {
				replayMust(t, ledger.record(batch, replayRolledBack))
			}
			if err := ledger.record(batch, replayCommitted); !errors.Is(err, errReplayLimit) {
				t.Fatalf("record beyond limit: %v", err)
			}
			view, err := ledger.view(0, 0)
			replayMust(t, err)
			if len(view.rows) != 0 || string(view.wal) != walMagic {
				t.Fatalf("rejected input changed the view: %+v", view)
			}
		})
	}
}

func TestReplayObservation(t *testing.T) {
	row := querymodel.Entry{Row: querymodel.Row{Time: replayFrom, Line: "line"}, Ordinal: 1}
	duplicate := row
	duplicate.Ordinal++
	want := replayExpectation{
		rows: []querymodel.Entry{row, duplicate},
		replayEvidence: replayEvidence{
			boundary: replayBoundary{covered: 8, end: 99},
			files:    files{"file": {1, 2, 3}},
		},
	}
	wrongLine := row
	wrongLine.Line = "Line"
	longLine := row
	longLine.Line = strings.Repeat("x", replayMaxLineBytes+1)
	wrongBoundary := want.replayEvidence
	wrongBoundary.boundary.end++
	wrongFile := want.replayEvidence
	wrongFile.files = files{"file": {1, 2, 4}}
	largeFile := want.replayEvidence
	largeFile.files = files{"file": make([]byte, replayMaxFileBytes+1)}
	reject := errors.New("expected rejection")
	alternate := errors.New("other failure")
	for _, tc := range []struct {
		name     string
		rows     []querymodel.Entry
		evidence replayEvidence
		err      error
		reject   bool
		wantText string
	}{
		{"exact", want.rows, want.replayEvidence, nil, false, ""},
		{"missing", want.rows[:1], want.replayEvidence, nil, false, "rows"},
		{"extra", append(slices.Clone(want.rows), duplicate), want.replayEvidence, nil, false, "unexpected replay row"},
		{"duplicate-order", []querymodel.Entry{duplicate, row}, want.replayEvidence, nil, false, "replay row 0"},
		{"line-bytes", []querymodel.Entry{wrongLine, duplicate}, want.replayEvidence, nil, false, "replay row 0"},
		{"line-limit", []querymodel.Entry{longLine}, want.replayEvidence, nil, false, errReplayLimit.Error()},
		{"boundary", want.rows, wrongBoundary, nil, false, "boundary"},
		{"artifact-bytes", want.rows, wrongFile, nil, false, "artifact bytes differ"},
		{"artifact-limit", want.rows, largeFile, nil, false, errReplayLimit.Error()},
		{"rejection", nil, replayEvidence{}, errors.Join(reject), true, ""},
		{"wrong-rejection", nil, replayEvidence{}, alternate, true, "expected clean rejection"},
		{"canceled-rejection", nil, replayEvidence{}, errors.Join(reject, context.Canceled), true, "expected clean rejection"},
		{"partial-rejection", []querymodel.Entry{row}, replayEvidence{}, reject, true, "unexpected replay row"},
		{"accepted-invalid", nil, want.replayEvidence, nil, true, "expected clean rejection"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expect := want
			if tc.reject {
				expect = replayExpectation{reject: reject}
			}
			err := checkReplay(t.Context(), func(ctx context.Context, emit func(querymodel.Entry) error) (replayEvidence, error) {
				if _, ok := ctx.Deadline(); !ok {
					t.Error("observation has no deadline")
				}
				for _, row := range tc.rows {
					// Simulate a faulty adapter that keeps going after rejection.
					_ = emit(row)
				}
				return tc.evidence, tc.err
			}, expect)
			if tc.wantText == "" {
				replayMust(t, err)
			} else if err == nil || !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("observation: %v, want %q", err, tc.wantText)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := checkReplay(ctx, func(context.Context, func(querymodel.Entry) error) (replayEvidence, error) {
		t.Fatal("canceled observation was started")
		return replayEvidence{}, nil
	}, want); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled observation: %v", err)
	}
}

func writeReplayImage(t *testing.T, image files) string {
	t.Helper()
	replayMust(t, validateReplayFiles(image))
	dir := t.TempDir()
	for name, data := range image {
		replayMust(t, os.WriteFile(filepath.Join(dir, name), data, 0600))
	}
	return dir
}

func TestReplayReaders(t *testing.T) {
	var ledger committedBatchLedger
	for _, rows := range replayHistory(255) {
		batch, err := prepareReplayBatch(rows)
		replayMust(t, err)
		replayMust(t, ledger.record(batch, replayCommitted))
	}
	covered, err := ledger.view(1, 1)
	replayMust(t, err)
	full, err := ledger.view(1, 3)
	replayMust(t, err)

	path := t.TempDir()
	root, err := os.OpenRoot(path)
	replayMust(t, err)
	t.Cleanup(func() { replayMust(t, root.Close()) })
	dir, err := root.Open(".")
	replayMust(t, err)
	t.Cleanup(func() { replayMust(t, dir.Close()) })
	sum := sha256.Sum256(covered.wal)
	m, err := checkpoint.PublishCaptured(t.Context(), root, dir, bytes.NewReader(covered.wal), int64(len(covered.wal)), nil, uint64(len(covered.rows)), hex.EncodeToString(sum[:]))
	replayMust(t, err)
	onDisk, err := readReplayFiles(t.Context(), path)
	replayMust(t, err)
	source := files{manifestFile: onDisk[manifestFile]}
	for _, s := range m.Segments {
		source[s.Name] = onDisk[s.Name]
	}
	var envelope struct{ Payload json.RawMessage }
	replayMust(t, json.Unmarshal(source[manifestFile], &envelope))
	generation := sha256.Sum256(envelope.Payload)

	bundle := filepath.Join(t.TempDir(), "bundle")
	point := backup.Point{Generation: generation, CoveredOffset: int64(len(covered.wal)), NextOrdinal: uint64(len(covered.rows)), CommittedEnd: int64(len(full.wal))}
	replayMust(t, backup.Export(t.Context(), capture{point, source, full.wal}, bundle))
	restored := filepath.Join(t.TempDir(), "restore")
	replayMust(t, backup.Restore(t.Context(), bundle, restored))
	wantBundle := maps.Clone(source)
	wantBundle[tailFile] = full.tail()
	wantBundle[metadataFile] = replayBackupMetadata(full, generation)

	for _, tc := range []struct {
		name    string
		observe artifactObservation
		want    replayExpectation
	}{
		{"checkpoint", observeCheckpoint(path), covered.expect(generation, onDisk)},
		{"export", observeBackup(bundle, true), full.expect(generation, wantBundle)},
		{"restore", observeBackup(restored, false), full.expect(generation, wantBundle)},
		{"concurrent-restore", observeBackup(restored, false), full.expect(generation, wantBundle)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			replayMust(t, checkReplay(t.Context(), tc.observe, tc.want))
		})
	}
	t.Run("truncated-checkpoint", func(t *testing.T) {
		bad := maps.Clone(source)
		bad[manifestFile] = bad[manifestFile][:len(bad[manifestFile])-1]
		replayMust(t, checkReplay(t.Context(), observeCheckpoint(writeReplayImage(t, bad)), replayExpectation{reject: history.ErrIntegrity}))
	})
	t.Run("stale-metadata", func(t *testing.T) {
		old := maps.Clone(wantBundle)
		old[metadataFile] = replayBackupMetadata(covered, generation)
		replayMust(t, checkReplay(t.Context(), observeBackup(writeReplayImage(t, old), true), replayExpectation{reject: backup.ErrIntegrity}))
		old[tailFile] = nil
		replayMust(t, checkReplay(t.Context(), observeBackup(writeReplayImage(t, old), true), covered.expect(generation, old)))
	})
}

func TestReplayFileBounds(t *testing.T) {
	for _, tc := range []struct {
		name        string
		count, size int
	}{
		{"file-count", replayMaxFiles + 1, 1},
		{"file-size", 1, replayMaxFileBytes + 1},
		{"total-size", 3, replayMaxFileBytes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := t.TempDir()
			image := make(files, tc.count)
			for i := range tc.count {
				name, data := fmt.Sprintf("file-%d", i), make([]byte, tc.size)
				image[name] = data
				replayMust(t, os.WriteFile(filepath.Join(path, name), data, 0600))
			}
			if err := validateReplayFiles(image); !errors.Is(err, errReplayLimit) {
				t.Fatalf("validate files: %v", err)
			}
			if _, err := readReplayFiles(t.Context(), path); !errors.Is(err, errReplayLimit) {
				t.Fatalf("read files: %v", err)
			}
		})
	}
}

func TestReplayWorkerLogBound(t *testing.T) {
	data := strings.Repeat("a", replayWorkerLogBytes) + "discarded"
	out := new(replayWorkerLog)
	n, err := io.Copy(out, io.LimitReader(strings.NewReader(data), int64(len(data))))
	replayMust(t, err)
	if n != int64(len(data)) || out.buffer.String() != data[:replayWorkerLogBytes] ||
		!errors.Is(out.limitError(), errReplayLimit) {
		t.Fatalf("worker log copied %d bytes, kept %d, limit error %v", n, out.buffer.Len(), out.limitError())
	}
}
