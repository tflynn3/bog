package fuzz

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"maps"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	"bog/internal/backup"
	"bog/internal/history"
	"bog/internal/store"
	"bog/internal/wal"
	querymodel "bog/verify/query-model"
)

// The fixture submits one batch at a time, so a completed outcome belongs to
// this append. Unexpected outcomes fail the case before any view is produced.
func appendReplayBatch(t testing.TB, owner *wal.Log, ledger *committedBatchLedger, rows []store.Row) {
	t.Helper()
	batch, err := prepareReplayBatch(rows)
	must(t, err)
	must(t, ledger.checkBatch(batch))
	err = owner.Append(slices.Clone(batch.payload), slices.Clone(batch.rows))
	outcome := owner.LastCommitOutcome()
	if err != nil || outcome.State != wal.CommitDurable {
		t.Fatalf("append fixture batch: outcome %+v, error %v", outcome, err)
	}
	must(t, ledger.record(batch, replayCommitted))
}

func fixtureReplayView(t testing.TB, ledger *committedBatchLedger, covered, committed int) replayView {
	t.Helper()
	v, err := ledger.view(covered, committed)
	must(t, err)
	return v
}

func replayBackupRows(view replayView) []backup.Row {
	rows := make([]backup.Row, len(view.rows))
	for i, row := range view.rows {
		rows[i] = backup.Row{Row: store.Row{TS: row.Time, Line: row.Line}, Ordinal: uint64(row.Ordinal)}
	}
	return rows
}

func replayGenerationID(t testing.TB, image files) [sha256.Size]byte {
	t.Helper()
	return sha256.Sum256(decodeEnvelope(t, image[manifestFile]).Payload)
}

func observeReopenedWAL(path string) artifactObservation {
	return func(ctx context.Context, emit func(querymodel.Entry) error) (_ replayEvidence, err error) {
		if _, err := readReplayFiles(ctx, path); err != nil {
			return replayEvidence{}, err
		}
		var s store.Store
		log, err := wal.Open(path, &s)
		if err != nil {
			return replayEvidence{}, err
		}
		defer func() { err = errors.Join(err, log.Close()) }()
		capture, err := log.Capture(ctx)
		if err != nil {
			return replayEvidence{}, err
		}
		defer func() { err = errors.Join(err, capture.Release()) }()
		end := capture.CommittedEnd()
		if end < int64(len(walMagic)) || end > replayMaxInputBytes || s.PublishedCount() > replayMaxRows {
			return replayEvidence{}, fmt.Errorf("%w: reopened WAL end %d, rows %d", errReplayLimit, end, s.PublishedCount())
		}
		data := make([]byte, end+1)
		n, err := capture.ReadAt(data, 0)
		if n != int(end) || !errors.Is(err, io.EOF) {
			return replayEvidence{}, fmt.Errorf("reopened WAL capture boundary: read %d bytes through %d: %w", n, end, err)
		}
		rows, more, err := s.QueryPositionedPage(replayFrom, replayUntil, "", replayMaxRows, s.PublishedCount(), 0, nil, store.NewQueryWork(ctx, replayMaxImageBytes))
		if err != nil {
			return replayEvidence{}, err
		}
		if more || len(rows) != s.PublishedCount() {
			return replayEvidence{}, errors.New("reopened WAL did not return a complete bounded view")
		}
		for _, row := range rows {
			if err := emitReplayRow(emit, row.Row, row.Position.Ordinal); err != nil {
				return replayEvidence{}, err
			}
		}
		image, err := readReplayFiles(ctx, path)
		if err != nil {
			return replayEvidence{}, err
		}
		if !bytes.Equal(image[walFile], data[:n]) {
			return replayEvidence{}, errors.New("reopened WAL file differs from captured prefix")
		}
		return replayEvidence{
			boundary: replayBoundary{prefixHash: sha256.Sum256(data[:n]), covered: end, end: end, next: capture.NextOrdinal()},
			files:    image,
		}, nil
	}
}

func checkCheckpointReplay(t *testing.T, view replayView, image files) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "checkpoint")
	must(t, validateReplayFiles(image))
	writeFiles(t, path, image)
	want := view.expect(replayGenerationID(t, image), image)
	must(t, checkReplay(t.Context(), observeCheckpoint(path), want))
	// After closing the independently opened catalog and its reader, the
	// serialized authority and every segment remain byte-identical.
	assertFiles(t, path, checkpointImageWithLock(image))
}

func checkpointImageWithLock(image files) files {
	withLock := maps.Clone(image)
	withLock["bog.checkpoint.lock"] = []byte{}
	return withLock
}

func checkTruncatedCheckpointReplay(t *testing.T, image files) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "checkpoint")
	damaged := maps.Clone(image)
	manifest := damaged[manifestFile]
	damaged[manifestFile] = manifest[:len(manifest)-1]
	must(t, validateReplayFiles(damaged))
	writeFiles(t, path, damaged)
	must(t, checkReplay(t.Context(), observeCheckpoint(path), replayExpectation{reject: history.ErrIntegrity}))
	assertFiles(t, path, checkpointImageWithLock(damaged))
}

func checkBackupReplayRoundTrip(t *testing.T, path string, view replayView, image files, generation [sha256.Size]byte) {
	t.Helper()
	want := view.expect(generation, image)
	must(t, checkReplay(t.Context(), observeBackup(path, true), want))
	restored := filepath.Join(t.TempDir(), "restored")
	ctx, cancel := context.WithTimeout(t.Context(), replayDeadline)
	defer cancel()
	must(t, backup.Restore(ctx, path, restored))
	must(t, checkReplay(ctx, observeBackup(restored, false), want))
	assertFiles(t, restored, image)
	assertFiles(t, path, image)
}

func checkBackupReplayRejections(t *testing.T, image files, staleMetadata []byte) {
	t.Helper()
	for _, tc := range []struct {
		name, file string
		data       []byte
	}{
		{"truncated-tail", tailFile, image[tailFile][:len(image[tailFile])-1]},
		{"stale-metadata", metadataFile, staleMetadata},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := t.TempDir()
			path, restored := filepath.Join(parent, "bundle"), filepath.Join(parent, "restore")
			damaged := maps.Clone(image)
			damaged[tc.file] = tc.data
			must(t, validateReplayFiles(damaged))
			writeFiles(t, path, damaged)
			want := replayExpectation{reject: backup.ErrIntegrity}
			must(t, checkReplay(t.Context(), observeBackup(path, true), want))
			must(t, checkReplay(t.Context(), observeBackup(path, false), want))
			ctx, cancel := context.WithTimeout(t.Context(), replayDeadline)
			defer cancel()
			requireIntegrity(t, "restore damaged bundle", backup.Restore(ctx, path, restored), backup.ErrIntegrity)
			assertAbsent(t, restored)
			assertRestoreParent(t, parent, "bundle")
			assertFiles(t, path, damaged)
		})
	}
}

func runBoundedReplayWorker(cmd *exec.Cmd) ([]byte, error) {
	output := new(replayWorkerLog)
	cmd.Stdout, cmd.Stderr = output, output
	err := cmd.Run()
	return output.buffer.Bytes(), errors.Join(err, output.limitError())
}
