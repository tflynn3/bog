package fuzz

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
	"bog/internal/wal"
	querymodel "bog/verify/query-model"
)

const checkpointStage = ".bog-checkpoint-stage"

func openPublicationDir(t *testing.T, path string) (*os.Root, *os.File) {
	t.Helper()
	root, err := os.OpenRoot(path)
	must(t, err)
	t.Cleanup(func() { must(t, root.Close()) })
	dir, err := root.Open(".")
	must(t, err)
	t.Cleanup(func() { must(t, dir.Close()) })
	return root, dir
}

func openOwner(t *testing.T, dir string) *wal.Log {
	t.Helper()
	var s store.Store
	log, err := wal.Open(dir, &s)
	must(t, err)
	t.Cleanup(func() { must(t, log.Close()) })
	return log
}

func checkpointRows(rows []store.Row) []backup.Row {
	want := make([]backup.Row, len(rows))
	for i, row := range rows {
		want[i] = backup.Row{Row: row, Ordinal: uint64(i)}
	}
	return ordered(want)
}

func publishFixtureCheckpoint(t *testing.T, path string, f recoveryFixture) {
	t.Helper()
	if !t.Run("publish-fixture", func(t *testing.T) {
		openOwner(t, path)
		root, dir := openPublicationDir(t, path)
		firstCheckpoint(t, f, root, dir)
	}) {
		t.FailNow()
	}
}

func assertGeneration(t *testing.T, catalog *history.Catalog, path string, input []byte, rows []store.Row) {
	t.Helper()
	g, err := catalog.Replace(t.Context(), path)
	must(t, err)
	if g.Boundary.CoveredOffset != int64(len(input)) ||
		g.Boundary.NextOrdinal != uint64(len(rows)) ||
		g.Boundary.SourceSHA256 != sha256.Sum256(input) {
		t.Fatalf("loaded checkpoint boundary %+v, want %d bytes and %d rows", g.Boundary, len(input), len(rows))
	}
	reader, err := catalog.AcquireCurrent()
	must(t, err)
	defer func() { must(t, reader.Close()) }()
	checkHistoryRowsIn(t, reader, g, checkpointRows(rows), time.Time{}, recoveryEnd)
}

func firstCheckpoint(t *testing.T, f recoveryFixture, root *os.Root, dir *os.File) *checkpoint.Manifest {
	t.Helper()
	prefix := f.disk[walFile][:f.firstEnd]
	previous, err := checkpoint.PublishCaptured(t.Context(), root, dir, bytes.NewReader(prefix), int64(len(prefix)), nil, 1, digest(prefix))
	must(t, err)
	return previous
}

func runCheckpointCorpus(t *testing.T) {
	f := newRecoveryFixture(t, t.TempDir())
	for _, tc := range recoverySuffixes(f) {
		t.Run(tc.name, func(t *testing.T) {
			checkPublicationCandidate(t, f, tc.data, 2, nil, checkpoint.ErrIntegrity)
		})
	}
	valid := f.disk[walFile][f.firstEnd:]
	t.Run("ordinal-mismatch", func(t *testing.T) {
		checkPublicationCandidate(t, f, valid, 2, nil, checkpoint.ErrIntegrity)
	})
	t.Run("staging-admission", func(t *testing.T) {
		checkPublicationCandidate(t, f, valid, uint64(len(f.rows)), nil, checkpoint.ErrExhausted)
	})
	t.Run("stale-commit-marker", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "owner")
		stale := markerAt(f.disk[commitFile], uint64(len(f.first.wal)))
		writeFiles(t, path, files{walFile: f.full.wal, commitFile: stale})
		want := f.first.expect([sha256.Size]byte{}, files{walFile: f.first.wal, commitFile: stale})
		for range 2 {
			must(t, checkReplay(t.Context(), observeReopenedWAL(path), want))
		}
	})
	t.Run("interrupted-before-manifest-rename", func(t *testing.T) {
		checkpointInterruption(t, f)
	})
}

func checkPublicationCandidate(t *testing.T, f recoveryFixture, suffix []byte, next uint64, rows []store.Row, wantErr error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owner")
	input := f.withSuffix(suffix)
	disk := maps.Clone(f.disk)
	if wantErr == nil {
		disk[walFile] = input
		disk[commitFile] = markerAt(disk[commitFile], uint64(len(input)))
	}
	writeFiles(t, path, disk)
	log := openOwner(t, path)
	root, dir := openPublicationDir(t, path)
	previous := firstCheckpoint(t, f, root, dir)
	var catalog history.Catalog
	t.Cleanup(func() { must(t, catalog.Close()) })
	assertGeneration(t, &catalog, path, f.disk[walFile][:f.firstEnd], f.rows[:1])
	pinned, err := catalog.AcquireCurrent()
	must(t, err)
	defer func() { must(t, pinned.Close()) }()
	generation := pinned.Generation()
	before := readFiles(t, path)

	stageLimit := uint64(maxArtifactSize)
	if wantErr == checkpoint.ErrExhausted {
		stageLimit = 1
	}
	budget := checkpoint.NewBudget(checkpoint.Limits{checkpoint.StagingBytes: stageLimit})
	p := checkpoint.NewPublisher(root, dir, budget)
	var reader io.ReaderAt = bytes.NewReader(input)
	if wantErr == nil {
		c, err := log.Capture(t.Context())
		must(t, err)
		defer func() { must(t, c.Release()) }()
		reader = c
	}
	m, err := p.PublishCaptured(t.Context(), reader, int64(len(input)), previous, next, digest(input))
	if wantErr == nil {
		must(t, err)
		if m == nil {
			t.Fatal("publication succeeded without a manifest")
		}
		assertGeneration(t, &catalog, path, input, rows)
	} else {
		if m != nil || errors.Is(err, checkpoint.ErrPublicationUncertain) {
			t.Fatalf("rejected publication left an ambiguous result: manifest=%+v err=%v", m, err)
		}
		requireIntegrity(t, "publish candidate", err, wantErr)
		if wantErr == checkpoint.ErrExhausted {
			var e *checkpoint.ExhaustedError
			if !errors.As(err, &e) || e.Resource != checkpoint.StagingBytes || e.Limit != stageLimit || e.InUse != 0 || e.Need <= stageLimit {
				t.Fatalf("unexpected staging admission details: %v", err)
			}
		}
		t.Logf("publication rejected candidate: %v", err)
		assertFiles(t, path, before)
		assertHistoryRows(t, catalog.AcquireCurrent, generation, checkpointRows(f.rows[:1]))
	}
	if budget.Usage().InUse[checkpoint.StagingBytes] != 0 {
		t.Fatal("publication retained a staging reservation")
	}
	checkHistoryRows(t, pinned, generation, checkpointRows(f.rows[:1]))
	must(t, checkpoint.CheckPublication(dir))
}

func FuzzCheckpointPublication(f *testing.F) {
	base := newRecoveryFixture(f, f.TempDir())
	valid := base.disk[walFile][base.firstEnd:]
	f.Add(valid, false)
	f.Add(valid, true)
	f.Add([]byte{}, false)
	for _, tc := range recoverySuffixes(base) {
		f.Add(tc.data, false)
	}
	f.Fuzz(func(t *testing.T, suffix []byte, wrongOrdinal bool) {
		if len(suffix) > maxRecoverySuffix {
			t.Skip()
		}
		rows, valid := replaySuffix(t, base, suffix)
		want := append(slices.Clone(base.rows[:1]), rows...)
		// A legacy WAL may contain timestamps outside the block format.
		// Keep this target focused on frame and publication validation.
		for _, row := range rows {
			if !row.TS.Before(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)) {
				t.Skip()
			}
		}
		next := uint64(len(want))
		var wantErr error
		if wrongOrdinal {
			next++
		}
		if !valid || wrongOrdinal {
			wantErr = checkpoint.ErrIntegrity
		}
		checkPublicationCandidate(t, base, suffix, next, want, wantErr)
	})
}

func checkpointInterruption(t *testing.T, f recoveryFixture) {
	path := filepath.Join(t.TempDir(), "owner")
	writeFiles(t, path, f.disk)
	publishFixtureCheckpoint(t, path, f)
	before := readFiles(t, path)
	stop := startInterrupted(t, "checkpoint", path, "")
	assertChildren(t, filepath.Join(path, checkpointStage), "manifest", "installed-v1")
	added := slices.DeleteFunc(children(t, path, maxFixtureFiles), func(name string) bool {
		_, existed := before[name]
		return existed || name == checkpointStage
	})
	if len(added) != 1 || !strings.HasPrefix(added[0], "bog-segment-") ||
		!bytes.Equal(readArtifact(t, filepath.Join(path, manifestFile)), before[manifestFile]) {
		t.Fatalf("install successor: new files %v, want one losing segment and the previous manifest", added)
	}
	if len(readArtifact(t, filepath.Join(path, added[0]))) == 0 {
		t.Fatal("installed successor segment is empty")
	}
	t.Log("phase=segments-installed-before-manifest-rename")
	stop()

	must(t, checkReplay(t.Context(), func(ctx context.Context, emit func(querymodel.Entry) error) (replayEvidence, error) {
		return loadReplayCheckpoint(ctx, path, nil, emit)
	}, replayExpectation{reject: checkpoint.ErrPublicationUncertain}))

	log := openOwner(t, path)
	assertWALPoint(t, log, f.disk[walFile], uint64(len(f.rows)))
	root, dir := openPublicationDir(t, path)
	requireIntegrity(t, "check interrupted publication", checkpoint.CheckPublication(dir), checkpoint.ErrPublicationUncertain)
	p := checkpoint.NewPublisher(root, dir, checkpoint.NewBudget(checkpoint.Limits{checkpoint.StagingBytes: maxArtifactSize}))
	input := f.disk[walFile]
	m, err := p.PublishCaptured(t.Context(), bytes.NewReader(input), int64(len(input)), nil, uint64(len(f.rows)), digest(input))
	if m != nil {
		t.Fatal("publication proceeded with unresolved staging")
	}
	requireIntegrity(t, "publish while recovery is pending", err, checkpoint.ErrPublicationUncertain)
	var previous *checkpoint.Manifest
	must(t, p.RecoverCaptured(func() (_ *checkpoint.Manifest, err error) {
		previous, err = history.ValidatePinnedAuthority(t.Context(), dir)
		if err != nil {
			return nil, err
		}
		if previous.CoveredOffset != int64(f.firstEnd) || previous.NextOrdinal != 1 || previous.SourceSHA256 != digest(input[:f.firstEnd]) {
			return nil, checkpoint.ErrIntegrity
		}
		return previous, nil
	}))
	assertFiles(t, path, before)
	checkCheckpointReplay(t, f.first, historyFiles(before))
	var catalog history.Catalog
	t.Cleanup(func() { must(t, catalog.Close()) })
	assertGeneration(t, &catalog, path, input[:f.firstEnd], f.rows[:1])
	_, err = p.PublishCaptured(t.Context(), bytes.NewReader(input), int64(len(input)), previous, uint64(len(f.rows)), digest(input))
	must(t, err)
	must(t, checkpoint.CheckPublication(dir))
	assertGeneration(t, &catalog, path, input, f.rows)
	successor := historyFiles(readFiles(t, path))
	checkCheckpointReplay(t, f.full, successor)
	checkTruncatedCheckpointReplay(t, successor)
}

func interruptCheckpoint(t *testing.T, mode, path string, park func()) {
	log := openOwner(t, path)
	c, err := log.Capture(t.Context())
	must(t, err)
	defer func() { must(t, c.Release()) }()
	root, dir := openPublicationDir(t, path)
	lock, err := checkpoint.LockPublication(dir, false)
	must(t, err)
	previous, err := history.ValidatePinnedAuthority(t.Context(), dir)
	must(t, errors.Join(err, lock.Close()))
	input := readArtifact(t, filepath.Join(path, walFile))
	ctx := phaseContext{t.Context(), func() {
		if mode == "checkpoint-staged" {
			entries, err := os.ReadDir(filepath.Join(path, checkpointStage))
			if errors.Is(err, os.ErrNotExist) {
				return
			}
			must(t, err)
			if len(entries) == 1 && strings.HasPrefix(entries[0].Name(), ceilingSegment) {
				park()
			}
			return
		}
		_, err := os.Lstat(filepath.Join(path, checkpointStage, "installed-v1"))
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		must(t, err)
		park()
	}}
	_, err = checkpoint.PublishCaptured(ctx, root, dir, c, c.CommittedEnd(), previous, c.NextOrdinal(), digest(input))
	t.Fatalf("checkpoint returned before the %s barrier: %v", mode, err)
}

// Storage checks cancellation between phases. A worker parks at the first
// such check that sees its phase marker, then the parent kills the process.
type phaseContext struct {
	context.Context
	observe func()
}

func (c phaseContext) Err() error {
	if err := c.Context.Err(); err != nil {
		return err
	}
	c.observe()
	return c.Context.Err()
}
