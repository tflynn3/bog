package fuzz

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/wal"
)

// retainedExportSnapshot borrows the publication root and WAL capture. Its
// caller holds the history reader until Export has closed all its readers.
type retainedExportSnapshot struct {
	point    backup.Point
	manifest []byte
	root     *os.Root
	wal      *wal.Capture

	ctx     context.Context
	barrier sync.Once
	entered chan struct{}
	resume  chan struct{}
}

func (s *retainedExportSnapshot) Point() backup.Point { return s.point }

func (s *retainedExportSnapshot) OpenManifest() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.manifest)), nil
}

func (s *retainedExportSnapshot) OpenSegment(name string) (io.ReadCloser, error) {
	// Export has installed the manifest in its private stage. Delay opening
	// the first segment so the segment and WAL reads happen after activation.
	s.barrier.Do(func() { close(s.entered) })
	select {
	case <-s.resume:
		return s.root.Open(name)
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *retainedExportSnapshot) OpenWAL() (backup.WALReader, error) {
	return borrowedExportWAL{s.wal}, nil
}

type borrowedExportWAL struct{ io.ReaderAt }

func (borrowedExportWAL) Close() error { return nil }

func TestRetainedExportLifecycle(t *testing.T) {
	const worker = "BOG_RETAINED_EXPORT_WORKER"
	if os.Getenv(worker) == "1" {
		setWorkerLimits(t)
		for _, cancel := range []bool{false, true} {
			name := "resume"
			if cancel {
				name = "cancel"
			}
			t.Run(name, func(t *testing.T) { runRetainedExportLifecycle(t, cancel) })
		}
		return
	}

	ctx, cancel := context.WithTimeout(t.Context(), corpusDeadline)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/proc/self/exe", "-test.run=^TestRetainedExportLifecycle$", "-test.count=1", "-test.v")
	cmd.WaitDelay = time.Second
	cmd.Env = append(cmd.Environ(), worker+"=1", "TMPDIR="+t.TempDir())
	output, err := runBoundedReplayWorker(cmd)
	if ctx.Err() != nil {
		t.Fatalf("retained export worker exceeded %s: %v\n%s", corpusDeadline, ctx.Err(), output)
	}
	if err != nil {
		t.Fatalf("retained export worker: %v\n%s", err, output)
	}
	if testing.Verbose() {
		t.Logf("%s", output)
	}
}

func captureExportWAL(t *testing.T, owner *wal.Log, data []byte, next uint64) *wal.Capture {
	t.Helper()
	c, err := owner.Capture(t.Context())
	must(t, err)
	t.Cleanup(func() { must(t, c.Release()) })
	if c.CommittedEnd() != int64(len(data)) || c.NextOrdinal() != next {
		t.Fatalf("captured WAL end/ordinal = %d/%d, want %d/%d", c.CommittedEnd(), c.NextOrdinal(), len(data), next)
	}
	got := make([]byte, len(data)+1)
	n, err := c.ReadAt(got, 0)
	if n != len(data) || !errors.Is(err, io.EOF) || !bytes.Equal(got[:n], data) {
		t.Fatalf("captured WAL differs from fixture input: read %d bytes, error %v", n, err)
	}
	return c
}

func assertExportUsage(t *testing.T, budget *checkpoint.Budget, generations, captures uint64) {
	t.Helper()
	want := checkpoint.Limits{checkpoint.PinnedGenerations: generations, checkpoint.LiveCaptures: captures}
	if got := budget.Usage().InUse; got != want {
		t.Fatalf("resource usage = %v, want %v", got, want)
	}
}

func runRetainedExportLifecycle(t *testing.T, cancelExport bool) {
	const wait = 8 * time.Second
	ctx := t.Context()
	source := t.TempDir()
	owner := openOwner(t, source)
	root, dir := openPublicationDir(t, source)
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.StagingBytes:      maxFixtureFiles * maxArtifactSize,
		checkpoint.PinnedGenerations: 2,
		checkpoint.LiveCaptures:      2,
	})
	catalog := history.NewCatalog(budget)
	t.Cleanup(func() { must(t, catalog.Close()) })
	publisher := checkpoint.NewPublisher(root, dir, budget)
	batches := replayHistory(42)
	var ledger committedBatchLedger
	appendReplayBatch(t, owner, &ledger, batches[0])
	checkpointView := fixtureReplayView(t, &ledger, 1, 1)
	prefix := checkpointView.wal
	initial := captureExportWAL(t, owner, prefix, 1)
	first, err := publisher.PublishCaptured(ctx, initial, initial.CommittedEnd(), nil, initial.NextOrdinal(), digest(prefix))
	must(t, err)
	must(t, initial.Release())
	old, err := catalog.ReplacePinned(ctx, dir)
	must(t, err)
	if want := (history.Boundary{SourceSHA256: sha256.Sum256(prefix), CoveredOffset: int64(len(prefix)), NextOrdinal: 1}); old.Boundary != want {
		t.Fatalf("original generation boundary = %+v, want %+v", old.Boundary, want)
	}

	appendReplayBatch(t, owner, &ledger, batches[1])
	exportView := fixtureReplayView(t, &ledger, 1, 2)
	originalWAL := exportView.wal
	walPin := captureExportWAL(t, owner, originalWAL, uint64(len(exportView.rows)))
	pin, err := catalog.AcquireCurrent()
	must(t, err)
	t.Cleanup(func() { must(t, pin.Close()) })
	checkHistoryRows(t, pin, old, replayBackupRows(checkpointView))
	assertExportUsage(t, budget, 1, 1)

	parent := t.TempDir()
	dest := filepath.Join(parent, "export")
	var placeholder os.FileInfo
	if cancelExport {
		must(t, os.Mkdir(dest, 0700))
		placeholder, err = os.Stat(dest)
		must(t, err)
	}
	sourceImage := historyFiles(readFiles(t, source))
	generationID := replayGenerationID(t, sourceImage)
	boundary := exportView.boundary(generationID)
	exportCtx, cancel := context.WithTimeout(ctx, wait)
	t.Cleanup(cancel)
	snapshot := &retainedExportSnapshot{
		point: backup.Point{
			Generation: generationID, CoveredOffset: boundary.covered,
			NextOrdinal: boundary.next, CommittedEnd: boundary.end,
		},
		manifest: sourceImage[manifestFile],
		root:     root, wal: walPin, ctx: exportCtx,
		entered: make(chan struct{}), resume: make(chan struct{}),
	}

	done := make(chan struct{})
	var exportErr error
	awaitExport := func() {
		t.Helper()
		select {
		case <-done:
		case <-time.After(wait):
			t.Fatal("export did not finish")
		}
	}
	t.Cleanup(func() {
		cancel()
		awaitExport()
	})
	go func() {
		defer close(done)
		exportErr = backup.Export(exportCtx, snapshot, dest)
	}()

	select {
	case <-snapshot.entered:
	case <-done:
		t.Fatalf("export exited before the copy barrier: %v", exportErr)
	case <-time.After(wait):
		t.Fatal("export did not reach the copy barrier")
	}
	stage, claim := stagePaths(t, dest)
	if stage == "" {
		t.Fatal("export reached the copy barrier without a stage")
	}
	_, err = os.Readlink(claim + ".owner")
	must(t, err)
	assertFiles(t, stage, files{manifestFile: snapshot.manifest})
	if cancelExport {
		assertChildren(t, dest)
	} else {
		assertAbsent(t, dest)
	}

	// This append is acknowledged while export is paused. Publish the same
	// WAL inode at its newer cutoff and activate under the same owner.
	appendReplayBatch(t, owner, &ledger, batches[2])
	liveView := fixtureReplayView(t, &ledger, 3, 3)
	laterWAL := liveView.wal
	later := captureExportWAL(t, owner, laterWAL, uint64(len(liveView.rows)))
	if later.Identity() != walPin.Identity() {
		t.Fatal("incremental replacement changed WAL identity")
	}
	var probe [1]byte
	if n, err := walPin.ReadAt(probe[:], snapshot.point.CommittedEnd); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("old capture read past its cutoff: %d bytes, %v", n, err)
	}
	replacement, err := catalog.ReserveReplacement(ctx)
	must(t, err)
	defer replacement.Close()
	second, err := publisher.PublishCaptured(ctx, later, later.CommittedEnd(), first, later.NextOrdinal(), digest(laterWAL))
	must(t, err)
	must(t, later.Release())
	if len(second.Segments) <= len(first.Segments) || !slices.Equal(second.Segments[:len(first.Segments)], first.Segments) {
		t.Fatal("successor did not extend the previous checkpoint runs")
	}
	fresh, err := replacement.ReplacePinned(ctx, dir)
	must(t, err)
	if want := (history.Boundary{SourceSHA256: sha256.Sum256(laterWAL), CoveredOffset: int64(len(laterWAL)), NextOrdinal: uint64(len(liveView.rows))}); fresh.ID == old.ID || fresh.Boundary != want {
		t.Fatalf("successor generation = %+v, want new ID and boundary %+v", fresh, want)
	}

	// The old ID is retired from admission, but the export's reader still
	// owns its generation slot and can scan the original rows.
	reacquired, err := catalog.Acquire(old.ID)
	if reacquired != nil {
		must(t, reacquired.Close())
	}
	var expired *history.ExpiredError
	if !errors.As(err, &expired) || expired.ID != old.ID {
		t.Fatalf("acquire retired generation: got %v, want expiry for %x", err, old.ID)
	}
	assertHistoryRows(t, catalog.AcquireCurrent, fresh, replayBackupRows(liveView))
	checkHistoryRows(t, pin, old, replayBackupRows(checkpointView))
	assertExportUsage(t, budget, 2, 1)
	select {
	case <-old.Drained:
		t.Fatal("retired generation drained while export was paused")
	default:
	}
	extra, err := catalog.ReserveReplacement(ctx)
	if extra != nil {
		extra.Close()
	}
	var exhausted *checkpoint.ExhaustedError
	wantExhausted := checkpoint.ExhaustedError{Resource: checkpoint.PinnedGenerations, Limit: 2, InUse: 2, Need: 1}
	if !errors.As(err, &exhausted) || *exhausted != wantExhausted {
		t.Fatalf("reserve another generation while export is paused: got %v, want %v", err, &wantExhausted)
	}

	if cancelExport {
		cancel()
	} else {
		close(snapshot.resume)
	}
	awaitExport()
	if cancelExport {
		if !errors.Is(exportErr, context.Canceled) || errors.Is(exportErr, backup.ErrPublicationUncertain) {
			t.Fatalf("cancel paused export: got %v, want cancellation before publication", exportErr)
		}
		info, err := os.Stat(dest)
		must(t, err)
		if !os.SameFile(placeholder, info) {
			t.Fatal("cancellation replaced the empty destination")
		}
		assertChildren(t, dest)
	} else {
		must(t, exportErr)
	}
	assertAbsent(t, stage)
	assertAbsent(t, claim)
	assertAbsent(t, claim+".owner")
	assertChildren(t, parent, backupGuard, "export")

	// Export owns its opened readers. The caller releases both snapshot
	// references only after export has finished publication or cleanup.
	assertExportUsage(t, budget, 2, 1)
	select {
	case <-old.Drained:
		t.Fatal("retired generation drained before the caller released its pins")
	default:
	}
	must(t, walPin.Release())
	if n, err := walPin.ReadAt(probe[:], 0); n != 0 || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("read released WAL capture: %d bytes, %v", n, err)
	}
	must(t, pin.Close())
	select {
	case <-old.Drained:
	case <-time.After(wait):
		t.Fatal("retired generation did not drain after export released its pins")
	}
	assertExportUsage(t, budget, 1, 0)
	extra, err = catalog.ReserveReplacement(ctx)
	must(t, err)
	extra.Close()

	if !cancelExport {
		sourceImage[tailFile] = exportView.tail()
		sourceImage[metadataFile] = replayBackupMetadata(exportView, generationID)
		checkBackupReplayRoundTrip(t, dest, exportView, sourceImage, generationID)
		checkBackupReplayRejections(t, sourceImage, replayBackupMetadata(checkpointView, generationID))
	}
	assertHistoryRows(t, catalog.AcquireCurrent, fresh, replayBackupRows(liveView))
	must(t, catalog.Close())
	assertExportUsage(t, budget, 0, 0)
}
