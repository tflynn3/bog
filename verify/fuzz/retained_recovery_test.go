//go:build linux || (darwin && cgo)

package fuzz

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"syscall"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/history"
	querymodel "bog/verify/query-model"
)

const retainedReplayBytes = replayMaxInputBytes

func writeRetainedImage(ctx context.Context, path string, image files) error {
	if err := validateReplayFiles(image); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Mkdir(path, 0700); err != nil {
		return err
	}
	for _, name := range slices.Sorted(maps.Keys(image)) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(path, name), image[name], 0600); err != nil {
			return err
		}
	}
	return nil
}

func verifyRetainedRestore(ctx context.Context, path string, budget *checkpoint.Budget) (_ backup.Point, _ backup.WALReader, admissionCalls int, err error) {
	root, err := os.OpenRoot(path)
	if err != nil {
		return backup.Point{}, nil, 0, err
	}
	point, tail, err := backup.VerifyPinnedRestore(ctx, root, func(start, end int64) error {
		admissionCalls++
		reservation, err := budget.ReserveReplayTail(ctx, start, end)
		if err == nil {
			reservation.Release()
		}
		return err
	})
	if closeErr := root.Close(); closeErr != nil {
		if tail != nil {
			closeErr = errors.Join(closeErr, tail.Close())
		}
		return backup.Point{}, nil, admissionCalls, closeErr
	}
	return point, tail, admissionCalls, err
}

// checkRetainedFrames compares the storage replay's physical frame ranges
// and append-order rows before the offline oracle checks full query order.
func checkRetainedFrames(ctx context.Context, reader io.ReaderAt, f retainedFixture, g retainedGeneration, budget *checkpoint.Budget) (err error) {
	view := g.view
	from, end := int64(view.covered.end), int64(len(view.wal))
	b := make([]byte, len(view.tail()))
	if n, err := reader.ReadAt(b, from); err != nil || n != len(b) || !bytes.Equal(b, view.tail()) {
		return fmt.Errorf("committed tail bytes differ from ledger: read %d of %d: %w", n, len(b), err)
	}
	replayed, err := budget.ReplayCapturedTail(ctx, reader, from, end, uint64(view.covered.rows))
	defer func() {
		if replayed != nil {
			releaseRetainedFrames(replayed)
		}
		if budget.Usage().InUse != (checkpoint.Limits{}) {
			err = errors.Join(err, errors.New("replay reservations did not drain"))
		}
	}()
	if err != nil {
		return err
	}
	first := slices.Index(f.cuts, view.covered)
	if first < 0 || replayed == nil || len(replayed.Batches) != len(f.cuts)-first-1 || replayed.NextOrdinal != uint64(len(view.rows)) {
		return errors.New("replayed suffix does not match ledger cuts or next ordinal")
	}
	appendOrder := make([]querymodel.Entry, len(view.rows))
	for _, row := range view.rows {
		appendOrder[row.Ordinal] = row
	}
	for i, batch := range replayed.Batches {
		before, after := f.cuts[first+i], f.cuts[first+i+1]
		if batch.Start != int64(before.end) || batch.End != int64(after.end) ||
			batch.FirstOrdinal != uint64(before.rows) || batch.Reservation == nil || len(batch.Rows) != after.rows-before.rows {
			return fmt.Errorf("replayed frame %d does not match ledger range or ordinal coverage", i)
		}
		for j, row := range batch.Rows {
			want := appendOrder[before.rows+j]
			if !row.TS.Equal(want.Time) || row.Line != want.Line {
				return fmt.Errorf("replayed frame %d row %d differs from ledger ordinal %d", i, j, want.Ordinal)
			}
		}
	}
	if usage := budget.Usage(); usage.InUse[checkpoint.ReplayTailBytes] != uint64(end-from) {
		return fmt.Errorf("replay charged %d bytes, want %d", usage.InUse[checkpoint.ReplayTailBytes], end-from)
	}
	return nil
}

func releaseRetainedFrames(tail *checkpoint.ReplayedTail) {
	for _, batch := range tail.Batches {
		if batch.Reservation != nil {
			batch.Reservation.Release()
		}
	}
}

func checkRetainedRestore(ctx context.Context, path string, f retainedFixture, g retainedGeneration) (err error) {
	budget := checkpoint.NewBudget(checkpoint.Limits{checkpoint.ReplayTailBytes: retainedReplayBytes})
	point, tail, calls, err := verifyRetainedRestore(ctx, path, budget)
	if tail != nil {
		defer func() { err = errors.Join(err, tail.Close()) }()
	}
	if err != nil {
		return err
	}
	if tail == nil || calls != 1 || !retainedGenerationMatches(g, point) {
		return fmt.Errorf("pinned restore returned unexpected point %+v, tail present %t, admission calls %d", point, tail != nil, calls)
	}
	if err := checkRetainedFrames(ctx, tail, f, g, budget); err != nil {
		return err
	}
	return checkReplay(ctx, observeBackup(path, true), g.expectation(g.image))
}

func rejectRetainedRestore(ctx context.Context, path, previous string, tc retainedCase, f retainedFixture) error {
	budget := checkpoint.NewBudget(checkpoint.Limits{checkpoint.ReplayTailBytes: retainedReplayBytes})
	point, tail, calls, err := verifyRetainedRestore(ctx, path, budget)
	if tail != nil {
		return fmt.Errorf("rejected restore exposed a tail: %w", errors.Join(err, tail.Close()))
	}
	if point != (backup.Point{}) || budget.Usage().InUse != (checkpoint.Limits{}) || calls != tc.admissionCalls {
		return fmt.Errorf("rejected restore exposed point %+v, usage %+v or incorrect admission calls (%d)", point, budget.Usage(), calls)
	}
	if err := requireRetainedRejection(err, tc.reject); err != nil {
		return err
	}
	if tc.outcome == retainedRefuse {
		var exhausted *checkpoint.ExhaustedError
		if !errors.As(err, &exhausted) || exhausted.Resource != checkpoint.ReplayTailBytes ||
			exhausted.Limit != retainedReplayBytes || exhausted.InUse != 0 || exhausted.Need <= retainedReplayBytes {
			return fmt.Errorf("unexpected replay admission evidence: %v", err)
		}
	}
	if tc.malformedFrame {
		if err := rejectMalformedRetainedFrame(ctx, path, f.previous.view, budget); err != nil {
			return err
		}
	}
	// The offline paths have no replay-budget admission callback. They must
	// also reject the inconsistent declared length or committed authority.
	want := replayExpectation{reject: backup.ErrIntegrity}
	for _, verifyFirst := range []bool{true, false} {
		if err := checkReplay(ctx, observeBackup(path, verifyFirst), want); err != nil {
			return err
		}
	}
	dest := filepath.Dir(previous)
	if err := requireRetainedRejection(backup.Restore(ctx, path, filepath.Join(dest, "absent")), backup.ErrIntegrity); err != nil {
		return err
	}
	if err := backup.Restore(ctx, path, previous); err == nil || errors.Is(err, backup.ErrPublicationUncertain) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("restore over last valid generation did not cleanly reject: %v", err)
	}
	got, err := readReplayDirectory(ctx, dest, filepath.Base(previous))
	if err != nil {
		return err
	}
	return compareReplayFiles(got, files{".bog-backup-v1-lock": nil})
}

func rejectMalformedRetainedFrame(ctx context.Context, path string, view replayView, budget *checkpoint.Budget) error {
	image, err := readReplayFiles(ctx, path)
	if err != nil {
		return err
	}
	badWAL := slices.Concat(view.prefix(), image[tailFile])
	from, end := int64(view.covered.end), int64(len(badWAL))
	tail, err := budget.ReplayCapturedTail(ctx, bytes.NewReader(badWAL), from, end, uint64(view.covered.rows))
	if tail != nil {
		releaseRetainedFrames(tail)
		return fmt.Errorf("rejected frame exposed a replay result: %v", err)
	}
	if usage := budget.Usage(); usage.InUse != (checkpoint.Limits{}) || usage.Peak[checkpoint.ReplayTailBytes] != uint64(end-from) {
		return fmt.Errorf("rejected frame left unexpected replay usage %+v", usage)
	}
	return requireRetainedRejection(err, checkpoint.ErrIntegrity)
}

func recoverRetainedCheckpoint(ctx context.Context, path string, actualWAL []byte, previous retainedGeneration) (err error) {
	// The offline WAL reader holds the same exclusive directory lock
	// required by the captured-publication recovery contract.
	owner, err := checkpoint.OpenLegacy(path)
	if err != nil {
		return err
	}
	defer func() { err = retainedClose(err, owner) }()
	if owner.CommittedOffset() != int64(len(previous.view.wal)) {
		return fmt.Errorf("committed offset %d does not match ledger", owner.CommittedOffset())
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return err
	}
	defer func() { err = retainedClose(err, root) }()
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer func() { err = retainedClose(err, dir) }()
	return checkpoint.RecoverCaptured(root, dir, func() (*checkpoint.Manifest, error) {
		m, err := history.ValidatePinnedAuthority(ctx, dir)
		if err != nil {
			return nil, err
		}
		if !sameRetainedPrefix(m, previous.view) || m.CoveredOffset > int64(len(actualWAL)) ||
			m.SourceSHA256 != retainedDigest(actualWAL[:m.CoveredOffset]) {
			return nil, fmt.Errorf("%w: committed WAL boundary differs from ledger", checkpoint.ErrIntegrity)
		}
		return m, nil
	})
}

func checkRetainedCheckpoint(ctx context.Context, root, path string, f retainedFixture, settled files) error {
	previous := f.previous
	covered := previous.view
	covered.wal = covered.prefix()
	covered.rows = slices.DeleteFunc(slices.Clone(covered.rows), func(row querymodel.Entry) bool {
		return row.Ordinal >= covered.covered.rows
	})
	if err := checkReplay(ctx, observeCheckpoint(path), covered.expect(previous.id, settled)); err != nil {
		return err
	}
	// Bind the validated on-disk predecessor to the committed suffix read
	// from the same recovered directory, then check the merged query order.
	observed, err := readReplayFiles(ctx, path)
	if err != nil {
		return err
	}
	actualWAL := observed[retainedWALFile]
	if !bytes.Equal(actualWAL, previous.view.wal) {
		return errors.New("recovered WAL bytes differ from committed ledger")
	}
	delete(observed, retainedWALFile)
	delete(observed, retainedLock)
	delete(observed, retainedCommitFile)
	delete(observed, retainedUnrelatedSegment)
	observed[metadataFile] = replayBackupMetadata(previous.view, previous.id)
	observed[tailFile] = actualWAL[previous.view.covered.end:]
	recovered := filepath.Join(root, "recovered")
	if err := writeRetainedImage(ctx, recovered, observed); err != nil {
		return err
	}
	if err := checkRetainedRestore(ctx, recovered, f, previous); err != nil {
		return err
	}
	return recoverRetainedCheckpoint(ctx, path, actualWAL, previous)
}

func checkRetainedAbsent(path string) error {
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("expected %s to be absent, stat error %v", path, err)
	}
	return nil
}

func runRetainedOperation(ctx context.Context, root string, f retainedFixture, tc retainedCase) (retainedGeneration, error) {
	images := tc.prepare(f)
	var err error
	path := filepath.Join(root, "candidate")
	preserved := filepath.Join(root, "preserved")
	if err := os.Mkdir(preserved, 0700); err != nil {
		return retainedGeneration{}, err
	}
	previous := filepath.Join(preserved, "previous")
	if err := writeRetainedImage(ctx, previous, f.previous.image); err != nil {
		return retainedGeneration{}, err
	}
	if err := writeRetainedImage(ctx, path, images.main); err != nil {
		return retainedGeneration{}, err
	}
	selected := f.previous
	if images.stage == nil {
		if tc.outcome == retainedAccept {
			selected = f.successor
			err = checkRetainedRestore(ctx, path, f, selected)
		} else {
			err = rejectRetainedRestore(ctx, path, previous, tc, f)
		}
	} else {
		stage := filepath.Join(path, retainedStage)
		if err := bindRetainedIntent(path, f.successorSegment, &images); err != nil {
			return retainedGeneration{}, err
		}
		if err := writeRetainedImage(ctx, stage, images.stage); err != nil {
			return retainedGeneration{}, err
		}
		disk, readErr := readReplayDirectory(ctx, path, retainedStage)
		if readErr != nil {
			return retainedGeneration{}, readErr
		}
		if err := compareReplayFiles(disk, images.main); err != nil {
			return retainedGeneration{}, err
		}
		err = recoverRetainedCheckpoint(ctx, path, disk[retainedWALFile], f.previous)
		if tc.outcome == retainedFallback {
			if err == nil {
				delete(images.main, f.successorSegment)
				err = errors.Join(checkRetainedAbsent(stage), checkRetainedCheckpoint(ctx, root, path, f, images.main))
			}
		} else if err = requireRetainedRejection(err, tc.reject); err == nil {
			err = assertRetainedImage(ctx, stage, images.stage)
		}
	}
	if err != nil {
		return retainedGeneration{}, fmt.Errorf("%s: %w", tc.name, err)
	}
	if images.stage != nil && tc.outcome != retainedFallback {
		// The only directory is the known staging directory. Compare its
		// bytes separately and require the exact remaining top-level names.
		got, err := readReplayDirectory(ctx, path, retainedStage)
		if err != nil {
			return retainedGeneration{}, err
		}
		if err := compareReplayFiles(got, images.main); err != nil {
			return retainedGeneration{}, err
		}
	} else if err := assertRetainedImage(ctx, path, images.main); err != nil {
		return retainedGeneration{}, err
	}
	if err := assertRetainedImage(ctx, previous, f.previous.image); err != nil {
		return retainedGeneration{}, err
	}
	if err := checkRetainedRestore(ctx, previous, f, f.previous); err != nil {
		return retainedGeneration{}, fmt.Errorf("last valid generation and committed suffix: %w", err)
	}
	return selected, assertRetainedImage(ctx, previous, f.previous.image)
}

// The publication protocol binds an installed segment to its actual inode.
func bindRetainedIntent(path, name string, image *retainedImages) error {
	info, err := os.Stat(filepath.Join(path, name))
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("installed segment lacks inode identity")
	}
	type ownedSegment struct {
		Name   string
		Inode  uint64
		Size   int64
		SHA256 string
	}
	intent, _, err := retainedEnvelope(struct {
		HadManifest bool
		Segments    []ownedSegment
	}{true, []ownedSegment{{name, stat.Ino, info.Size(), retainedDigest(image.main[name])}}})
	if err != nil {
		return err
	}
	if image.truncateIntent {
		intent = intent[:len(intent)-1]
	}
	image.stage[retainedIntent] = intent
	return nil
}

func retainedClose(prior error, closer io.Closer) error {
	if err := closer.Close(); err != nil {
		return fmt.Errorf("close retained recovery handle after %v: %w", prior, err)
	}
	return prior
}
