//go:build linux || (darwin && cgo)

package fuzz

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"syscall"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	querymodel "bog/verify/query-model"
)

const (
	retainedReaderSelector = "fixture-selection"
	retainedBackupGuard    = ".bog-backup-v1-lock"
)

func retainedRecoveryPoint(g retainedGeneration) backup.RecoveryPoint {
	b := g.view.boundary(g.id)
	return backup.RecoveryPoint{
		Generation: b.generation, CoveredOffset: b.covered, NextOrdinal: b.next, CommittedEnd: b.end,
		CommittedOrdinal: uint64(len(g.view.rows)), SuffixSHA256: sha256.Sum256(g.view.tail()),
	}
}

func runRetainedReaders(ctx context.Context, path string, f retainedFixture, tc retainedReaderCase) (err error) {
	defer func() { err = errors.Join(err, checkRetainedReaderFDs(path, nil)) }()
	owner := filepath.Join(path, "owner")
	source := filepath.Join(path, "input")
	exports := filepath.Join(path, "exports")
	dest := filepath.Join(exports, "bundle")
	valid := (retainedReaderCase{}).input(f)
	ownerFiles := cloneRetainedFiles(valid.image)
	ownerFiles[retainedLock] = nil
	input := tc.input(f)
	point := retainedRecoveryPoint(f.previous)
	bad := point
	bad.SuffixSHA256 = input.digest
	input.image[metadataFile] = recoveryBundleMetadata(bad)
	if err := errors.Join(
		writeRetainedImage(ctx, owner, ownerFiles),
		writeRetainedImage(ctx, source, input.image),
		os.Mkdir(exports, 0700),
	); err != nil {
		return err
	}
	root, err := os.OpenRoot(owner)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, dir.Close()) }()
	if err := syscall.Flock(int(dir.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return err
	}

	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.StagingBytes: replayMaxImageBytes, checkpoint.ReplayTailBytes: retainedReplayBytes,
		checkpoint.LiveCaptures: 1,
	})
	pub := checkpoint.NewPublisher(root, dir, budget)
	cut := f.successor.view.covered
	consumed := cut.end - int(point.CoveredOffset)
	cancelAt := consumed + frameHeader
	baseline, err := retainedReaderFDs(path)
	if err != nil {
		return err
	}

	exercise := func(label string, want error, bundlePresent bool, run func(context.Context, *retainedReaderView) error) error {
		if want != nil {
			opErr, cleanupErr := runWithRetainedView(ctx, source, manifestFile, bad, 0, budget, tc, cancelAt, run)
			if err := errors.Join(checkRetainedReaderRejection(opErr, want), cleanupErr,
				checkRetainedReaderFDs(path, baseline), checkRetainedReaderBudget(budget)); err != nil {
				return fmt.Errorf("%s rejection: %w", label, err)
			}
			if err := errors.Join(assertRetainedImage(ctx, owner, ownerFiles), assertRetainedImage(ctx, source, input.image),
				checkRetainedExportDirectory(ctx, exports, bundlePresent)); err != nil {
				return fmt.Errorf("%s modified a fixture or left staging: %w", label, err)
			}
		}
		opErr, cleanupErr := runWithRetainedView(ctx, owner, manifestFile, point, 0, budget, retainedReaderCase{}, 0, run)
		if err := errors.Join(opErr, cleanupErr, checkRetainedReaderFDs(path, baseline), checkRetainedReaderBudget(budget)); err != nil {
			return fmt.Errorf("%s valid view: %w", label, err)
		}
		return nil
	}

	// Export the predecessor view and use the very same source for
	// checkpoint advancement. Each reader gets its own failed/valid pair.
	if tc.edit != nil {
		if err := rejectRetainedPinnedReader(ctx, source, point, budget, tc.offlineAdmission); err != nil {
			return fmt.Errorf("backup pinned recovery rejection: %w", err)
		}
		if err := checkRetainedReaderFDs(path, baseline); err != nil {
			return err
		}
	}
	if err := exercise("backup", tc.backup, false, func(ctx context.Context, view *retainedReaderView) error {
		return backup.ExportRecoveryView(ctx, view, dest, uint64(point.CommittedEnd-point.CoveredOffset))
	}); err != nil {
		return err
	}
	if err := errors.Join(checkRetainedReaderBackup(ctx, dest, f, point, budget),
		checkRetainedExportDirectory(ctx, exports, true)); err != nil {
		return fmt.Errorf("read backup: %w", err)
	}
	if err := checkRetainedReaderFDs(path, baseline); err != nil {
		return err
	}
	var published *checkpoint.Manifest
	var selected backup.RecoveryPoint
	selects := 0
	if err := exercise("checkpoint", tc.checkpoint, true, func(ctx context.Context, view *retainedReaderView) error {
		m, err := pub.PublishRecoveryView(ctx, retainedCheckpointView{view}, int64(cut.end), uint64(cut.rows),
			func(_ context.Context, p checkpoint.RecoveryPoint) error {
				selects++
				selected = backup.RecoveryPoint(p)
				want := retainedRecoveryPoint(f.successor)
				want.Generation = selected.Generation
				if selected != want || selected.Generation == point.Generation || selected.Generation == ([32]byte{}) {
					return fmt.Errorf("selector got %+v, want new generation at %+v", selected, want)
				}
				return writeRetainedSelection(root, dir, selected)
			})
		if err != nil {
			if m != nil || selects != 0 {
				return fmt.Errorf("checkpoint failure exposed a manifest or called the selector: %v", err)
			}
			return err
		}
		published = m
		return nil
	}); err != nil {
		return err
	}
	if published == nil || selects != 1 {
		return fmt.Errorf("checkpoint valid retry returned manifest %t and selected %d times", published != nil, selects)
	}
	return errors.Join(
		checkRetainedReaderCheckpoint(ctx, owner, pub, f, selected, published, budget),
		checkRetainedReaderFDs(path, baseline), checkRetainedReaderBudget(budget),
	)
}

func checkRetainedExportDirectory(ctx context.Context, path string, bundlePresent bool) error {
	child := ""
	if bundlePresent {
		child = "bundle"
	}
	got, err := readReplayDirectory(ctx, path, child)
	if err != nil {
		return err
	}
	return compareReplayFiles(got, files{retainedBackupGuard: nil})
}

func checkRetainedReaderRejection(got, want error) error {
	if want == context.Canceled {
		if !errors.Is(got, want) || errors.Is(got, context.DeadlineExceeded) ||
			errors.Is(got, checkpoint.ErrPublicationUncertain) || errors.Is(got, backup.ErrPublicationUncertain) ||
			errors.Is(got, checkpoint.ErrIntegrity) || errors.Is(got, backup.ErrIntegrity) {
			return fmt.Errorf("want clean cancellation, got %w", got)
		}
		return nil
	}
	return requireRetainedRejection(got, want)
}

func checkRetainedReaderFDs(path string, want []string) error {
	got, err := retainedReaderFDs(path)
	if err != nil {
		return err
	}
	if !slices.Equal(got, want) {
		return fmt.Errorf("fixture descriptors %v, want %v", got, want)
	}
	return nil
}

func checkRetainedReaderBudget(b *checkpoint.Budget) error {
	usage := b.Usage()
	if usage.InUse != (checkpoint.Limits{}) {
		return fmt.Errorf("recovery reservations still charged: %+v", usage.InUse)
	}
	for resource, peak := range usage.Peak {
		if peak > usage.Limit[resource] {
			return fmt.Errorf("recovery reservation peak exceeded limit: %+v", usage)
		}
	}
	return nil
}

// This fixture selector records a single generation and logical boundary.
// The source tail stays immutable across selection, with the consumed bytes
// skipped when the selected view reopens it.
func writeRetainedSelection(root *os.Root, dir *os.File, point backup.RecoveryPoint) error {
	b, err := json.Marshal(point)
	if err != nil {
		return err
	}
	tmp := retainedReaderSelector + ".tmp"
	w, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if _, err := w.Write(b); err != nil {
		return errors.Join(err, w.Close())
	}
	if err := errors.Join(w.Sync(), w.Close()); err != nil {
		return err
	}
	if err := root.Rename(tmp, retainedReaderSelector); err != nil {
		return err
	}
	return dir.Sync()
}

func rejectRetainedPinnedReader(ctx context.Context, path string, point backup.RecoveryPoint,
	budget *checkpoint.Budget, wantAdmission bool) (err error) {
	root, err := os.OpenRoot(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close(), checkRetainedReaderBudget(budget)) }()
	var reservation *checkpoint.Reservation
	defer func() {
		if reservation != nil {
			reservation.Release()
		}
	}()
	calls := 0
	got, tail, opErr := backup.VerifyPinnedRecovery(ctx, root, func(start, end int64) error {
		calls++
		if calls != 1 || start != point.CoveredOffset || end != point.CommittedEnd {
			return fmt.Errorf("unexpected backup replay admission [%d,%d)", start, end)
		}
		var err error
		reservation, err = budget.ReserveReplayTail(ctx, start, end)
		return err
	})
	if tail != nil {
		defer func() { err = errors.Join(err, tail.Close()) }()
	}
	if got != (backup.RecoveryPoint{}) || tail != nil ||
		calls > 1 || (calls == 1) != wantAdmission || (reservation != nil) != wantAdmission {
		return fmt.Errorf("damaged image exposed point %+v, tail present %t, admission calls %d: %v", got, tail != nil, calls, opErr)
	}
	if err := requireRetainedRejection(opErr, backup.ErrIntegrity); err != nil {
		return err
	}
	return checkReplay(ctx, observeBackup(path, false), replayExpectation{reject: backup.ErrIntegrity})
}

func checkRetainedReaderBackup(ctx context.Context, path string, f retainedFixture, point backup.RecoveryPoint, budget *checkpoint.Budget) (err error) {
	root, err := os.OpenRoot(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close(), checkRetainedReaderBudget(budget)) }()
	var replay *checkpoint.Reservation
	defer func() {
		if replay != nil {
			replay.Release()
		}
	}()
	got, tail, err := backup.VerifyPinnedRecovery(ctx, root, func(start, end int64) error {
		if replay != nil || start != point.CoveredOffset || end != point.CommittedEnd {
			return fmt.Errorf("unexpected backup replay admission [%d,%d)", start, end)
		}
		var err error
		replay, err = budget.ReserveReplayTail(ctx, start, end)
		return err
	})
	if tail != nil {
		defer func() { err = errors.Join(err, tail.Close()) }()
	}
	if err != nil {
		return err
	}
	if got != point || tail == nil || replay == nil {
		return fmt.Errorf("backup point %+v, tail present %t and admitted %t", got, tail != nil, replay != nil)
	}
	expected := cloneRetainedFiles(f.previous.image)
	expected[metadataFile] = recoveryBundleMetadata(point)
	return checkReplay(ctx, observeBackup(path, true), f.previous.expectation(expected))
}

func recoveryBundleMetadata(p backup.RecoveryPoint) []byte {
	b := make([]byte, 136)
	copy(b, "BOGBAK02")
	copy(b[8:40], p.Generation[:])
	le.PutUint64(b[40:48], uint64(p.CoveredOffset))
	le.PutUint64(b[48:56], p.NextOrdinal)
	le.PutUint64(b[56:64], uint64(p.CommittedEnd))
	le.PutUint64(b[64:72], p.CommittedOrdinal)
	copy(b[72:104], p.SuffixSHA256[:])
	sum := sha256.Sum256(b[:104])
	copy(b[104:], sum[:])
	return b
}

func checkRetainedReaderCheckpoint(ctx context.Context, owner string, pub *checkpoint.Publisher, f retainedFixture,
	selected backup.RecoveryPoint, published *checkpoint.Manifest, budget *checkpoint.Budget) error {
	disk, err := readReplayFiles(ctx, owner)
	if err != nil {
		return err
	}
	var point backup.RecoveryPoint
	if err := json.Unmarshal(disk[retainedReaderSelector], &point); err != nil {
		return err
	}
	if point != selected {
		return fmt.Errorf("persisted checkpoint point %+v, want %+v", point, selected)
	}
	if !bytes.Equal(disk[tailFile], f.previous.view.tail()) || !bytes.Equal(disk[manifestFile], f.previous.image[manifestFile]) {
		return errors.New("checkpoint advance changed a pinned predecessor artifact")
	}
	prefix := f.previous.view.covered
	skip := point.CoveredOffset - int64(prefix.end)
	name := checkpoint.GenerationManifestName(point.Generation)
	opErr, cleanupErr := runWithRetainedView(ctx, owner, name, point, skip, budget, retainedReaderCase{}, 0,
		func(ctx context.Context, view *retainedReaderView) error {
			return pub.RecoverRecoveryView(ctx, func() (checkpoint.RecoveryView, error) {
				return retainedCheckpointView{view}, nil
			})
		})
	if err := errors.Join(opErr, cleanupErr); err != nil {
		return err
	}
	m, id, err := checkpoint.DecodeManifest(disk[name])
	if err != nil {
		return err
	}
	wantAdvance := checkpoint.Advance{
		Generation: fmt.Sprintf("%x", f.previous.id), CoveredOffset: int64(prefix.end), NextOrdinal: uint64(prefix.rows),
		ConsumedSHA256: retainedDigest(f.previous.view.tail()[:skip]),
	}
	if id != point.Generation || m.Advance == nil || *m.Advance != wantAdvance ||
		m.Version != published.Version || m.CoveredOffset != published.CoveredOffset ||
		m.NextOrdinal != published.NextOrdinal || !slices.Equal(m.Segments, published.Segments) {
		return fmt.Errorf("persisted checkpoint %+v does not match the selected advance", m)
	}
	if peak := budget.Usage().Peak[checkpoint.StagingBytes]; peak == 0 {
		return errors.New("checkpoint advancement did not charge any staging bytes")
	}
	return checkRetainedCheckpointRows(ctx, disk, m, point, skip, f.successor.view.rows, budget)
}

// Decode the actual selected checkpoint blocks and independently replay the
// carried tail through checkpoint. Merge by the public timestamp/ordinal
// ordering contract and compare against the shared append ledger.
func checkRetainedCheckpointRows(ctx context.Context, disk files, m *checkpoint.Manifest, point backup.RecoveryPoint,
	skip int64, want []querymodel.Entry, budget *checkpoint.Budget) error {
	var rows []querymodel.Entry
	emit := func(entry querymodel.Entry) error {
		if len(rows) == replayMaxRows || len(entry.Line) > replayMaxLineBytes {
			return errReplayLimit
		}
		rows = append(rows, entry)
		return nil
	}
	for _, segment := range m.Segments {
		block, err := checkpoint.DecodeBlock(disk[segment.Name])
		if err != nil {
			return err
		}
		if !slices.IsSortedFunc(block, func(a, b checkpoint.BlockRow) int {
			if c := a.Row.TS.Compare(b.Row.TS); c != 0 {
				return c
			}
			return cmp.Compare(a.Ordinal, b.Ordinal)
		}) {
			return fmt.Errorf("checkpoint block %s is out of order", segment.Name)
		}
		for _, row := range block {
			if err := emitReplayRow(emit, row.Row, row.Ordinal); err != nil {
				return err
			}
		}
	}
	source := retainedLogicalTail{data: disk[tailFile][skip:], start: point.CoveredOffset}
	tail, err := budget.ReplayCapturedTail(ctx, source, point.CoveredOffset, point.CommittedEnd, point.NextOrdinal)
	if tail != nil {
		defer releaseRetainedFrames(tail)
	}
	if err != nil {
		return err
	}
	if tail == nil || len(tail.Batches) != 1 || tail.NextOrdinal != point.CommittedOrdinal ||
		budget.Usage().InUse[checkpoint.ReplayTailBytes] != uint64(point.CommittedEnd-point.CoveredOffset) {
		return errors.New("checkpoint carried-tail replay has inconsistent ordinal or reservation")
	}
	batch := tail.Batches[0]
	if batch.Start != point.CoveredOffset || batch.End != point.CommittedEnd ||
		batch.FirstOrdinal != point.NextOrdinal || batch.Reservation == nil {
		return fmt.Errorf("unexpected carried-tail frame range [%d,%d) at ordinal %d", batch.Start, batch.End, batch.FirstOrdinal)
	}
	for i, row := range batch.Rows {
		if err := emitReplayRow(emit, row, batch.FirstOrdinal+uint64(i)); err != nil {
			return err
		}
	}
	slices.SortFunc(rows, func(a, b querymodel.Entry) int {
		if c := a.Time.Compare(b.Time); c != 0 {
			return c
		}
		return cmp.Compare(a.Ordinal, b.Ordinal)
	})
	if !slices.EqualFunc(rows, want, func(a, b querymodel.Entry) bool {
		return a.Time.Equal(b.Time) && a.Ordinal == b.Ordinal && a.Line == b.Line
	}) {
		return fmt.Errorf("checkpoint rows %+v, want %+v", rows, want)
	}
	return nil
}

type retainedLogicalTail struct {
	data  []byte
	start int64
}

func (t retainedLogicalTail) ReadAt(p []byte, offset int64) (int, error) {
	if offset < t.start {
		return 0, fmt.Errorf("read before logical suffix at %d", offset)
	}
	relative := offset - t.start
	if relative >= int64(len(t.data)) {
		return 0, io.EOF
	}
	n := copy(p, t.data[relative:])
	if n != len(p) {
		return n, io.EOF
	}
	return n, nil
}
