//go:build linux || (darwin && cgo)

package fuzz

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"bog/internal/backup"
	"bog/internal/checkpoint"
)

// Storage calls and fixture setup/removal open files internally. Reserve
// room for those synchronous opens in addition to the view's directory
// pin and independently transferred readers. Backup publication can hold
// the parent, directory, guard, claimed stage, stage root and output file
// together. Leave room for directory traversal during these calls.
const retainedWorkerPrivateFDs = 8

type retainedReaderKind string

const (
	retainedCheckpointReader retainedReaderKind = "checkpoint"
	retainedBackupReader     retainedReaderKind = "backup"
	retainedWALReader        retainedReaderKind = "wal-tail"
)

// Charge the bounded fixture workspace and the existing per-case memory
// envelope before cloning a fixture or invoking a decoder. The latter is
// an admission charge for decoder, oracle, and staging allocations. The
// subprocess corpus separately measures cumulative Go allocations.
func (k retainedReaderKind) resources() (retainedResources, error) {
	const viewFDs = 1 + replayMaxFiles
	switch k {
	case retainedCheckpointReader:
		// Recovery reads an immutable generation and takes a zero-byte lock.
		return retainedResources{replayMaxImageBytes, retainedAllocationBytes, retainedWorkerPrivateFDs + viewFDs + 2}, nil
	case retainedBackupReader:
		// Export copies the image into a stage and renames it. The extra
		// space covers the small staging claim and v2 backup metadata.
		return retainedResources{2*replayMaxImageBytes + 4096, retainedAllocationBytes, retainedWorkerPrivateFDs + viewFDs}, nil
	case retainedWALReader:
		return retainedResources{retainedReplayBytes, retainedAllocationBytes, retainedWorkerPrivateFDs + 2}, nil
	default:
		return retainedResources{}, fmt.Errorf("unknown retained reader %q", k)
	}
}

// Each worker owns its scratch directory. Cleanup runs even if the context
// has expired. Keep the charge if cleanup cannot establish that files and
// descriptors have drained.
func runRetainedFixtureWorker(ctx context.Context, parent string, budget *retainedReaderBudget, need retainedResources,
	wait bool, run func(*retainedReaderReservation, string) error) (err error) {
	reservation, err := budget.reserve(ctx, need, wait)
	if err != nil {
		return err
	}
	path := ""
	defer func() {
		if path != "" {
			fdErr := checkRetainedReaderFDs(path, nil)
			removeErr := os.RemoveAll(path)
			absentErr := checkRetainedAbsent(path)
			err = errors.Join(err, fdErr, removeErr, absentErr)
			if fdErr != nil || absentErr != nil {
				return
			}
		}
		err = errors.Join(err, reservation.release())
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err = os.MkdirTemp(parent, "retained-reader-")
	if err != nil {
		return err
	}
	return errors.Join(run(reservation, path), ctx.Err())
}

type retainedReaderWorker struct {
	kind       retainedReaderKind
	fault      retainedReaderCase
	beforeRead func(context.Context) error
}

func (w retainedReaderWorker) run(ctx context.Context, parent string, shared *retainedReaderBudget, fixture retainedFixture, wait bool) error {
	need, err := w.kind.resources()
	if err != nil {
		return err
	}
	return runRetainedFixtureWorker(ctx, parent, shared, need, wait, func(reservation *retainedReaderReservation, path string) (err error) {
		source := filepath.Join(path, "source")
		point := retainedRecoveryPoint(fixture.previous)
		options := retainedViewOptions{reservation: reservation, beforeRead: w.beforeRead, fault: w.fault,
			cancelAt: fixture.cuts[1].end - int(point.CoveredOffset) + frameHeader}
		image := w.fault.input(fixture)
		point.SuffixSHA256 = image.digest
		switch w.kind {
		case retainedCheckpointReader:
			image.image[retainedLock] = nil
		case retainedWALReader:
			tail := image.image[tailFile]
			if len(tail) > retainedReplayBytes {
				return fmt.Errorf("%w: fixture WAL tail exceeds %d bytes", errReplayLimit, retainedReplayBytes)
			}
			image.image = files{tailFile: tail}
		}
		if err := writeRetainedImage(ctx, source, image.image); err != nil {
			return err
		}

		storage := checkpoint.NewBudget(checkpoint.Limits{
			checkpoint.StagingBytes: replayMaxImageBytes, checkpoint.ReplayTailBytes: retainedReplayBytes, checkpoint.LiveCaptures: 1,
		})
		defer func() { err = errors.Join(err, checkRetainedReaderBudget(storage)) }()
		runView := func(run func(context.Context, *retainedReaderView) error) error {
			opErr, closeErr := runRetainedReaderView(ctx, source, manifestFile, point, 0, storage, options, run)
			return errors.Join(opErr, closeErr)
		}

		switch w.kind {
		case retainedCheckpointReader:
			return recoverRetainedWorkerCheckpoint(ctx, source, reservation, storage, runView)
		case retainedBackupReader:
			dest := filepath.Join(path, "bundle")
			if err := runView(func(ctx context.Context, view *retainedReaderView) error {
				return backup.ExportRecoveryView(ctx, view, dest, retainedReplayBytes)
			}); err != nil {
				return err
			}
			return checkRetainedReaderBackup(ctx, dest, fixture, point, storage)
		case retainedWALReader:
			// The ledger checker expects a replay-only budget. The view's
			// capture is held in storage until its independent reader closes.
			replay := checkpoint.NewBudget(checkpoint.Limits{checkpoint.ReplayTailBytes: retainedReplayBytes})
			defer func() { err = errors.Join(err, checkRetainedReaderBudget(replay)) }()
			return runView(func(ctx context.Context, view *retainedReaderView) error {
				return replayRetainedWorkerTail(ctx, view, fixture, replay)
			})
		default:
			return fmt.Errorf("unknown retained reader %q", w.kind)
		}
	})
}

func recoverRetainedWorkerCheckpoint(ctx context.Context, path string, reservation *retainedReaderReservation, storage *checkpoint.Budget,
	runView func(func(context.Context, *retainedReaderView) error) error) (err error) {
	root, closeRoot, err := openRetainedHandle(ctx, reservation, retainedWorkerPrivateFDs, func() (*os.Root, error) {
		return os.OpenRoot(path)
	})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, closeRoot()) }()
	dir, closeDir, err := openRetainedHandle(ctx, reservation, retainedWorkerPrivateFDs, func() (*os.File, error) {
		return root.Open(".")
	})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, closeDir()) }()
	if err := syscall.Flock(int(dir.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return err
	}
	publisher := checkpoint.NewPublisher(root, dir, storage)
	return runView(func(ctx context.Context, view *retainedReaderView) error {
		return publisher.RecoverRecoveryView(ctx, func() (checkpoint.RecoveryView, error) {
			return retainedCheckpointView{view}, nil
		})
	})
}

type retainedLogicalReaderAt struct {
	reader io.ReaderAt
	start  int64
}

func (r retainedLogicalReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < r.start {
		return 0, fmt.Errorf("read before logical suffix at %d", off)
	}
	return r.reader.ReadAt(p, off-r.start)
}

func replayRetainedWorkerTail(ctx context.Context, view *retainedReaderView, fixture retainedFixture, storage *checkpoint.Budget) (err error) {
	reader, err := view.open(tailFile, 0)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, reader.Close()) }()
	point := view.Point()
	logical := retainedLogicalReaderAt{
		reader: io.NewSectionReader(reader, 0, point.CommittedEnd-point.CoveredOffset),
		start:  point.CoveredOffset,
	}
	return checkRetainedFrames(ctx, logical, fixture, fixture.previous, storage)
}
