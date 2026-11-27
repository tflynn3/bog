//go:build linux || (darwin && cgo)

package fuzz

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"bog/internal/backup"
	"bog/internal/checkpoint"
)

func TestRetainedRecoveryViewConcurrentReaders(t *testing.T) {
	f, err := seedRetainedFixture()
	requireRetainedOK(t, err)
	path := filepath.Join(t.TempDir(), "inputs")
	in := (retainedReaderCase{}).input(f)
	requireRetainedOK(t, writeRetainedImage(t.Context(), path, in.image))
	budget := checkpoint.NewBudget(checkpoint.Limits{checkpoint.LiveCaptures: 1})
	point := retainedRecoveryPoint(f.previous)

	opErr, cleanupErr := runWithRetainedView(t.Context(), path, manifestFile, point, 0, budget, retainedReaderCase{}, 0,
		func(_ context.Context, view *retainedReaderView) error {
			checkpointView := retainedCheckpointView{view}
			if checkpointView.Point() != checkpoint.RecoveryPoint(point) || view.Point() != point {
				return errors.New("package adapters disagree on the pinned point")
			}
			var wg sync.WaitGroup
			start := make(chan struct{})
			results := make(chan error, 4)
			for _, job := range []struct {
				open func() (io.ReadCloser, error)
				want []byte
			}{
				{view.OpenSuffix, in.image[tailFile]}, {checkpointView.OpenSuffix, in.image[tailFile]},
				{view.OpenManifest, in.image[manifestFile]}, {checkpointView.OpenManifest, in.image[manifestFile]},
			} {
				wg.Go(func() {
					reader, err := job.open()
					// All opens complete before any reader can consume bytes.
					results <- err
					if err == nil {
						<-start
						// Read the complete independent stream, including EOF.
						got, err := io.ReadAll(reader)
						if !bytes.Equal(got, job.want) {
							err = errors.Join(err, errors.New("concurrent reader did not start at byte zero"))
						}
						results <- errors.Join(err, reader.Close())
					}
				})
			}
			var err error
			for range 4 {
				err = errors.Join(err, <-results)
			}
			close(start)
			wg.Wait()
			close(results)
			for result := range results {
				err = errors.Join(err, result)
			}
			return err
		})
	requireRetainedOK(t, errors.Join(opErr, cleanupErr, checkRetainedReaderBudget(budget), checkRetainedReaderFDs(path, nil)))
}

func TestRetainedRecoveryViewStagingReservationRetry(t *testing.T) {
	f, err := seedRetainedFixture()
	requireRetainedOK(t, err)
	path := filepath.Join(t.TempDir(), "owner")
	in := (retainedReaderCase{}).input(f)
	in.image[retainedLock] = nil
	requireRetainedOK(t, writeRetainedImage(t.Context(), path, in.image))

	root, err := os.OpenRoot(path)
	requireRetainedOK(t, err)
	t.Cleanup(func() { requireRetainedOK(t, root.Close()) })
	dir, err := root.Open(".")
	requireRetainedOK(t, err)
	t.Cleanup(func() { requireRetainedOK(t, dir.Close()) })
	requireRetainedOK(t, syscall.Flock(int(dir.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))

	point := retainedRecoveryPoint(f.previous)
	newBlockBytes := uint64(len(f.successor.image[f.successorSegment]))
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.StagingBytes: replayMaxImageBytes, checkpoint.ReplayTailBytes: retainedReplayBytes, checkpoint.LiveCaptures: 1,
	})
	publisher := checkpoint.NewPublisher(root, dir, budget)
	// Leave just enough room for the new block. The manifest reservation
	// fails after the attempt has already written and charged that block.
	blocker, err := budget.Reserve(t.Context(), checkpoint.StagingBytes, replayMaxImageBytes-newBlockBytes)
	requireRetainedOK(t, err)
	defer blocker.Release()
	var selected backup.RecoveryPoint
	var manifest *checkpoint.Manifest
	attempt := func() (error, error) {
		cut := f.successor.view.covered
		return runWithRetainedView(t.Context(), path, manifestFile, point, 0, budget, retainedReaderCase{}, 0,
			func(ctx context.Context, view *retainedReaderView) (err error) {
				manifest, err = publisher.PublishRecoveryView(ctx, retainedCheckpointView{view}, int64(cut.end), uint64(cut.rows),
					func(_ context.Context, p checkpoint.RecoveryPoint) error {
						selected = backup.RecoveryPoint(p)
						return writeRetainedSelection(root, dir, selected)
					})
				return err
			})
	}
	baseline, err := retainedReaderFDs(path)
	requireRetainedOK(t, err)
	opErr, cleanupErr := attempt()
	requireRetainedOK(t, cleanupErr)
	requireRetainedOK(t, requireRetainedRejection(opErr, checkpoint.ErrExhausted))
	var exhausted *checkpoint.ExhaustedError
	if manifest != nil || selected != (backup.RecoveryPoint{}) || !errors.As(opErr, &exhausted) ||
		exhausted.Resource != checkpoint.StagingBytes || exhausted.Limit != replayMaxImageBytes ||
		exhausted.InUse != exhausted.Limit || exhausted.Need == 0 {
		t.Fatalf("expected exhaustion after staging a block, got %v with manifest %+v and selection %+v", opErr, manifest, selected)
	}
	usage := budget.Usage()
	if usage.Peak[checkpoint.StagingBytes] != replayMaxImageBytes ||
		usage.InUse != (checkpoint.Limits{checkpoint.StagingBytes: replayMaxImageBytes - newBlockBytes}) {
		t.Fatalf("unexpected staging cleanup %+v", usage)
	}
	requireRetainedOK(t, errors.Join(assertRetainedImage(t.Context(), path, in.image), checkRetainedReaderFDs(path, baseline)))
	blocker.Release()
	requireRetainedOK(t, checkRetainedReaderBudget(budget))
	opErr, cleanupErr = attempt()
	requireRetainedOK(t, errors.Join(opErr, cleanupErr))
	if manifest == nil || selected == (backup.RecoveryPoint{}) {
		t.Fatalf("retry returned manifest %+v and selection %+v", manifest, selected)
	}
	requireRetainedOK(t, checkRetainedReaderCheckpoint(t.Context(), path, publisher, f, selected, manifest, budget))
	requireRetainedOK(t, errors.Join(checkRetainedReaderFDs(path, baseline), checkRetainedReaderBudget(budget)))
}
