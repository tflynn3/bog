package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const tmpfsSize = 8 << 20
const mountTimeout = 5 * time.Second
const diskDirectionTimeout = time.Minute

type diskMountOps struct {
	device        func(path string) (uint64, error)
	mount         func(ctx context.Context, path string) error
	unmount       func(ctx context.Context, path string, lazy bool) error
	verifyFresh   func(path string) error
	verifyHealthy func(path string) error
}

type diskFixture struct {
	root, mountpoint, full, healthy, link, filler string
	ops                                           diskMountOps
	parentDevice                                  uint64
	mountAttempted                                bool
}

func newFixtureRoot() (string, error) {
	parent, err := filepath.Abs(os.TempDir())
	if err != nil {
		return "", fmt.Errorf("resolve fixture temporary directory: %w", err)
	}
	root, err := os.MkdirTemp(parent, "bog-durability-")
	if err != nil {
		return "", fmt.Errorf("create fixture directory: %w", err)
	}
	return root, nil
}

func newDiskFixture(root string, ops diskMountOps) (*diskFixture, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve disk fixture directory: %w", err)
	}
	dir, err := os.MkdirTemp(root, "disk-")
	if err != nil {
		return nil, fmt.Errorf("create disk fixture directory: %w", err)
	}
	mountpoint := filepath.Join(dir, "enospc")
	return &diskFixture{
		root: dir, mountpoint: mountpoint, ops: ops,
		full: filepath.Join(mountpoint, "data"), filler: filepath.Join(mountpoint, "filler"),
		healthy: filepath.Join(dir, "healthy"),
	}, nil
}

func (f *diskFixture) prepare(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := os.Mkdir(f.mountpoint, 0700); err != nil {
		return false, fmt.Errorf("create mountpoint: %w", err)
	}
	dev, err := f.ops.device(f.mountpoint)
	if err != nil {
		return false, fmt.Errorf("inspect mountpoint before mount: %w", err)
	}
	f.parentDevice = dev
	// The helper may report a failure or cancellation after the kernel mounts
	// the filesystem. Cleanup must inspect the path even on that return.
	f.mountAttempted = true
	mountCtx, cancel := context.WithTimeout(ctx, mountTimeout)
	err = f.ops.mount(mountCtx, f.mountpoint)
	if err == nil {
		err = mountCtx.Err()
	}
	cancel()
	if err != nil {
		return false, fmt.Errorf("mount private tmpfs: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	dev, err = f.ops.device(f.mountpoint)
	if err != nil {
		return false, fmt.Errorf("inspect mounted tmpfs: %w", err)
	}
	if dev == f.parentDevice {
		return false, errors.New("tmpfs mount did not replace the mountpoint filesystem")
	}
	if err := f.ops.verifyFresh(f.mountpoint); err != nil {
		return false, fmt.Errorf("validate private tmpfs: %w", err)
	}
	if err := os.Mkdir(f.full, 0700); err != nil {
		return true, fmt.Errorf("create tmpfs data directory: %w", err)
	}
	if err := os.Mkdir(f.healthy, 0700); err != nil {
		return true, fmt.Errorf("create healthy data directory: %w", err)
	}
	if err := f.ops.verifyHealthy(f.healthy); err != nil {
		return true, fmt.Errorf("validate healthy data directory: %w", err)
	}
	return true, ctx.Err()
}

func (f *diskFixture) cleanup(ctx context.Context) (err error) {
	if f.mountAttempted {
		detached, detachErr := f.detach(ctx)
		if !detached {
			// Leave the tree in place when the mount state is unknown. The
			// caller also removes its parent non-recursively.
			return fmt.Errorf("retain disk fixture %s: %w", f.root, detachErr)
		}
		err = detachErr
	}
	if removeErr := os.RemoveAll(f.root); removeErr != nil {
		err = errors.Join(err, fmt.Errorf("remove disk fixture: %w", removeErr))
	}
	return err
}

func (f *diskFixture) detach(ctx context.Context) (bool, error) {
	dev, err := f.ops.device(f.mountpoint)
	if err == nil && dev == f.parentDevice {
		return true, nil
	}
	if err != nil {
		err = fmt.Errorf("inspect mountpoint during cleanup: %w", err)
	}
	ctx = context.WithoutCancel(ctx)
	if unmountErr := f.unmount(ctx, false); unmountErr != nil {
		err = errors.Join(err, unmountErr)
	}
	detached, inspectErr := f.detached()
	if detached {
		return true, err
	}
	// A busy mount can be detached from the namespace after an ordinary
	// unmount fails. The ordinary failure still fails the case.
	err = errors.Join(err, inspectErr, f.unmount(ctx, true))
	detached, inspectErr = f.detached()
	return detached, errors.Join(err, inspectErr)
}

func (f *diskFixture) unmount(ctx context.Context, lazy bool) error {
	ctx, cancel := context.WithTimeout(ctx, mountTimeout)
	defer cancel()
	if err := f.ops.unmount(ctx, f.mountpoint, lazy); err != nil {
		if lazy {
			return fmt.Errorf("detach tmpfs: %w", err)
		}
		return fmt.Errorf("unmount tmpfs: %w", err)
	}
	return nil
}

func (f *diskFixture) detached() (bool, error) {
	dev, err := f.ops.device(f.mountpoint)
	if err != nil {
		return false, fmt.Errorf("verify unmounted tmpfs: %w", err)
	}
	if dev != f.parentDevice {
		return false, errors.New("tmpfs remains mounted")
	}
	return true, nil
}

func (f *diskFixture) runDirection(ctx context.Context, target string, run func(context.Context) error) (err error) {
	ctx, cancel := context.WithTimeout(ctx, diskDirectionTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := retargetDirectory(f.link, target); err != nil {
		return fmt.Errorf("set launch symlink: %w", err)
	}
	defer func() {
		if removeErr := os.Remove(f.filler); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("remove filesystem filler: %w", removeErr))
		}
	}()
	if err := run(ctx); err != nil {
		return err
	}
	return ctx.Err()
}

// retargetDirectory replaces the symlink atomically while the server is live.
func retargetDirectory(path, target string) error {
	next := path + ".next"
	if err := os.Symlink(target, next); err != nil {
		return err
	}
	if err := os.Rename(next, path); err != nil {
		return errors.Join(err, os.Remove(next))
	}
	return nil
}

type diskEvidence struct {
	reconciliation
	Errno                     string            `json:"errno"`
	DisposableLocalFilesystem bool              `json:"disposable_local_filesystem"`
	ObservedENOSPC            bool              `json:"observed_enospc"`
	RecoveredAfterSpaceFreed  bool              `json:"recovered_after_space_freed"`
	StaleCursorsRejected      int               `json:"stale_cursors_rejected"`
	HealthyToFull             *retargetEvidence `json:"healthy_to_full,omitempty"`
	FullToHealthy             *retargetEvidence `json:"full_to_healthy,omitempty"`
}

type retargetEvidence struct {
	ReadinessStatus      int    `json:"readiness_status"`
	ReadinessReason      string `json:"readiness_reason"`
	IngestStatus         int    `json:"ingest_status"`
	PostIngestReason     string `json:"post_ingest_reason,omitempty"`
	PostIngestCategory   string `json:"post_ingest_category,omitempty"`
	ReplayedAfterRestart bool   `json:"replayed_after_restart,omitempty"`
}

type diskDirection string

const (
	healthyToFull diskDirection = "healthy_to_full"
	fullToHealthy diskDirection = "full_to_healthy"
)

func runDiskFixture(ctx context.Context, root string, ops diskMountOps, check func(context.Context, *diskFixture, *diskEvidence, diskDirection) error) (proof diskEvidence, err error) {
	fixture, err := newDiskFixture(root, ops)
	if err != nil {
		return proof, fmt.Errorf("set up disk fixture: %w", err)
	}
	defer func() {
		if cleanupErr := fixture.cleanup(ctx); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("clean up disk fixture: %w", cleanupErr))
		}
	}()
	proof.DisposableLocalFilesystem, err = fixture.prepare(ctx)
	if err != nil {
		return proof, fmt.Errorf("set up disk fixture: %w", err)
	}

	proof.HealthyToFull = &retargetEvidence{}
	proof.FullToHealthy = &retargetEvidence{}
	for _, direction := range []struct {
		name   diskDirection
		target string
	}{
		{healthyToFull, fixture.healthy},
		{fullToHealthy, fixture.full},
	} {
		fixture.link = filepath.Join(fixture.root, string(direction.name))
		probeErr := fixture.runDirection(ctx, direction.target, func(ctx context.Context) error {
			return check(ctx, fixture, &proof, direction.name)
		})
		if probeErr != nil {
			err = errors.Join(err, fmt.Errorf("%s: %w", direction.name, probeErr))
		}
	}
	return proof, err
}
