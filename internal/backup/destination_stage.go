package backup

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	destinationStagePrefix = ".bog-backup-stage-"
	destinationLockPrefix  = ".bog-backup-lock-"
	destinationLockPoll    = 10 * time.Millisecond
)

type destinationPublication struct {
	parent    *os.Root
	name      string
	stageName string
	lockName  string
	lock      *os.File
	lockInfo  os.FileInfo
	key       [sha256.Size]byte
}

// publishDestinationStage is the publication seam for Export and Restore.
// fill owns writing, validating, and syncing the image files. It writes only
// the flat backup image layout and leaves no file handles open on return.
//
// The caller controls the destination namespace and uses a trusted local
// filesystem with advisory locks and directory sync. Publishers of the same
// basename under the same pinned parent must all use this protocol. The lock
// key assumes a case-sensitive filesystem. The lock file is permanent:
// unlinking it could split subsequent callers across inodes.
func publishDestinationStage(ctx context.Context, destination string, fill func(*os.Root) error) error {
	return publishDestinationStageWith(ctx, destination, fill, destinationStageOps{
		sync:   syncDirectory,
		rename: (*os.Root).Rename,
	})
}

type destinationStageOps struct {
	sync   func(*os.Root) error
	rename func(*os.Root, string, string) error
}

func publishDestinationStageWith(ctx context.Context, destination string, fill func(*os.Root) error, ops destinationStageOps) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	path := filepath.Clean(destination)
	name := filepath.Base(path)
	lowerName := strings.ToLower(name)
	if name == "." || name == ".." || !filepath.IsLocal(name) ||
		strings.HasPrefix(lowerName, destinationStagePrefix) || strings.HasPrefix(lowerName, destinationLockPrefix) {
		return errors.New("invalid backup destination")
	}
	parent, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return err
	}
	p := destinationPublication{parent: parent, name: name, key: sha256.Sum256([]byte(name))}
	p.stageName = fmt.Sprintf("%s%x", destinationStagePrefix, p.key)
	p.lockName = fmt.Sprintf("%s%x", destinationLockPrefix, p.key)
	published := false
	defer func() {
		closeErr := parent.Close()
		if p.lock != nil {
			closeErr = errors.Join(closeErr, p.lock.Close())
		}
		if published && closeErr != nil {
			closeErr = fmt.Errorf("%w: close backup publication: %w", ErrPublicationUncertain, closeErr)
		}
		err = errors.Join(err, closeErr)
	}()

	if err := p.acquire(ctx); err != nil {
		return err
	}
	// A completed destination always wins, even if staging evidence exists.
	if _, err := p.emptyDestination(); err != nil {
		return err
	}
	if err := p.reclaim(); err != nil {
		return fmt.Errorf("recover backup staging: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	renameUncertain := false
	defer func() {
		if !published && !renameUncertain {
			if cleanupErr := p.reclaim(); cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("clean up backup staging: %w", cleanupErr))
			}
		}
	}()
	stageInfo, err := p.prepare(ctx, fill, ops)
	if err != nil {
		return err
	}

	placeholder, err := p.emptyDestination()
	if err != nil {
		return err
	}
	if err := p.checkLock(); err != nil {
		return err
	}
	if err := matchDestinationEntry(parent, p.stageName, stageInfo); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Root.Rename requires an absent directory target. Keep any placeholder
	// until the staged image is ready, and remove it only while it is empty.
	if placeholder != nil {
		if err := matchDestinationEntry(parent, name, placeholder); err != nil {
			return err
		}
		if err := parent.Remove(name); err != nil {
			return err
		}
	}
	if err := ops.rename(parent, p.stageName, name); err != nil {
		if errors.Is(err, syscall.EIO) {
			renameUncertain = true
			return fmt.Errorf("%w: rename backup image: %w", ErrPublicationUncertain, err)
		}
		return err
	}
	published = true
	if err := ops.sync(parent); err != nil {
		return fmt.Errorf("%w: sync backup parent: %w", ErrPublicationUncertain, err)
	}
	if err := p.clearRecord(); err != nil {
		return fmt.Errorf("%w: clear published backup staging record: %w", ErrPublicationUncertain, err)
	}
	return nil
}

func (p *destinationPublication) acquire(ctx context.Context) error {
	const flags = os.O_RDWR | syscall.O_NOFOLLOW | syscall.O_NONBLOCK
	f, err := p.parent.OpenFile(p.lockName, flags|os.O_CREATE|os.O_EXCL, 0600)
	created := err == nil
	if errors.Is(err, os.ErrExist) {
		f, err = p.parent.OpenFile(p.lockName, flags, 0)
	}
	if err != nil {
		return fmt.Errorf("open backup destination lock: %w", err)
	}
	p.lock = f
	if created {
		if err := f.Chmod(0600); err != nil {
			return err
		}
	}
	p.lockInfo, err = f.Stat()
	if err != nil {
		return err
	}
	if !privateDestinationFile(p.lockInfo) {
		return corrupt("unsafe backup destination lock %q", p.lockName)
	}

	ticker := time.NewTicker(destinationLockPoll)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		switch {
		case err == nil:
			if err := ctx.Err(); err != nil {
				return err
			}
			return p.checkLock()
		case errors.Is(err, syscall.EINTR):
			continue
		case !errors.Is(err, syscall.EWOULDBLOCK):
			return fmt.Errorf("lock backup destination: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (p *destinationPublication) checkLock() error {
	info, err := p.parent.Lstat(p.lockName)
	if err != nil {
		return err
	}
	if !privateDestinationFile(info) || !os.SameFile(info, p.lockInfo) {
		return corrupt("backup destination lock %q changed", p.lockName)
	}
	return nil
}

func (p *destinationPublication) emptyDestination() (os.FileInfo, error) {
	info, err := p.parent.Lstat(p.name)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New("backup destination must be a directory")
	}
	dir, err := p.parent.OpenFile(p.name, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	current, err := dir.Stat()
	if err == nil && !os.SameFile(info, current) {
		err = corrupt("backup destination changed during open")
	}
	if err == nil {
		names, readErr := dir.Readdirnames(1)
		err = unexpectedDirRead(readErr)
		if len(names) != 0 {
			err = errors.Join(err, errors.New("backup destination is not empty"))
		}
	}
	return info, errors.Join(err, dir.Close())
}

func (p *destinationPublication) prepare(ctx context.Context, fill func(*os.Root) error, ops destinationStageOps) (_ os.FileInfo, err error) {
	// Sync the intent and its parent entry before creating anything it owns.
	intent := p.intent()
	if err := p.writeRecord(intent[:], 0); err != nil {
		return nil, err
	}
	if err := syncDirectory(p.parent); err != nil {
		return nil, err
	}
	if err := p.parent.Mkdir(p.stageName, 0700); err != nil {
		return nil, err
	}
	info, err := p.parent.Lstat(p.stageName)
	if err != nil {
		return nil, err
	}
	if !privateDestinationDirectory(info, p.lockInfo) {
		return nil, corrupt("unsafe new backup staging directory")
	}
	if info.Mode().Perm() != 0700 {
		if err := p.parent.Chmod(p.stageName, 0700); err != nil {
			return nil, err
		}
	}

	binding := p.binding(info)
	if err := p.writeRecord(binding[:], destinationIntentSize); err != nil {
		return nil, err
	}
	// The image may only be populated once its name and identity are durable.
	if err := syncDirectory(p.parent); err != nil {
		return nil, err
	}
	stage, err := p.openStage(info)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, stage.Close()) }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := fill(stage); err != nil {
		return nil, err
	}
	return info, ops.sync(stage)
}

func (p *destinationPublication) openStage(info os.FileInfo) (*os.Root, error) {
	stage, err := p.parent.OpenRoot(p.stageName)
	if err != nil {
		return nil, err
	}
	current, err := stage.Stat(".")
	if err == nil && !os.SameFile(info, current) {
		err = corrupt("backup staging directory changed during open")
	}
	if err != nil {
		return nil, errors.Join(err, stage.Close())
	}
	return stage, nil
}

func matchDestinationEntry(root *os.Root, name string, expected os.FileInfo) error {
	current, err := root.Lstat(name)
	if err != nil {
		return err
	}
	if !os.SameFile(expected, current) || expected.Mode().Type() != current.Mode().Type() {
		return corrupt("backup publication entry %q changed", name)
	}
	return nil
}
