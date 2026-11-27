package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type publisher func(context.Context, string, func(*os.Root) error) error

type publicationOps struct {
	rename     func(*os.Root, *os.File, string, string, bool) error
	syncParent func(*os.File) error
}

func publish(ctx context.Context, destination string, fill func(*os.Root) error) error {
	return publishWithOps(ctx, destination, fill, publicationOps{renameStage, (*os.File).Sync})
}

func publishWithOps(ctx context.Context, destination string, fill func(*os.Root) error, ops publicationOps) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	path := filepath.Clean(destination)
	name := filepath.Base(path)
	if name == "." || name == ".." || !filepath.IsLocal(name) || strings.HasPrefix(strings.ToLower(name), stagingPrefix) {
		return errors.New("invalid backup destination")
	}
	parent, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { err = publicationClose(err, parent.Close()) }()
	dir, err := parent.Open(".")
	if err != nil {
		return err
	}
	defer func() { err = publicationClose(err, dir.Close()) }()
	info, err := dir.Stat()
	if err != nil {
		return err
	}
	guard, err := openStagingGuard(ctx, parent, dir, info)
	if err != nil {
		return err
	}
	defer func() { err = publicationClose(err, guard.file.Close()) }()
	claim := newStageClaim(parent, dir, info, name)
	attempt, err := claim.begin(ctx, guard)
	if err != nil {
		return err
	}
	defer func() { err = publicationClose(err, attempt.file.Close()) }()

	fillErr := ctx.Err()
	if fillErr == nil {
		fillErr = attempt.fill(fill)
	}
	// Even a canceled fill should clean up. Bound the wait if the backup
	// guard is held by another process. A later retry can finish cleanup.
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := guard.lock(finishCtx); err != nil {
		return errors.Join(fillErr, ctx.Err(), err)
	}
	defer func() { err = publicationClose(err, unlock(guard.file)) }()
	if fillErr = errors.Join(fillErr, ctx.Err()); fillErr != nil {
		return errors.Join(fillErr, claim.clear(attempt.name, attempt.info, true))
	}
	exists, err := emptyDestination(parent, name)
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = checkStageIdentity(parent, attempt.name, attempt.info)
	}
	if err != nil {
		return errors.Join(err, claim.clear(attempt.name, attempt.info, true))
	}
	if err := ops.rename(parent, dir, attempt.name, name, exists); err != nil {
		if errors.Is(err, syscall.EIO) {
			return publicationUncertain("rename stage", err)
		}
		return errors.Join(err, claim.clear(attempt.name, attempt.info, true))
	}
	if err := ops.syncParent(dir); err != nil {
		return publicationUncertain("sync published destination", err)
	}
	if err := claim.clearClaim(true); err != nil {
		return publicationUncertain("clear published staging claim", err)
	}
	return nil
}

func publicationUncertain(step string, err error) error {
	return fmt.Errorf("%w: %s: %w", ErrPublicationUncertain, step, err)
}

// Close or unlock errors after success carry the same reopen requirement as
// a failed final directory sync.
func publicationClose(err, closeErr error) error {
	if err == nil && closeErr != nil {
		return publicationUncertain("release publication resources", closeErr)
	}
	return errors.Join(err, closeErr)
}

func (a stageAttempt) fill(fill func(*os.Root) error) (err error) {
	stage, err := a.parent.OpenRoot(a.name)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, stage.Close()) }()
	info, err := stage.Stat(".")
	if err != nil {
		return err
	}
	if !os.SameFile(a.info, info) {
		return errors.New("backup stage changed during open")
	}
	if err := fill(stage); err != nil {
		return err
	}
	return syncDirectory(stage)
}

func emptyDestination(parent *os.Root, name string) (bool, error) {
	st, err := parent.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !st.IsDir() {
		return false, errors.New("backup destination must be a directory")
	}
	f, err := parent.OpenFile(name, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return false, err
	}
	names, readErr := f.Readdirnames(1)
	readErr = unexpectedDirRead(readErr)
	if len(names) != 0 {
		readErr = errors.Join(readErr, errors.New("backup destination is not empty"))
	}
	return true, errors.Join(readErr, f.Close())
}

func syncDirectory(root *os.Root) error {
	d, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}
