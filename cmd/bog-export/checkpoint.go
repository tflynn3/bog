package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"bog/internal/client"
)

const checkpointLockPrefix = ".bog-export-lock-"

func checkpointPath(path string) (string, error) {
	// Validate the original spelling before Abs cleans trailing separators.
	if _, err := client.NewFileExportProgressStore(path); err != nil {
		return "", err
	}
	if strings.HasPrefix(strings.ToLower(filepath.Base(path)), checkpointLockPrefix) {
		return "", errors.New("checkpoint path uses the reserved lock prefix")
	}
	return filepath.Abs(path)
}

// checkpointProgress keeps the client store exclusive across processes and
// checks whether this invocation was asked to start or resume. Later loads
// in the same invocation can recover from a retried query request.
type checkpointProgress struct {
	*client.FileExportProgressStore
	lock   *os.File
	resume bool
	loaded bool
}

func (p *checkpointProgress) Load(ctx context.Context, maxBytes int64) (client.ExportProgress, bool, error) {
	progress, found, err := p.FileExportProgressStore.Load(ctx, maxBytes)
	if err != nil {
		return client.ExportProgress{}, false, err
	}
	if !p.loaded {
		switch {
		case p.resume && !found:
			return client.ExportProgress{}, false, errors.New("checkpoint does not exist")
		case !p.resume && found:
			return client.ExportProgress{}, false, errors.New("checkpoint already exists; use -resume")
		}
		p.loaded = true
	}
	return progress, found, nil
}

func (p *checkpointProgress) close() error {
	return releaseCheckpointLock(p.lock)
}

func releaseCheckpointLock(lock *os.File) error {
	// Keep the lock file in place so concurrent opens share an inode.
	// Unlock explicitly before close. A concurrent fork can briefly
	// inherit the descriptor before exec closes it.
	return errors.Join(syscall.Flock(int(lock.Fd()), syscall.LOCK_UN), lock.Close())
}

func openCheckpoint(path string, resume bool, out io.Writer) (_ *checkpointProgress, resultErr error) {
	outputInfo, err := appendFileInfo(out)
	if err != nil {
		return nil, err
	}
	store, err := client.NewFileExportProgressStore(path)
	if err != nil {
		return nil, err
	}

	base := filepath.Base(path)
	// Case-fold the lock key to serialize aliases on case-insensitive
	// filesystems as well. The progress file itself retains its spelling.
	key := sha256.Sum256([]byte(strings.ToLower(base)))
	lockPath := filepath.Join(filepath.Dir(path), fmt.Sprintf("%s%x", checkpointLockPrefix, key))
	// O_NONBLOCK keeps startup bounded even if the lock path was replaced
	// with a special file. Never follow a symlink for the lock.
	lock, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, fmt.Errorf("open export checkpoint lock: %w", err)
	}
	release := lock.Close
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, release())
		}
	}()

	info, err := lock.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat export checkpoint lock: %w", err)
	}
	if !info.Mode().IsRegular() || os.SameFile(outputInfo, info) {
		return nil, errors.New("checkpoint lock must be a regular file separate from stdout")
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("lock export checkpoint: %w", err)
	}
	release = func() error { return releaseCheckpointLock(lock) }

	// The store may replace the progress file or remove a stale stage.
	// Keep stdout separate from both, including through hard links.
	stageKey := sha256.Sum256([]byte(base))
	stage := filepath.Join(filepath.Dir(path), fmt.Sprintf(".export-progress-stage-%x", stageKey))
	for _, candidate := range []string{path, stage} {
		info, err := os.Lstat(candidate)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("stat checkpoint entry: %w", err)
		}
		if err == nil && os.SameFile(outputInfo, info) {
			return nil, errors.New("checkpoint files must be separate from stdout")
		}
	}
	return &checkpointProgress{FileExportProgressStore: store, lock: lock, resume: resume}, nil
}

func appendFileInfo(out io.Writer) (os.FileInfo, error) {
	const requirement = "checkpoint mode requires stdout opened for append to a regular file"
	file, ok := out.(*os.File)
	if !ok {
		return nil, errors.New(requirement)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat stdout: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New(requirement)
	}
	raw, err := file.SyscallConn()
	if err != nil {
		return nil, fmt.Errorf("inspect stdout: %w", err)
	}
	var flags uintptr
	var flagsErr syscall.Errno
	if err := raw.Control(func(fd uintptr) {
		flags, _, flagsErr = syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFL, 0)
	}); err != nil {
		return nil, fmt.Errorf("inspect stdout flags: %w", err)
	}
	if flagsErr != 0 {
		return nil, fmt.Errorf("read stdout flags: %w", flagsErr)
	}
	if flags&syscall.O_APPEND == 0 || flags&syscall.O_ACCMODE == syscall.O_RDONLY {
		return nil, errors.New(requirement)
	}
	return info, nil
}

func exportCheckpointed(ctx context.Context, server *client.Client, opts options, progress *checkpointProgress, out *recordOutput) error {
	retries := 0
	session, err := server.NewExportSession(opts.query, opts.maxPages, progress,
		func(_ context.Context, page client.ExportPage) error {
			// A successful fetch resets the per-page retry budget. The
			// client calls this only after validating the entire page.
			retries = 0
			if err := writePage(out, page.Number, page.Page.Rows); err != nil {
				return err
			}
			// Output bytes can precede the durable progress update. A
			// restart may replay this page if interrupted in that window.
			if err := out.Sync(); err != nil {
				return fmt.Errorf("sync stdout (page %d): %w", page.Number, err)
			}
			return nil
		})
	if err != nil {
		return fmt.Errorf("prepare export session: %w", err)
	}
	for {
		err := outputCancellation(ctx, session.Run(ctx))
		if !retryableExportFetch(err) {
			return err
		}
		if retries == opts.retry.MaxRetries {
			return fmt.Errorf("%w (max retries %d): %w", client.ErrPageRetryExhausted, retries, err)
		}
		retries++
		if err := waitExportRetry(ctx, opts.retry.Delay); err != nil {
			return err
		}
	}
}

// ExportSession retries by reloading its durable state. Use the bounded
// walk's fetch classifications, leaving all sink and progress failures
// to an explicit restart.
func retryableExportFetch(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, client.ErrPageBytes) || errors.Is(err, client.ErrMalformedPage) {
		return false
	}
	if errors.Is(err, client.ErrPageInterrupted) {
		return true
	}
	var response *client.HTTPError
	return errors.As(err, &response) && response.Retryable()
}

func waitExportRetry(ctx context.Context, delay time.Duration) error {
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
	}
	return outputCancellation(ctx, nil)
}
