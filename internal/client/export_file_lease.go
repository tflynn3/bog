package client

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
)

const exportLocksDir = ".export-progress-locks"

// ErrExportCheckpointBusy means another owner holds the checkpoint lease.
var ErrExportCheckpointBusy = errors.New("export checkpoint is in use")

// FileExportProgressLease owns a checkpoint and pins its parent directory
// until Close. The lock entries persist so every open of a checkpoint name
// locks the same inode, including across processes and atomic replacements.
type FileExportProgressLease struct {
	root   *os.Root
	lock   *os.File
	name   string
	stages []string

	mu         sync.Mutex
	closed     bool
	running    bool
	lockSynced bool
}

// Acquire claims the checkpoint without waiting. Close releases ownership.
// Keep the reserved ownership directory in place while using this store.
func (s *FileExportProgressStore) Acquire() (_ *FileExportProgressLease, resultErr error) {
	root, err := os.OpenRoot(s.dir)
	if err != nil {
		return nil, fmt.Errorf("open export checkpoint directory: %w", err)
	}
	defer func() {
		if resultErr != nil {
			resultErr = exportIOError(resultErr, "close export checkpoint directory root", root.Close())
		}
	}()

	if err := root.Mkdir(exportLocksDir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("create export checkpoint lock directory: %w", err)
	}
	info, err := root.Lstat(exportLocksDir)
	if err != nil {
		return nil, fmt.Errorf("stat export checkpoint lock directory: %w", err)
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("export checkpoint lock directory must be a private directory")
	}
	// Let the filesystem compare names. In particular, two differently
	// cased names may refer to separate locks in this directory.
	lockPath := filepath.Join(exportLocksDir, s.name)
	lock, err := openExportLock(root, lockPath)
	if err != nil {
		return nil, fmt.Errorf("open export checkpoint lock: %w", err)
	}
	locked := false
	defer func() {
		if resultErr != nil {
			resultErr = joinQueryErrors(resultErr, closeExportLock(lock, locked))
		}
	}()
	lockInfo, err := lock.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat export checkpoint lock: %w", err)
	}
	entry, err := root.Lstat(lockPath)
	if err != nil {
		return nil, fmt.Errorf("stat export checkpoint lock entry: %w", err)
	}
	if !privateExportFile(entry) || !os.SameFile(entry, lockInfo) {
		return nil, errors.New("export checkpoint lock must be a private regular file with one link")
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("lock export checkpoint %q: %w; wait for the other export to finish", filepath.Join(s.dir, s.name), ErrExportCheckpointBusy)
		}
		return nil, fmt.Errorf("lock export checkpoint: %w", err)
	}
	locked = true

	owner, err := exportEntryName(root, exportLocksDir, s.name, lockInfo)
	if err != nil {
		return nil, fmt.Errorf("resolve export checkpoint lock name: %w", err)
	}
	stages := []string{exportStageName(owner)}
	addStage := func(name string) {
		stage := exportStageName(name)
		if !slices.Contains(stages, stage) {
			stages = append(stages, stage)
		}
	}
	addStage(s.name)
	// The previous implementation hashed the caller's spelling. The
	// installed entry preserves a legacy spelling that may differ from
	// the one used to reopen it. If a legacy writer used an unrecorded
	// spelling, its hash alone cannot identify the checkpoint.
	checkpoint, err := root.Lstat(s.name)
	switch {
	case err == nil:
		name, err := exportEntryName(root, ".", s.name, checkpoint)
		if err != nil {
			return nil, fmt.Errorf("resolve installed export checkpoint name: %w", err)
		}
		addStage(name)
	case !errors.Is(err, os.ErrNotExist):
		return nil, fmt.Errorf("stat export checkpoint: %w", err)
	}
	return &FileExportProgressLease{root: root, lock: lock, name: s.name, stages: stages}, nil
}

func openExportLock(root *os.Root, path string) (*os.File, error) {
	const flags = os.O_RDWR | syscall.O_NONBLOCK
	lock, err := root.OpenFile(path, flags|os.O_CREATE|os.O_EXCL, 0600)
	if !errors.Is(err, os.ErrExist) {
		return lock, err
	}
	info, err := root.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !privateExportFile(info) {
		return nil, errors.New("expected a private regular file with one link")
	}
	return root.OpenFile(path, flags, 0)
}

func exportStageName(name string) string {
	return fmt.Sprintf("%s%x", exportStagePrefix, sha256.Sum256([]byte(name)))
}

// Match the exact entry first. In a case-sensitive directory, separate
// entries can be hard links to the same inode and still need separate stages.
func exportEntryName(root *os.Root, dir, name string, info os.FileInfo) (_ string, resultErr error) {
	file, err := root.Open(dir)
	if err != nil {
		return "", err
	}
	defer func() { resultErr = exportIOError(resultErr, "close export entry directory", file.Close()) }()

	var alias string
	singleLink := info.Sys().(*syscall.Stat_t).Nlink == 1
	for {
		entries, err := file.ReadDir(128)
		if err != nil && !errors.Is(err, io.EOF) {
			return "", err
		}
		for _, entry := range entries {
			if entry.Name() == name {
				return name, nil
			}
			if !singleLink && !strings.EqualFold(entry.Name(), name) {
				continue
			}
			candidate, err := root.Lstat(filepath.Join(dir, entry.Name()))
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return "", err
			}
			if os.SameFile(info, candidate) {
				alias = entry.Name()
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}
	if alias == "" {
		return "", errors.New("entry disappeared during name resolution")
	}
	return alias, nil
}

func privateExportFile(info os.FileInfo) bool {
	return info.Mode().IsRegular() && info.Mode().Perm()&0077 == 0 && info.Sys().(*syscall.Stat_t).Nlink == 1
}

// CheckOutput rejects a sink that shares an inode with the checkpoint,
// lock, or any stage this lease can recover. Call before loading or saving.
func (s *FileExportProgressLease) CheckOutput(output os.FileInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	for _, name := range append([]string{s.name, filepath.Join(exportLocksDir, s.name)}, s.stages...) {
		info, err := s.root.Lstat(name)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat export checkpoint entry: %w", err)
		}
		if err == nil && os.SameFile(output, info) {
			return errors.New("checkpoint files must be separate from stdout")
		}
	}
	return nil
}

func (s *FileExportProgressLease) recoverStages() (bool, error) {
	removed := false
	for _, stage := range s.stages {
		info, err := s.root.Lstat(stage)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return removed, fmt.Errorf("stat staged export checkpoint: %w", err)
		}
		if !privateExportFile(info) {
			return removed, fmt.Errorf("staged export checkpoint %q must be a private regular file with one link", stage)
		}
		if err := s.root.Remove(stage); err != nil {
			return removed, fmt.Errorf("remove staged export checkpoint: %w", err)
		}
		removed = true
	}
	return removed, nil
}

func (s *FileExportProgressLease) checkOpen(ctx context.Context) error {
	if s.closed {
		return os.ErrClosed
	}
	return withQueryCancellation(ctx, nil)
}

func closeExportLock(lock *os.File, locked bool) error {
	var err error
	if locked {
		// Release even if a concurrent fork briefly inherited the descriptor.
		err = exportIOError(nil, "unlock export checkpoint", syscall.Flock(int(lock.Fd()), syscall.LOCK_UN))
	}
	return exportIOError(err, "close export checkpoint lock", lock.Close())
}

// Close releases the lease. An active ExportSession must finish first.
func (s *FileExportProgressLease) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return ErrExportRunning
	}
	if s.closed {
		return nil
	}
	s.closed = true
	return exportIOError(closeExportLock(s.lock, true), "close export checkpoint directory root", s.root.Close())
}

func (s *FileExportProgressStore) beginExportRun() (ExportProgressStore, func() error, error) {
	lease, err := s.Acquire()
	if err != nil {
		return nil, nil, err
	}
	return lease, lease.Close, nil
}

func (s *FileExportProgressLease) beginExportRun() (ExportProgressStore, func() error, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil, os.ErrClosed
	}
	if s.running {
		return nil, nil, ErrExportRunning
	}
	s.running = true
	return nil, func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.running = false
		return nil
	}, nil
}
