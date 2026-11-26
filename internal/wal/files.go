package wal

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"syscall"
)

const (
	markerMagic       = "BOGCMT01"
	markerSize        = 24
	walStagePrefix    = ".bog-wal-init-"
	markerStagePrefix = ".bog-commit-"
	stageIDSize       = 16
)

func openRegular(dir *os.File, name string, flags int) (*os.File, error) {
	fd, err := openat(int(dir.Fd()), name, flags|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return nil, &os.PathError{Op: "openat", Path: name, Err: err}
	}
	f := os.NewFile(uintptr(fd), name)
	info, err := f.Stat()
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	if !info.Mode().IsRegular() {
		return nil, errors.Join(fmt.Errorf("%s is not a regular file", name), f.Close())
	}
	if st, ok := info.Sys().(*syscall.Stat_t); !ok || st.Nlink != 1 ||
		st.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0022 != 0 {
		return nil, errors.Join(fmt.Errorf("%s has unsafe ownership, permissions, or link count", name), f.Close())
	}
	return f, nil
}

// openParent resolves the parent of the pinned directory, even when the path
// used to open that directory was "." or a symlink.
func openParent(dir *os.File) (*os.File, error) {
	fd, err := openat(int(dir.Fd()), "..", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), "data directory parent"), nil
}

func (l *Log) removeStage(name string) error {
	if err := l.root.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove WAL staging entry: %w", err)
	}
	return nil
}

// writeStaged closes and syncs a new file before its name can be installed.
func (l *Log) writeStaged(prefix string, data []byte) (string, error) {
	var id [stageIDSize]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	name := prefix + hex.EncodeToString(id[:])
	f, err := openRegular(l.dir, name, os.O_CREATE|os.O_EXCL|os.O_WRONLY)
	if err != nil {
		return "", err
	}
	// Apply the final mode to the new inode even under a restrictive umask.
	// Existing authoritative files never go through this path.
	err = f.Chmod(0600)
	if err == nil {
		err = writeAll(f, data)
	}
	if err == nil {
		err = f.Sync()
	}
	if err = errors.Join(err, f.Close()); err != nil {
		return "", errors.Join(err, l.removeStage(name))
	}
	return name, nil
}

// commit runs after a WAL sync. Once rename has been attempted, the caller
// must stop on error and let recovery determine which marker is authoritative.
func (l *Log) commit(offset int64) (string, error) {
	var b [markerSize]byte
	copy(b[:], markerMagic)
	binary.LittleEndian.PutUint64(b[8:16], uint64(offset))
	binary.LittleEndian.PutUint64(b[16:], ^uint64(offset))

	name, err := l.writeStaged(markerStagePrefix, b[:])
	if err != nil {
		return "marker_preparation", err
	}
	if err := l.root.Rename(name, markerName); err != nil {
		return "marker_rename", errors.Join(ErrUncertain, err)
	}
	if err := l.syncDir(); err != nil {
		return "marker_sync", errors.Join(ErrUncertain, err)
	}
	return "", nil
}
