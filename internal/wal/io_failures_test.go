package wal

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"bog/internal/store"
)

func TestPostOpenStatFailureCleansOnlyExclusiveFiles(t *testing.T) {
	for _, tc := range []struct {
		name      string
		exclusive bool
	}{
		{".bog-wal-init-test", true},
		{".bog-commit-test", true},
		{"bog.wal", false},
		{"bog.commit", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, err := os.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := dir.Close(); err != nil {
					t.Error(err)
				}
			})
			path := filepath.Join(dir.Name(), tc.name)
			flags := os.O_RDWR
			if tc.exclusive {
				flags |= os.O_CREATE | os.O_EXCL
			} else if err := os.WriteFile(path, nil, 0600); err != nil {
				t.Fatal(err)
			}
			fd, err := openat(int(dir.Fd()), tc.name, flags|syscall.O_CLOEXEC, 0600)
			if err != nil {
				t.Fatal(err)
			}
			f := os.NewFile(uintptr(fd), tc.name)
			err = validateOpenFile(dir, f, tc.exclusive, func() (os.FileInfo, error) { return nil, syscall.EIO })
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("validation error = %v", err)
			}
			if _, err = f.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("file remained open: %v", err)
			}
			_, err = os.Stat(path)
			if tc.exclusive && !errors.Is(err, os.ErrNotExist) || !tc.exclusive && err != nil {
				t.Fatalf("exclusive = %t, path stat = %v", tc.exclusive, err)
			}
		})
	}
}

type rollbackFailureFile struct {
	file
	failAt string
}

func (f rollbackFailureFile) Write([]byte) (int, error) { return 0, syscall.ENOSPC }

func (f rollbackFailureFile) Truncate(size int64) error {
	if f.failAt == "truncate" {
		return syscall.EIO
	}
	return f.file.Truncate(size)
}

func (f rollbackFailureFile) Sync() error {
	if f.failAt == "sync" {
		return syscall.EIO
	}
	return f.file.Sync()
}

func (f rollbackFailureFile) Seek(offset int64, whence int) (int64, error) {
	if f.failAt == "seek" {
		return 0, syscall.EIO
	}
	return f.file.Seek(offset, whence)
}

func TestUncertainRollbackPreservesStorageErrors(t *testing.T) {
	for _, step := range []string{"truncate", "sync", "seek"} {
		t.Run(step, func(t *testing.T) {
			f, err := os.CreateTemp(t.TempDir(), "wal")
			if err != nil {
				t.Fatal(err)
			}
			l := &Log{f: rollbackFailureFile{file: f, failAt: step}, s: &store.Store{}}
			l.diagnostics.queue = make(diagnosticQueue, 1)
			t.Cleanup(func() {
				if err := l.Close(); err != nil {
					t.Error(err)
				}
			})
			rows, err := store.Decode(batch)
			if err != nil {
				t.Fatal(err)
			}
			err = l.Append(batch, rows)
			for _, want := range []error{ErrUncertain, syscall.ENOSPC, syscall.EIO} {
				if !errors.Is(err, want) {
					t.Errorf("append error = %v, missing %v", err, want)
				}
			}
			first := l.LastCommitOutcome()
			if first.State != "uncertain" || first.Category != "enospc" || first.At.IsZero() {
				t.Fatalf("uncertain outcome = %+v", first)
			}
			if err := l.Append(batch, rows); err == nil || !errors.Is(err, ErrUncertain) {
				t.Fatalf("poisoned append = %v", err)
			}
			if got := l.LastCommitOutcome(); got != first {
				t.Fatalf("poisoned append changed outcome to %+v", got)
			}
			if len(l.diagnostics.queue) != 0 {
				t.Fatal("reported a confirmed rollback")
			}
		})
	}
}

type headerFailureFile struct {
	file
	failAt string
}

func (f headerFailureFile) Seek(offset int64, whence int) (int64, error) {
	if f.failAt == "size seek" && whence == io.SeekEnd ||
		f.failAt == "header seek" && whence == io.SeekStart {
		return 0, syscall.EIO
	}
	return f.file.Seek(offset, whence)
}

func (f headerFailureFile) Read(p []byte) (int, error) {
	if f.failAt == "header read" {
		return 0, syscall.EIO
	}
	return f.file.Read(p)
}

func TestReplayPreservesHeaderIOErrors(t *testing.T) {
	for _, step := range []string{"size seek", "header seek", "header read"} {
		t.Run(step, func(t *testing.T) {
			f, err := os.CreateTemp(t.TempDir(), "wal")
			if err != nil {
				t.Fatal(err)
			}
			l := &Log{f: headerFailureFile{file: f, failAt: step}}
			t.Cleanup(func() {
				if err := l.Close(); err != nil {
					t.Error(err)
				}
			})
			if err = l.replay(int64(len(magic)), store.NewRecoveryBuilder()); !errors.Is(err, syscall.EIO) {
				t.Fatalf("replay error = %v", err)
			}
		})
	}
}
