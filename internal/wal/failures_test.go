package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"bog/internal/store"
)

const testWait = 5 * time.Second

func awaitTestValue[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(testWait):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

type ioFailureFile struct {
	file
	writeErr, truncateErr, syncErr, seekErr, readErr error
	seekWhence                                       int
}

func (f *ioFailureFile) Write(p []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return f.file.Write(p)
}

func (f *ioFailureFile) Truncate(n int64) error {
	if f.truncateErr != nil {
		return f.truncateErr
	}
	return f.file.Truncate(n)
}

func (f *ioFailureFile) Sync() error {
	if f.syncErr != nil {
		return f.syncErr
	}
	return f.file.Sync()
}

func (f *ioFailureFile) Seek(offset int64, whence int) (int64, error) {
	if f.seekErr != nil && whence == f.seekWhence {
		return 0, f.seekErr
	}
	return f.file.Seek(offset, whence)
}

func (f *ioFailureFile) Read(p []byte) (int, error) {
	if f.readErr != nil {
		return 0, f.readErr
	}
	return f.file.Read(p)
}

func checkCommitOutcome(t *testing.T, l *Log, state CommitState, operation, category string) {
	t.Helper()
	got := l.LastCommitOutcome()
	if got.State != state || got.At.IsZero() != (state == CommitNone) || got.Operation != operation || got.Category != category {
		t.Fatalf("commit outcome = %+v, want state=%q operation=%q category=%q", got, state, operation, category)
	}
}

func TestUncertainRollbackPreservesCauses(t *testing.T) {
	for _, name := range []string{"truncate", "sync", "seek"} {
		t.Run(name, func(t *testing.T) {
			l, err := Open(t.TempDir(), &store.Store{})
			if err != nil {
				t.Fatal(err)
			}
			defer closeLog(t, l)
			rows, err := store.Decode(batch)
			if err != nil {
				t.Fatal(err)
			}
			ff := &ioFailureFile{file: l.f, writeErr: syscall.ENOSPC}
			switch name {
			case "truncate":
				ff.truncateErr = syscall.EIO
			case "sync":
				ff.syncErr = syscall.EIO
			case "seek":
				ff.seekErr = syscall.EIO
				ff.seekWhence = io.SeekStart
			}
			l.f = ff
			out := captureDiagnostics(l)
			checkCommitOutcome(t, l, CommitNone, "", "")
			err = l.Append(batch, rows)
			for _, want := range []error{ErrUncertain, syscall.ENOSPC, syscall.EIO} {
				if !errors.Is(err, want) {
					t.Fatalf("append error %v is missing %v", err, want)
				}
			}
			checkCommitOutcome(t, l, CommitUncertain, "wal_write", "enospc")
			before := l.LastCommitOutcome()
			if got := l.Append(batch, rows); got != err {
				t.Fatalf("poisoned append = %v, want %v", got, err)
			}
			if got := l.LastCommitOutcome(); got != before {
				t.Fatalf("poisoned append changed outcome from %+v to %+v", before, got)
			}
			checkNoDiagnostic(t, out)
		})
	}
}

func TestUncertainCommitRejectsQueuedAppendsWithCause(t *testing.T) {
	l, err := Open(t.TempDir(), &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeLog(t, l)
	f := &groupFile{File: l.f.(*os.File), entered: make(chan struct{}), release: make(chan struct{})}
	l.f = f
	l.syncDir = func() error { return syscall.EIO }
	release := sync.OnceFunc(func() { close(f.release) })
	defer release()
	rows, err := store.Decode(batch)
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 2)
	go func() { results <- l.Append(batch, rows) }()
	awaitTestValue(t, f.entered, "first WAL sync")
	checkCommitOutcome(t, l, CommitNone, "", "")
	go func() { results <- l.Append(batch, rows) }()
	deadline := time.Now().Add(testWait)
	for {
		l.mu.Lock()
		queued := len(l.queue)
		l.mu.Unlock()
		if queued != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second append was not queued")
		}
		runtime.Gosched()
	}
	release()
	for range cap(results) {
		if err := awaitTestValue(t, results, "append after uncertain commit"); !errors.Is(err, ErrUncertain) || !errors.Is(err, syscall.EIO) {
			t.Errorf("append error = %v", err)
		}
	}
	checkCommitOutcome(t, l, CommitUncertain, "marker_publication", "eio")
	if f.writes != 2 {
		t.Fatalf("got %d writes, want only the first group's header and payload", f.writes)
	}
}

func TestMarkerRenameFailurePreservesCauseAndRemovesTemp(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeLog(t, l)
	marker := filepath.Join(dir, "bog.commit")
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(marker, 0700); err != nil {
		t.Fatal(err)
	}
	rows, err := store.Decode(batch)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append(batch, rows); !errors.Is(err, ErrUncertain) || !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("append error = %v", err)
	}
	checkCommitOutcome(t, l, CommitUncertain, "marker_publication", "other")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Name() != "bog.commit" || entries[1].Name() != "bog.wal" {
		t.Fatalf("directory entries = %v", entries)
	}
	info, err := entries[1].Info()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(len(magic)+16+len(batch)) {
		t.Fatalf("WAL size after uncertain publication = %d", info.Size())
	}
}

func TestOpenRegularStatFailureCleanup(t *testing.T) {
	for _, tc := range []struct {
		name               string
		flags              int
		existing, wantStat bool
		want               error
	}{
		{".bog-wal-init-test", os.O_CREATE | os.O_EXCL | os.O_RDWR, false, true, syscall.EIO},
		{".bog-commit-test", os.O_CREATE | os.O_EXCL | os.O_WRONLY, false, true, syscall.EIO},
		{"bog.wal", os.O_RDWR, true, true, syscall.EIO},
		{"bog.commit", os.O_CREATE | os.O_EXCL | os.O_WRONLY, true, false, os.ErrExist},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := t.TempDir()
			dir, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := dir.Close(); err != nil {
					t.Error(err)
				}
			}()
			name := filepath.Join(path, tc.name)
			if tc.existing {
				if err := os.WriteFile(name, []byte("existing"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			var opened *os.File
			f, err := openRegularWithStat(dir, tc.name, tc.flags, func(f *os.File) (os.FileInfo, error) {
				opened = f
				return nil, syscall.EIO
			})
			if f != nil || !errors.Is(err, tc.want) || (opened != nil) != tc.wantStat {
				t.Fatalf("opened file = %v, stat file = %v, error = %v", f, opened, err)
			}
			if opened != nil {
				if _, err := opened.Stat(); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("descriptor was not closed: %v", err)
				}
			}
			if got, err := os.ReadFile(name); tc.existing {
				if err != nil || string(got) != "existing" {
					t.Fatalf("existing file changed: %q, %v", got, err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("exclusive file was not removed: %v", err)
			}
		})
	}
}

func TestReplayPreservesHeaderIOFailures(t *testing.T) {
	for _, tc := range []struct {
		name       string
		seekWhence int
		readErr    error
	}{
		{"seek end", io.SeekEnd, nil},
		{"seek start", io.SeekStart, nil},
		{"read magic", -1, syscall.EIO},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bog.wal")
			if err := os.WriteFile(path, []byte(magic), 0600); err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			l := &Log{f: &ioFailureFile{file: f, seekErr: syscall.EIO, seekWhence: tc.seekWhence, readErr: tc.readErr}}
			defer closeLog(t, l)
			if err := l.replay(int64(len(magic)), store.NewRecoveryBuilder()); !errors.Is(err, syscall.EIO) {
				t.Fatalf("replay error = %v", err)
			}
		})
	}
}

func TestObserveCommitOutcomeConcurrently(t *testing.T) {
	l, err := Open(t.TempDir(), &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeLog(t, l)
	rows, err := store.Decode(batch)
	if err != nil {
		t.Fatal(err)
	}
	const appends = 16
	var writers sync.WaitGroup
	results := make(chan error, appends)
	firstRead := make(chan struct{})
	stop := make(chan struct{})
	observed := make(chan error, 1)
	go func() {
		close(firstRead)
		var last time.Time
		for {
			outcome := l.LastCommitOutcome()
			_ = l.DroppedDiagnostics()
			if outcome.At.Before(last) || (outcome.State != CommitNone && outcome.State != CommitDurable) {
				observed <- fmt.Errorf("unexpected outcome: %+v", outcome)
				return
			}
			last = outcome.At
			select {
			case <-stop:
				observed <- nil
				return
			default:
			}
		}
	}()
	<-firstRead
	for range appends {
		writers.Go(func() { results <- l.Append(batch, rows) })
	}
	writers.Wait()
	close(stop)
	if err := <-observed; err != nil {
		t.Error(err)
	}
	for range appends {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
	checkCommitOutcome(t, l, CommitDurable, "", "")
}
