package wal

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"bog/internal/store"
)

func TestLegacyOpenRejectsRestoreLayoutWithoutMutation(t *testing.T) {
	const (
		backup = "bog.backup"
		tail   = "bog.wal.tail"
	)
	for _, tc := range []struct {
		name, diagnostic string
		metadata, suffix bool
		legacy, symlink  bool
	}{
		{name: "restored image", diagnostic: backup, metadata: true, suffix: true},
		{name: "metadata alone", diagnostic: backup, metadata: true},
		{name: "tail alone", diagnostic: tail, suffix: true},
		{name: "mixed legacy and backup", diagnostic: backup, metadata: true, suffix: true, legacy: true},
		{name: "mixed legacy and tail", diagnostic: tail, suffix: true, legacy: true},
		{name: "dangling reserved entry", diagnostic: tail, suffix: true, symlink: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			write := func(name, contents string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.legacy {
				l := openCaptureTestLog(t, dir)
				payload, rows := testBatch(t, "legacy")
				if err := l.Append(payload, rows); err != nil {
					t.Fatal(err)
				}
				if err := l.Close(); err != nil {
					t.Fatal(err)
				}
				// A normal startup would truncate this uncommitted suffix.
				f, err := os.OpenFile(filepath.Join(dir, walName), os.O_WRONLY|os.O_APPEND, 0)
				if err != nil {
					t.Fatal(err)
				}
				if err := errors.Join(writeAll(f, []byte("uncommitted")), f.Close()); err != nil {
					t.Fatal(err)
				}
			}
			write(walStagePrefix+strings.Repeat("0", 2*stageIDSize), "orphaned WAL staging")
			write(markerStagePrefix+strings.Repeat("a", 2*stageIDSize), "orphaned marker staging")
			if tc.metadata {
				write(backup, "backup metadata")
			}
			if tc.suffix {
				if tc.symlink {
					if err := os.Symlink("absent", filepath.Join(dir, tail)); err != nil {
						t.Fatal(err)
					}
				} else {
					write(tail, "backup tail")
				}
			}
			if tc.metadata && tc.suffix {
				write("bog.checkpoint", "checkpoint")
				write("segment-000", "checkpoint data")
			}

			before := restoreDirectoryState(t, dir)
			var live store.Store
			_, rows := testBatch(t, "preexisting store row")
			live.Publish(rows)
			for range 2 {
				l, err := Open(dir, &live)
				if l != nil {
					err = errors.Join(err, l.Close())
					t.Fatalf("legacy Open accepted a restore layout: %v", err)
				}
				if err == nil || !strings.Contains(err.Error(), tc.diagnostic) || !strings.Contains(err.Error(), "checkpoint-aware recovery") {
					t.Fatalf("restore diagnostic = %v", err)
				}
				if after := restoreDirectoryState(t, dir); !reflect.DeepEqual(after, before) {
					t.Fatalf("legacy Open changed restore layout:\nbefore = %v\nafter = %v", before, after)
				}
				if got := allTestRows(&live); !reflect.DeepEqual(got, rows) {
					t.Fatalf("legacy Open changed store contents to %v", got)
				}
			}
		})
	}
}

func TestRestoreGuardChecksUnderDirectoryLock(t *testing.T) {
	dir := t.TempDir()
	l := openCaptureTestLog(t, dir)
	if err := os.WriteFile(filepath.Join(dir, "bog.wal.tail"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	other, err := Open(dir, &store.Store{})
	if other != nil {
		t.Fatalf("second Open unexpectedly acquired directory: %v", errors.Join(err, other.Close()))
	}
	if !errors.Is(err, syscall.EWOULDBLOCK) || !strings.Contains(err.Error(), "lock data directory") {
		t.Fatalf("Open with an active directory owner = %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	other, err = Open(dir, &store.Store{})
	if other != nil {
		t.Fatalf("Open accepted restore layout: %v", errors.Join(err, other.Close()))
	}
	if err == nil || !strings.Contains(err.Error(), "checkpoint-aware recovery") {
		t.Fatalf("Open after releasing the directory = %v", err)
	}
}

func TestLegacyOpenAllowsCheckpointWithFullWAL(t *testing.T) {
	dir := t.TempDir()
	l := openCaptureTestLog(t, dir)
	payload, rows := testBatch(t, "full WAL")
	if err := l.Append(payload, rows); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bog.checkpoint"), []byte("staged checkpoint"), 0600); err != nil {
		t.Fatal(err)
	}
	reopened := openCaptureTestLog(t, dir)
	checkCapture(t, mustCapture(t, reopened), append([]byte(magic), replayTestFrame(payload)...), 1)
}

type restoreEntryState struct {
	mode    os.FileMode
	modTime time.Time
	inode   uint64
	data    string
}

func restoreDirectoryState(t *testing.T, dir string) map[string]restoreEntryState {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	state := make(map[string]restoreEntryState, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, entry.Name())
		var data string
		if info.Mode()&os.ModeSymlink != 0 {
			data, err = os.Readlink(path)
		} else {
			var contents []byte
			contents, err = os.ReadFile(path)
			data = string(contents)
		}
		if err != nil {
			t.Fatal(err)
		}
		state[entry.Name()] = restoreEntryState{info.Mode(), info.ModTime(), info.Sys().(*syscall.Stat_t).Ino, data}
	}
	return state
}
