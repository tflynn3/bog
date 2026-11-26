package backup

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"bog/internal/store"
	"bog/internal/wal"
)

func TestBackupCanPublishUnderActiveWALDirectory(t *testing.T) {
	dir := t.TempDir()
	var live store.Store
	log, err := wal.Open(dir, &live)
	must(t, err)
	t.Cleanup(func() { must(t, log.Close()) })

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	f := newFixture(t)
	bundle := filepath.Join(dir, "bundle")
	must(t, Export(ctx, f, bundle))
	assertBackupPoint(t, bundle, f)
	target := filepath.Join(dir, "restored")
	must(t, Restore(ctx, bundle, target))
	assertBackupPoint(t, target, f)
}

func TestBackupRetryAfterSIGKILL(t *testing.T) {
	for _, mode := range []string{"export", "restore"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			bundle := filepath.Join(t.TempDir(), "bundle")
			must(t, Export(t.Context(), f, bundle))
			dir := t.TempDir()
			dest := filepath.Join(dir, "dest")
			must(t, os.Mkdir(dest, 0700))
			legacy := filepath.Join(dir, ".bog-backup-legacy")
			must(t, os.WriteFile(legacy, []byte("keep"), 0600))

			previous := ""
			for range 3 {
				kill := startInterruptedBackup(t, mode, bundle, dest)
				entries, err := os.ReadDir(dir)
				must(t, err)
				if len(entries) != 6 {
					t.Fatalf("expected one staged copy, its two markers and the backup guard beside destination and legacy entries, got %v", entries)
				}
				stage := ""
				for _, entry := range entries {
					if entry.IsDir() && strings.HasPrefix(entry.Name(), stagePrefix) {
						stage = filepath.Join(dir, entry.Name())
					}
				}
				if stage == "" || stage == previous {
					t.Fatalf("retry did not allocate a fresh staging identity: %q after %q", stage, previous)
				}
				if previous != "" {
					if _, err := os.Lstat(previous); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("previous stage survived: %v", err)
					}
				}
				backupChildren(t, dest)
				assertBackupPoint(t, stage, f)
				kill()
				previous = stage
			}
			const count = 6
			var workers sync.WaitGroup
			defer workers.Wait()
			results := make(chan error, count)
			for range count {
				workers.Go(func() {
					if mode == "restore" {
						results <- Restore(t.Context(), bundle, dest)
					} else {
						results <- Export(t.Context(), f, dest)
					}
				})
			}
			success := 0
			for range count {
				switch err := <-results; {
				case err == nil:
					success++
				case errors.Is(err, ErrPublicationUncertain):
					t.Fatalf("retry returned an uncertain outcome: %v", err)
				}
			}
			if success != 1 {
				t.Fatalf("%d of %d retries succeeded", success, count)
			}
			assertBackupPoint(t, dest, f)
			backupChildren(t, dir, ".bog-backup-legacy", stagingGuard, "dest")
		})
	}
}

func TestBackupRetryAfterRenamedPublisherDies(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	out := filepath.Join(dir, "out")
	startInterruptedBackup(t, "renamed", "", out)()
	assertBackupPoint(t, out, f)
	before := stagingTree(t, out)
	if err := Export(t.Context(), f, out); err == nil || errors.Is(err, ErrPublicationUncertain) {
		t.Fatalf("retry at a published destination returned %v", err)
	}
	backupChildren(t, dir, stagingGuard, "out")
	if after := stagingTree(t, out); !reflect.DeepEqual(before, after) {
		t.Fatal("retry changed the published backup")
	}
}

func TestLinuxBackupAtomicallyReplacesPlaceholder(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "out")
	must(t, os.Mkdir(out, 0700))
	old, err := os.Stat(out)
	must(t, err)
	events, err := syscall.InotifyInit1(syscall.IN_CLOEXEC | syscall.IN_NONBLOCK)
	must(t, err)
	defer func() { must(t, syscall.Close(events)) }()
	_, err = syscall.InotifyAddWatch(events, dir, syscall.IN_DELETE|syscall.IN_MOVED_FROM|syscall.IN_MOVED_TO)
	must(t, err)
	f := newFixture(t)
	synced := false
	ops := publicationOps{
		rename: renameStage,
		syncParent: func(parent *os.File) error {
			assertBackupPoint(t, out, f)
			synced = true
			return parent.Sync()
		},
	}
	must(t, exportBackup(t.Context(), f, out, func(ctx context.Context, dest string, fill func(*os.Root) error) error {
		return publishWithOps(ctx, dest, fill, ops)
	}))
	if !synced {
		t.Fatal("publication skipped the parent sync")
	}
	current, err := os.Stat(out)
	must(t, err)
	if os.SameFile(old, current) {
		t.Fatal("publication wrote files into the placeholder")
	}
	var raw [4096]byte
	n, err := syscall.Read(events, raw[:])
	must(t, err)
	moves := 0
	for offset := 0; offset < n; {
		if n-offset < syscall.SizeofInotifyEvent {
			t.Fatal("truncated inotify event")
		}
		header := raw[offset : offset+syscall.SizeofInotifyEvent]
		mask := binary.NativeEndian.Uint32(header[4:8])
		nameLen := int(binary.NativeEndian.Uint32(header[12:16]))
		offset += syscall.SizeofInotifyEvent
		if nameLen < 0 || nameLen > n-offset {
			t.Fatal("truncated inotify name")
		}
		name := string(bytes.TrimRight(raw[offset:offset+nameLen], "\x00"))
		offset += nameLen
		if name == "out" {
			if mask&syscall.IN_MOVED_TO == 0 {
				t.Fatalf("placeholder was removed separately, inotify mask %#x", mask)
			}
			moves++
		}
	}
	if moves != 1 {
		t.Fatalf("got %d publication renames, want 1", moves)
	}
	backupChildren(t, dir, stagingGuard, "out")
}

func TestLinuxBackupRenamePreservesLateNonemptyDestination(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "out")
	must(t, os.Mkdir(out, 0700))
	f := newFixture(t)
	ops := publicationOps{
		rename: func(root *os.Root, parent *os.File, stage, name string, exists bool) error {
			if !exists {
				t.Fatal("rename did not see the placeholder")
			}
			must(t, root.WriteFile(filepath.Join(name, "late"), []byte("keep"), 0600))
			return renameStage(root, parent, stage, name, exists)
		},
		syncParent: func(*os.File) error {
			t.Error("publication synced a failed rename")
			return nil
		},
	}
	err := exportBackup(t.Context(), f, out, func(ctx context.Context, dest string, fill func(*os.Root) error) error {
		return publishWithOps(ctx, dest, fill, ops)
	})
	if !(errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EEXIST)) {
		t.Fatalf("late destination collision returned %v", err)
	}
	data, err := os.ReadFile(filepath.Join(out, "late"))
	must(t, err)
	if string(data) != "keep" {
		t.Fatal("publication changed late destination contents")
	}
	backupChildren(t, out, "late")
	backupChildren(t, dir, stagingGuard, "out")
}
