package backup

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func backupChildren(t *testing.T, dir string, want ...string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	must(t, err)
	var got []string
	for _, entry := range entries {
		got = append(got, entry.Name())
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("children of %s = %v, want %v", dir, got, want)
	}
}

func testStageClaim(t *testing.T, dir, name string) stageClaim {
	t.Helper()
	root, err := os.OpenRoot(dir)
	must(t, err)
	t.Cleanup(func() { must(t, root.Close()) })
	file, err := root.Open(".")
	must(t, err)
	t.Cleanup(func() { must(t, file.Close()) })
	info, err := file.Stat()
	must(t, err)
	return newStageClaim(root, file, info, name)
}

func assertBackupPoint(t *testing.T, path string, f *fixture) {
	t.Helper()
	point, err := Verify(t.Context(), path)
	must(t, err)
	if point != f.point {
		t.Fatalf("verified point = %+v, want %+v", point, f.point)
	}
	reader, err := OpenRestored(t.Context(), path)
	must(t, err)
	defer func() { must(t, reader.Close()) }()
	checkWalk(t, reader, f.rows)
}

func TestStageRecoveryOwnership(t *testing.T) {
	const token = "00112233445566778899aabbccddeeff"
	tests := []struct {
		name  string
		setup func(*testing.T, stageClaim, string)
		ok    bool
	}{
		{"intent only", func(t *testing.T, c stageClaim, _ string) {
			must(t, c.parent.Symlink(token, c.key))
		}, true},
		{"empty bootstrap", func(t *testing.T, c stageClaim, stage string) {
			must(t, c.parent.Symlink(token, c.key))
			must(t, c.parent.Mkdir(stage, 0700))
		}, true},
		{"unproved populated directory", func(t *testing.T, c stageClaim, stage string) {
			must(t, c.parent.Symlink(token, c.key))
			must(t, c.parent.Mkdir(stage, 0700))
			must(t, c.parent.WriteFile(filepath.Join(stage, "foreign"), []byte("keep"), 0600))
		}, false},
		{"unqualified regular file", func(t *testing.T, c stageClaim, stage string) {
			must(t, c.parent.Symlink(token, c.key))
			must(t, c.parent.WriteFile(stage, []byte("keep"), 0600))
		}, false},
		{"unqualified symlink", func(t *testing.T, c stageClaim, stage string) {
			must(t, c.parent.Symlink(token, c.key))
			must(t, c.parent.Symlink("legacy", stage))
		}, false},
		{"malformed intent", func(t *testing.T, c stageClaim, _ string) {
			must(t, c.parent.Symlink("../legacy", c.key))
		}, false},
		{"regular file at intent", func(t *testing.T, c stageClaim, _ string) {
			must(t, c.parent.WriteFile(c.key, []byte(token), 0600))
		}, false},
		{"owner without intent", func(t *testing.T, c stageClaim, _ string) {
			must(t, c.parent.Symlink(token, c.key+".owner"))
		}, false},
		{"corrupt owner", func(t *testing.T, c stageClaim, stage string) {
			must(t, c.parent.Symlink(token, c.key))
			must(t, c.parent.Mkdir(stage, 0700))
			must(t, c.parent.Symlink(strings.Repeat("0", 40), c.key+".owner"))
		}, false},
		{"replaced directory", func(t *testing.T, c stageClaim, stage string) {
			must(t, c.parent.Symlink(token, c.key))
			must(t, c.parent.Mkdir(stage, 0700))
			info, err := c.parent.Lstat(stage)
			must(t, err)
			owner, err := c.identity(token, info)
			must(t, err)
			must(t, c.parent.Symlink(owner, c.key+".owner"))
			must(t, c.parent.Rename(stage, "moved"))
			must(t, c.parent.Mkdir(stage, 0700))
			must(t, c.parent.WriteFile(filepath.Join(stage, "foreign"), []byte("keep"), 0600))
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			c := testStageClaim(t, dir, "out")
			stage := c.stageName(token)
			must(t, c.parent.Mkdir(".bog-backup-old", 0700))
			must(t, c.parent.WriteFile(stagingGuard, nil, 0600))
			must(t, c.parent.WriteFile("legacy", []byte("keep"), 0600))
			tt.setup(t, c, stage)
			before := stagingTree(t, dir)
			f := newFixture(t)
			out := filepath.Join(dir, "out")
			err := Export(t.Context(), f, out)
			if tt.ok {
				must(t, err)
				assertBackupPoint(t, out, f)
				backupChildren(t, dir, ".bog-backup-old", stagingGuard, "legacy", "out")
				return
			}
			if err == nil || errors.Is(err, ErrPublicationUncertain) {
				t.Fatalf("invalid ownership returned %v", err)
			}
			if after := stagingTree(t, dir); !reflect.DeepEqual(before, after) {
				t.Fatalf("recovery changed unowned entries\nbefore %v\nafter %v", before, after)
			}
		})
	}
}

func stagingTree(t *testing.T, dir string) map[string]string {
	t.Helper()
	result := make(map[string]string)
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		switch {
		case entry.IsDir():
			result[path] = "directory"
		case entry.Type()&os.ModeSymlink != 0:
			link, err := os.Readlink(path)
			result[path] = "symlink:" + link
			return err
		default:
			b, err := os.ReadFile(path)
			result[path] = "file:" + string(b)
			return err
		}
		return nil
	})
	must(t, err)
	return result
}

func TestConcurrentBackupPublishers(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	folder := filepath.Join(dir, "parent")
	must(t, os.Mkdir(folder, 0700))
	alias := filepath.Join(dir, "alias")
	must(t, os.Symlink(folder, alias))
	bundle := filepath.Join(dir, "bundle")
	must(t, Export(t.Context(), f, bundle))

	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	s := wiredSnapshot{fixture: f, openWAL: f.OpenWAL, openSegment: func(name string) (io.ReadCloser, error) {
		if name == f.order[0] {
			close(reached)
			<-release
		}
		return f.OpenSegment(name)
	}}
	var workers sync.WaitGroup
	defer workers.Wait()
	defer unblock()
	out := filepath.Join(folder, "out")
	result := make(chan error, 1)
	workers.Go(func() { result <- Export(t.Context(), s, out) })
	select {
	case <-reached:
	case err := <-result:
		t.Fatalf("export exited before staging: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("export did not enter staging")
	}

	during := stagingTree(t, folder)
	guardPath := filepath.Join(folder, stagingGuard)
	guard, err := os.Stat(guardPath)
	must(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := Restore(ctx, bundle, filepath.Join(alias, "out")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("restore waiting on live export returned %v", err)
	}
	if after := stagingTree(t, folder); !reflect.DeepEqual(during, after) {
		t.Fatal("retry modified the live stage")
	}
	other := filepath.Join(folder, "other")
	must(t, Restore(t.Context(), bundle, other))
	assertBackupPoint(t, other, f)

	const contenders = 8
	results := make(chan error, contenders)
	for range contenders {
		workers.Go(func() { results <- Restore(t.Context(), bundle, filepath.Join(alias, "out")) })
	}
	unblock()
	must(t, <-result)
	for range contenders {
		if err := <-results; err == nil || errors.Is(err, ErrPublicationUncertain) {
			t.Fatalf("contender returned %v over a published export", err)
		}
	}
	assertBackupPoint(t, out, f)
	currentGuard, err := os.Stat(guardPath)
	must(t, err)
	if !os.SameFile(guard, currentGuard) {
		t.Fatal("publication replaced the backup guard inode")
	}
	backupChildren(t, folder, stagingGuard, "other", "out")
}

func TestBackupPublicationErrors(t *testing.T) {
	tests := []struct {
		name      string
		renameErr error
		syncErr   error
		move      bool
	}{
		{name: "rename refused", renameErr: syscall.EACCES},
		{name: "uncertain before rename", renameErr: syscall.EIO},
		{name: "uncertain after rename", renameErr: syscall.EIO, move: true},
		{name: "parent sync after rename", syncErr: syscall.EIO, move: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			dir := t.TempDir()
			out := filepath.Join(dir, "out")
			synced := false
			ops := publicationOps{
				rename: func(root *os.Root, file *os.File, stage, name string, exists bool) error {
					if tt.move {
						if err := renameStage(root, file, stage, name, exists); err != nil {
							return err
						}
					}
					return tt.renameErr
				},
				syncParent: func(*os.File) error {
					synced = true
					return tt.syncErr
				},
			}
			err := exportBackup(t.Context(), f, out, func(ctx context.Context, dest string, fill func(*os.Root) error) error {
				return publishWithOps(ctx, dest, fill, ops)
			})
			if synced != (tt.renameErr == nil) {
				t.Fatalf("publication sync called = %v after rename error %v", synced, tt.renameErr)
			}
			cause := errors.Join(tt.renameErr, tt.syncErr)
			want := tt.renameErr
			if want == nil {
				want = tt.syncErr
			}
			if !errors.Is(err, want) {
				t.Fatalf("publication returned %v, want %v", err, cause)
			}
			if errors.Is(err, ErrPublicationUncertain) != errors.Is(cause, syscall.EIO) {
				t.Fatalf("unexpected uncertainty classification: %v", err)
			}
			if tt.move {
				before := stagingTree(t, out)
				assertBackupPoint(t, out, f)
				if err := Export(t.Context(), f, out); err == nil {
					t.Fatal("retry overwrote a published destination")
				}
				if after := stagingTree(t, out); !reflect.DeepEqual(before, after) {
					t.Fatal("retry changed the installed image")
				}
			} else {
				if _, err := os.Lstat(out); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("unpublished destination: %v", err)
				}
				must(t, Export(t.Context(), f, out))
			}
			assertBackupPoint(t, out, f)
			backupChildren(t, dir, stagingGuard, "out")
		})
	}
}

func TestBackupRecoveryPreservesAbandonedStageForOtherDestination(t *testing.T) {
	f := newFixture(t)
	dir := t.TempDir()
	other := filepath.Join(dir, "other")
	ops := publicationOps{
		rename:     func(*os.Root, *os.File, string, string, bool) error { return syscall.EIO },
		syncParent: (*os.File).Sync,
	}
	err := exportBackup(t.Context(), f, other, func(ctx context.Context, dest string, fill func(*os.Root) error) error {
		return publishWithOps(ctx, dest, fill, ops)
	})
	if !errors.Is(err, ErrPublicationUncertain) {
		t.Fatalf("publication returned %v", err)
	}
	before := stagingTree(t, dir)
	if len(before) < 5 {
		t.Fatalf("missing abandoned stage: %v", before)
	}
	out := filepath.Join(dir, "out")
	must(t, Export(t.Context(), f, out))
	after := stagingTree(t, dir)
	for name, want := range before {
		if got, ok := after[name]; !ok || got != want {
			t.Fatalf("publishing to %s changed %s", out, name)
		}
	}
	assertBackupPoint(t, out, f)
	must(t, Restore(t.Context(), out, other))
	assertBackupPoint(t, other, f)
	backupChildren(t, dir, stagingGuard, "other", "out")
}
