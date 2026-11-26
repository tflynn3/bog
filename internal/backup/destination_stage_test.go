package backup

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

func destinationStageTestNames(destination string) (stage, lock string) {
	parent, name := filepath.Split(destination)
	key := sha256.Sum256([]byte(name))
	return filepath.Join(parent, fmt.Sprintf("%s%x", destinationStagePrefix, key)),
		filepath.Join(parent, fmt.Sprintf("%s%x", destinationLockPrefix, key))
}

func destinationStageTestFill(root *os.Root) error {
	return writeBytes(root, metadataName, []byte("image"))
}

func requireDestinationStageContents(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	must(t, err)
	if string(got) != want {
		t.Fatalf("%s contains %q, want %q", path, got, want)
	}
}

func requireNoDestinationStage(t *testing.T, target string) {
	t.Helper()
	stage, lock := destinationStageTestNames(target)
	if _, err := os.Lstat(stage); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stage remains at %s: %v", stage, err)
	}
	info, err := os.Stat(lock)
	must(t, err)
	if info.Size() != 0 {
		t.Fatalf("lock still contains %d bytes of staging state", info.Size())
	}
}

func TestDestinationStageSerializesByPinnedDestination(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	parent := t.TempDir()
	alias := filepath.Join(t.TempDir(), "parent-alias")
	must(t, os.Symlink(parent, alias))
	first := filepath.Join(parent, "first")
	second := filepath.Join(parent, "second")

	entered := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		finished <- publishDestinationStage(ctx, first, func(root *os.Root) error {
			if err := destinationStageTestFill(root); err != nil {
				return err
			}
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-entered:
	case err := <-finished:
		t.Fatalf("first publisher returned early: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	sameDestination := filepath.Join(alias, "first")
	cancelled, stopWaiting := context.WithTimeout(ctx, 50*time.Millisecond)
	defer stopWaiting()
	unexpectedFill := errors.New("a second same-destination publisher entered fill")
	rejectFill := func(*os.Root) error { return unexpectedFill }
	if err := publishDestinationStage(cancelled, sameDestination, rejectFill); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting publisher returned %v", err)
	}

	retry := make(chan error, 1)
	go func() { retry <- publishDestinationStage(ctx, sameDestination, rejectFill) }()
	must(t, publishDestinationStage(ctx, second, destinationStageTestFill))
	select {
	case err := <-retry:
		t.Fatalf("same-destination retry returned while the first publisher was live: %v", err)
	default:
	}
	firstStage, _ := destinationStageTestNames(first)
	requireDestinationStageContents(t, filepath.Join(firstStage, metadataName), "image")
	close(release)
	must(t, <-finished)
	if err := <-retry; err == nil || errors.Is(err, unexpectedFill) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("same-destination retry did not reject a completed destination: %v", err)
	}
	for _, target := range []string{first, second} {
		requireDestinationStageContents(t, filepath.Join(target, metadataName), "image")
		requireNoDestinationStage(t, target)
	}
}

func TestDestinationStagePinsParentThroughPublication(t *testing.T) {
	base := t.TempDir()
	parent := filepath.Join(base, "parent")
	moved := filepath.Join(base, "pinned")
	must(t, os.Mkdir(parent, 0700))
	must(t, publishDestinationStage(t.Context(), filepath.Join(parent, "image"), func(stage *os.Root) error {
		if err := os.Rename(parent, moved); err != nil {
			return err
		}
		if err := os.Mkdir(parent, 0700); err != nil {
			return err
		}
		return destinationStageTestFill(stage)
	}))
	requireDestinationStageContents(t, filepath.Join(moved, "image", metadataName), "image")
	requireNoDestinationStage(t, filepath.Join(moved, "image"))
	entries, err := os.ReadDir(parent)
	must(t, err)
	if len(entries) != 0 {
		t.Fatalf("publication used the substituted parent: %v", entries)
	}
}

func TestDestinationStagePublicationFailures(t *testing.T) {
	failure := errors.New("injected publication failure")
	for _, tc := range []struct {
		name         string
		cause        error
		order        []string
		uncertain    bool
		published    bool
		stageRemains bool
	}{
		{"fill", failure, []string{"fill"}, false, false, false},
		{"cancel", context.Canceled, []string{"fill", "stage sync"}, false, false, false},
		{"stage sync", failure, []string{"fill", "stage sync"}, false, false, false},
		{"rename", failure, []string{"fill", "stage sync", "rename"}, false, false, false},
		{"rename EIO before install", syscall.EIO, []string{"fill", "stage sync", "rename"}, true, false, true},
		{"rename EIO after install", syscall.EIO, []string{"fill", "stage sync", "rename"}, true, true, false},
		{"parent sync", failure, []string{"fill", "stage sync", "rename", "parent sync"}, true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := t.TempDir()
			target := filepath.Join(parent, "image")
			stage, _ := destinationStageTestNames(target)
			must(t, os.Mkdir(target, 0700))
			before, err := os.Stat(target)
			must(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var order []string
			ops := destinationStageOps{
				sync: func(root *os.Root) error {
					phase := "stage sync"
					if root.Name() == parent {
						phase = "parent sync"
					}
					order = append(order, phase)
					if tc.name == phase {
						return failure
					}
					return syncDirectory(root)
				},
				rename: func(root *os.Root, from, to string) error {
					order = append(order, "rename")
					switch tc.name {
					case "rename":
						return failure
					case "rename EIO before install":
						return syscall.EIO
					case "rename EIO after install":
						if err := root.Rename(from, to); err != nil {
							return err
						}
						return syscall.EIO
					}
					return root.Rename(from, to)
				},
			}
			err = publishDestinationStageWith(ctx, target, func(root *os.Root) error {
				order = append(order, "fill")
				if err := destinationStageTestFill(root); err != nil {
					return err
				}
				switch tc.name {
				case "fill":
					return failure
				case "cancel":
					cancel()
				}
				return nil
			}, ops)
			if !errors.Is(err, tc.cause) || errors.Is(err, ErrPublicationUncertain) != tc.uncertain {
				t.Fatalf("publication error = %v, want %v with uncertainty %v", err, tc.cause, tc.uncertain)
			}
			if !reflect.DeepEqual(order, tc.order) {
				t.Fatalf("publication order = %v, want %v", order, tc.order)
			}
			if !strings.Contains(strings.Join(tc.order, ","), "rename") {
				after, err := os.Stat(target)
				must(t, err)
				if !os.SameFile(before, after) {
					t.Fatal("pre-publication failure replaced the empty placeholder")
				}
			}
			if tc.stageRemains {
				requireDestinationStageContents(t, filepath.Join(stage, metadataName), "image")
			} else if _, err := os.Lstat(stage); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unexpected staging directory: %v", err)
			}
			if tc.published {
				requireDestinationStageContents(t, filepath.Join(target, metadataName), "image")
				called := false
				err := publishDestinationStage(t.Context(), target, func(*os.Root) error {
					called = true
					return nil
				})
				if err == nil || called {
					t.Fatalf("retry entered fill for an installed image: %v", err)
				}
				return
			}
			retry, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			must(t, publishDestinationStage(retry, target, destinationStageTestFill))
			requireNoDestinationStage(t, target)
		})
	}
}

func TestDestinationStagePreservesForeignEntries(t *testing.T) {
	for _, kind := range []string{
		"unowned directory", "unowned empty directory", "stage symlink", "lock symlink",
		"lock hardlink", "foreign lock contents", "lock fifo", "replaced stage",
		"replaced lock", "unexpected staged file", "staged symlink", "staged hardlink",
		"late destination",
	} {
		t.Run(kind, func(t *testing.T) {
			parent := t.TempDir()
			target := filepath.Join(parent, "image")
			stage, lock := destinationStageTestNames(target)
			unrelated := filepath.Join(parent, ".bog-backup-legacy")
			must(t, os.WriteFile(unrelated, []byte("keep"), 0600))
			evidence := ""
			fill := destinationStageTestFill
			switch kind {
			case "unowned directory":
				must(t, os.Mkdir(stage, 0700))
				evidence = filepath.Join(stage, metadataName)
				must(t, os.WriteFile(evidence, []byte("keep"), 0600))
			case "unowned empty directory":
				must(t, os.Mkdir(stage, 0700))
				evidence = stage
			case "stage symlink":
				must(t, os.Symlink(unrelated, stage))
				evidence = stage
			case "lock symlink":
				must(t, os.Symlink(unrelated, lock))
				evidence = lock
			case "lock hardlink":
				must(t, os.Link(unrelated, lock))
				evidence = lock
			case "foreign lock contents":
				must(t, os.WriteFile(lock, []byte("keep"), 0600))
				evidence = lock
			case "lock fifo":
				must(t, syscall.Mkfifo(lock, 0600))
				evidence = lock
			default:
				fill = func(root *os.Root) error {
					if err := destinationStageTestFill(root); err != nil {
						return err
					}
					switch kind {
					case "replaced stage":
						evidence = stage
						if err := os.Rename(stage, stage+"-moved"); err != nil {
							return err
						}
						return os.Mkdir(stage, 0700)
					case "replaced lock":
						evidence = lock
						if err := os.Rename(lock, lock+"-moved"); err != nil {
							return err
						}
						return os.WriteFile(lock, []byte("keep"), 0600)
					case "unexpected staged file":
						evidence = filepath.Join(stage, "unrelated")
						return errors.Join(writeBytes(root, "unrelated", []byte("keep")), errors.New("abort fill"))
					case "staged symlink":
						evidence = filepath.Join(stage, tailName)
						return errors.Join(os.Symlink(unrelated, evidence), errors.New("abort fill"))
					case "staged hardlink":
						evidence = filepath.Join(stage, tailName)
						return errors.Join(os.Link(unrelated, evidence), errors.New("abort fill"))
					case "late destination":
						evidence = filepath.Join(target, "unrelated")
						if err := os.Mkdir(target, 0700); err != nil {
							return err
						}
						return os.WriteFile(evidence, []byte("keep"), 0600)
					default:
						return errors.New("unknown scenario")
					}
				}
			}
			if err := publishDestinationStage(t.Context(), target, fill); err == nil {
				t.Fatal("publication accepted foreign or changed state")
			}
			requireDestinationStageContents(t, unrelated, "keep")
			info, err := os.Lstat(evidence)
			must(t, err)
			if info.Mode().IsRegular() {
				requireDestinationStageContents(t, evidence, "keep")
			}
			switch kind {
			case "replaced stage":
				requireDestinationStageContents(t, filepath.Join(stage+"-moved", metadataName), "image")
			case "replaced lock", "unexpected staged file", "staged symlink", "staged hardlink":
				requireDestinationStageContents(t, filepath.Join(stage, metadataName), "image")
			case "late destination":
				requireNoDestinationStage(t, target)
			}
		})
	}
}

func TestDestinationStageRecoversOnlyAuthorizedCrashStates(t *testing.T) {
	for _, tc := range []struct {
		name        string
		hasStage    bool
		hasFile     bool
		bindingSize int
		damage      bool
		completed   bool
		retry       bool
	}{
		{name: "interrupted intent write", bindingSize: -5, retry: true},
		{name: "durable intent before mkdir", retry: true},
		{name: "empty stage before binding", hasStage: true, retry: true},
		{name: "nonempty stage before binding", hasStage: true, hasFile: true},
		{name: "interrupted binding write", hasStage: true, bindingSize: 17, retry: true},
		{name: "bound stage", hasStage: true, hasFile: true, bindingSize: destinationBindSize, retry: true},
		{name: "damaged binding", hasStage: true, hasFile: true, bindingSize: destinationBindSize, damage: true},
		{name: "completed destination with stage", hasStage: true, hasFile: true, bindingSize: destinationBindSize, completed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			name := "image"
			target := filepath.Join(dir, name)
			stagePath, lockPath := destinationStageTestNames(target)
			func() {
				parent, err := os.OpenRoot(dir)
				must(t, err)
				defer func() { must(t, parent.Close()) }()
				p := destinationPublication{
					parent: parent, name: name,
					stageName: filepath.Base(stagePath),
					lockName:  filepath.Base(lockPath),
					key:       sha256.Sum256([]byte(name)),
				}
				must(t, p.acquire(t.Context()))
				defer func() { must(t, p.lock.Close()) }()

				intent := p.intent()
				must(t, p.writeRecord(intent[:len(intent)+min(0, tc.bindingSize)], 0))
				if !tc.hasStage {
					return
				}
				must(t, parent.Mkdir(p.stageName, 0700))
				info, err := parent.Lstat(p.stageName)
				must(t, err)
				if tc.bindingSize > 0 {
					binding := p.binding(info)
					if tc.damage {
						binding[len(binding)-1] ^= 1
					}
					must(t, p.writeRecord(binding[:tc.bindingSize], destinationIntentSize))
				}
				if tc.hasFile {
					must(t, os.WriteFile(filepath.Join(stagePath, metadataName), []byte("old stage"), 0600))
				}
				if tc.completed {
					must(t, os.Mkdir(target, 0700))
					must(t, os.WriteFile(filepath.Join(target, metadataName), []byte("completed"), 0600))
				}
			}()

			err := publishDestinationStage(t.Context(), target, destinationStageTestFill)
			if tc.retry {
				must(t, err)
				requireNoDestinationStage(t, target)
				requireDestinationStageContents(t, filepath.Join(target, metadataName), "image")
				return
			}
			if err == nil {
				t.Fatal("retry succeeded for an ambiguous stage or a completed destination")
			}
			requireDestinationStageContents(t, filepath.Join(stagePath, metadataName), "old stage")
			if tc.completed {
				requireDestinationStageContents(t, filepath.Join(target, metadataName), "completed")
			}
		})
	}
}
