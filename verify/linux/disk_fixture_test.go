package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
)

type fakeDiskMount struct {
	t         *testing.T
	path      string
	mounted   bool
	calls     []string
	onMount   func(context.Context) error
	onUnmount func(lazy bool) error
	onDevice  func() error
	freshErr  error
	healthErr error
}

func (m *fakeDiskMount) ops() diskMountOps {
	return diskMountOps{
		device: func(path string) (uint64, error) {
			m.t.Helper()
			if !filepath.IsAbs(path) {
				m.t.Errorf("relative device probe path %q", path)
			}
			if m.onDevice != nil {
				if err := m.onDevice(); err != nil {
					return 0, err
				}
			}
			if _, err := os.Stat(path); err != nil {
				return 0, err
			}
			if m.mounted {
				return 2, nil
			}
			return 1, nil
		},
		mount: func(ctx context.Context, path string) error {
			m.t.Helper()
			m.calls = append(m.calls, "mount")
			m.path = path
			if !filepath.IsAbs(path) {
				m.t.Errorf("relative mountpoint %q", path)
			}
			if _, ok := ctx.Deadline(); !ok {
				m.t.Error("mount has no deadline")
			}
			if m.onMount != nil {
				return m.onMount(ctx)
			}
			m.mounted = true
			return nil
		},
		unmount: func(ctx context.Context, path string, lazy bool) error {
			m.t.Helper()
			call := "unmount"
			if lazy {
				call = "detach"
			}
			m.calls = append(m.calls, call)
			if path != m.path || ctx.Err() != nil {
				m.t.Errorf("%s path = %q, context = %v", call, path, ctx.Err())
			}
			if _, ok := ctx.Deadline(); !ok {
				m.t.Errorf("%s has no deadline", call)
			}
			if m.onUnmount != nil {
				return m.onUnmount(lazy)
			}
			m.mounted = false
			return nil
		},
		verifyFresh: func(path string) error {
			if path != m.path || !m.mounted {
				m.t.Errorf("verified unexpected mount %q", path)
			}
			return m.freshErr
		},
		verifyHealthy: func(path string) error {
			if !filepath.IsAbs(path) {
				m.t.Errorf("relative healthy path %q", path)
			}
			return m.healthErr
		},
	}
}

func assertFixtureRemoved(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("fixture left entries in %s: %v", root, entries)
	}
}

func TestDiskFixtureWithRelativeTMPDIR(t *testing.T) {
	launch := filepath.Join(t.TempDir(), "launch")
	if err := os.MkdirAll(filepath.Join(launch, "scratch"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Chdir(launch)
	t.Setenv("TMPDIR", "scratch")
	root, err := newFixtureRoot()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Remove(root); err != nil {
			t.Error(err)
		}
	})
	// On macOS, Getwd may spell a symlinked component of TempDir
	// differently from the path returned by TempDir itself.
	if !filepath.IsAbs(root) || filepath.Base(filepath.Dir(root)) != "scratch" {
		t.Fatalf("fixture root = %q", root)
	}
	// Exercise the disk entry point with a relative root too.
	m := &fakeDiskMount{t: t}
	got, err := runDiskFixture(t.Context(), filepath.Join("scratch", filepath.Base(root)), m.ops(),
		func(ctx context.Context, f *diskFixture, proof *diskEvidence, dir diskDirection) error {
			origin, destination := f.healthy, f.full
			if dir == fullToHealthy {
				origin, destination = destination, origin
			}
			for i, target := range []string{origin, destination} {
				if i != 0 {
					if err := retargetDirectory(f.link, target); err != nil {
						return err
					}
				}
				got, err := os.Readlink(f.link)
				if err != nil {
					return err
				}
				if got != target || !filepath.IsAbs(got) {
					return fmt.Errorf("%s linked to %q, want absolute %q", dir, got, target)
				}
				// Model candidate startup with the launch symlink as both
				// the working directory and the absolute data-dir argument.
				cmd := exec.CommandContext(ctx, "/bin/sh", "-c", `test "$1" -ef . && test "$2" -ef .`, "fixture", f.link, target)
				cmd.Dir = f.link
				if out, err := cmd.CombinedOutput(); err != nil {
					return fmt.Errorf("resolve %s after chdir: %w: %s", f.link, err, out)
				}
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if !got.DisposableLocalFilesystem || !slices.Equal(m.calls, []string{"mount", "unmount"}) {
		t.Fatalf("filesystem proof = %+v, calls = %v", got, m.calls)
	}
	assertFixtureRemoved(t, root)
}

func TestDiskFixtureSetupAndCleanup(t *testing.T) {
	t.Parallel()
	setupErr := errors.New("setup error")
	unmountErr := errors.New("unmount error")
	statErr := errors.New("device probe error")
	for _, tc := range []struct {
		name      string
		configure func(*fakeDiskMount, context.CancelFunc)
		wantErrs  []error
		wantCalls []string
		retained  bool
	}{
		{
			name: "canceled before mount",
			configure: func(_ *fakeDiskMount, cancel context.CancelFunc) {
				cancel()
			},
			wantErrs: []error{context.Canceled},
		},
		{
			name: "baseline stat fails",
			configure: func(m *fakeDiskMount, _ context.CancelFunc) {
				m.onDevice = func() error { return statErr }
			},
			wantErrs: []error{statErr},
		},
		{
			name: "helper fails before mounting",
			configure: func(m *fakeDiskMount, _ context.CancelFunc) {
				m.onMount = func(context.Context) error { return setupErr }
			},
			wantErrs: []error{setupErr}, wantCalls: []string{"mount"},
		},
		{
			name: "helper fails after mounting and lazy detach succeeds",
			configure: func(m *fakeDiskMount, _ context.CancelFunc) {
				m.onMount = func(context.Context) error {
					m.mounted = true
					return setupErr
				}
				m.onUnmount = func(lazy bool) error {
					if !lazy {
						return unmountErr
					}
					m.mounted = false
					return nil
				}
			},
			wantErrs: []error{setupErr, unmountErr}, wantCalls: []string{"mount", "unmount", "detach"},
		},
		{
			name: "mount completes despite cancellation",
			configure: func(m *fakeDiskMount, cancel context.CancelFunc) {
				m.onMount = func(context.Context) error {
					m.mounted = true
					cancel()
					return nil
				}
			},
			wantErrs: []error{context.Canceled}, wantCalls: []string{"mount", "unmount"},
		},
		{
			name: "invalid fresh filesystem",
			configure: func(m *fakeDiskMount, _ context.CancelFunc) {
				m.freshErr = setupErr
			},
			wantErrs: []error{setupErr}, wantCalls: []string{"mount", "unmount"},
		},
		{
			name: "healthy filesystem check fails after data directories exist",
			configure: func(m *fakeDiskMount, _ context.CancelFunc) {
				m.healthErr = setupErr
			},
			wantErrs: []error{setupErr}, wantCalls: []string{"mount", "unmount"},
		},
		{
			name: "mounted path cannot be inspected",
			configure: func(m *fakeDiskMount, _ context.CancelFunc) {
				m.onDevice = func() error {
					if m.mounted {
						return statErr
					}
					return nil
				}
			},
			wantErrs: []error{statErr}, wantCalls: []string{"mount", "unmount"},
		},
		{
			name: "unmount cannot release mountpoint",
			configure: func(m *fakeDiskMount, _ context.CancelFunc) {
				m.freshErr = setupErr
				m.onUnmount = func(lazy bool) error {
					if lazy {
						return unmountErr
					}
					return nil // cleanup must still verify removal
				}
			},
			wantErrs: []error{setupErr, unmountErr}, wantCalls: []string{"mount", "unmount", "detach"}, retained: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			m := &fakeDiskMount{t: t}
			tc.configure(m, cancel)
			proof, err := runDiskFixture(ctx, root, m.ops(),
				func(context.Context, *diskFixture, *diskEvidence, diskDirection) error {
					t.Fatal("probe ran despite setup failure")
					return nil
				})
			for _, want := range tc.wantErrs {
				if !errors.Is(err, want) {
					t.Errorf("missing %v in %v", want, err)
				}
			}
			if proof.HealthyToFull != nil || proof.FullToHealthy != nil {
				t.Errorf("unexpected retarget proof: %+v", proof)
			}
			if !slices.Equal(m.calls, tc.wantCalls) {
				t.Errorf("calls = %v, want %v", m.calls, tc.wantCalls)
			}
			if tc.retained {
				if _, err := os.Stat(m.path); err != nil || !m.mounted {
					t.Errorf("mountpoint removed or detached despite failed cleanup: %v", err)
				}
			} else {
				assertFixtureRemoved(t, root)
			}
		})
	}
}

func TestDiskFixtureRecordsBothDirectionsAndCleanup(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	firstErr := errors.New("first readiness failed")
	secondErr := errors.New("second ingest failed")
	cleanupErr := errors.New("unmount helper failed")
	m := &fakeDiskMount{t: t}
	m.onUnmount = func(bool) error {
		if _, err := os.Stat(filepath.Join(m.path, "filler")); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("unmount before filler cleanup: %v", err)
		}
		m.mounted = false
		return cleanupErr
	}
	var calls []diskDirection
	proof, err := runDiskFixture(t.Context(), root, m.ops(),
		func(_ context.Context, f *diskFixture, evidence *diskEvidence, dir diskDirection) error {
			calls = append(calls, dir)
			// Exclusive creation checks that the first direction's filler
			// is removed even when it returns a probe error.
			file, err := os.OpenFile(f.filler, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				return err
			}
			if err := file.Close(); err != nil {
				return err
			}
			switch dir {
			case healthyToFull:
				evidence.HealthyToFull.ReadinessStatus = 200
				return firstErr
			case fullToHealthy:
				evidence.FullToHealthy.ReadinessStatus = 503
				return secondErr
			default:
				t.Fatalf("unexpected direction %q", dir)
				return nil
			}
		})
	for _, want := range []error{firstErr, secondErr, cleanupErr} {
		if !errors.Is(err, want) {
			t.Errorf("missing %v in %v", want, err)
		}
	}
	if !slices.Equal(calls, []diskDirection{healthyToFull, fullToHealthy}) {
		t.Errorf("directions = %v", calls)
	}
	assertFixtureRemoved(t, root)

	binary := filepath.Join(root, "candidate")
	if err := os.WriteFile(binary, []byte("test candidate"), 0700); err != nil {
		t.Fatal(err)
	}
	r, reportErr := newReport(binary, testRevision, fixtureID)
	if reportErr != nil {
		t.Fatal(reportErr)
	}
	if reportErr := r.record("enospc", proof, err); !errors.Is(reportErr, cleanupErr) {
		t.Fatalf("record did not propagate cleanup failure: %v", reportErr)
	}
	path := filepath.Join(root, "durability.json")
	if err := r.write(path); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Cases map[string]struct {
			Status   string       `json:"status"`
			Detail   string       `json:"detail"`
			Evidence diskEvidence `json:"evidence"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	disk := got.Cases["enospc"]
	if disk.Status != "fail" || disk.Evidence.HealthyToFull == nil || disk.Evidence.FullToHealthy == nil ||
		disk.Evidence.HealthyToFull.ReadinessStatus != 200 || disk.Evidence.FullToHealthy.ReadinessStatus != 503 {
		t.Fatalf("missing per-direction evidence: %s", raw)
	}
	for _, message := range []string{"healthy_to_full: " + firstErr.Error(), "full_to_healthy: " + secondErr.Error(), cleanupErr.Error()} {
		if !strings.Contains(disk.Detail, message) {
			t.Errorf("missing %q in %q", message, disk.Detail)
		}
	}
}

func TestDiskFixtureDirectionDeadlineAndCancellation(t *testing.T) {
	t.Parallel()
	t.Run("first direction times out", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		synctest.Test(t, func(t *testing.T) {
			m := &fakeDiskMount{t: t}
			var calls []diskDirection
			_, err := runDiskFixture(t.Context(), root, m.ops(),
				func(ctx context.Context, _ *diskFixture, _ *diskEvidence, dir diskDirection) error {
					calls = append(calls, dir)
					if dir == healthyToFull {
						<-ctx.Done()
					}
					return ctx.Err()
				})
			if !errors.Is(err, context.DeadlineExceeded) || !slices.Equal(calls, []diskDirection{healthyToFull, fullToHealthy}) {
				t.Fatalf("directions = %v, error = %v", calls, err)
			}
			assertFixtureRemoved(t, root)
		})
	})
	t.Run("run canceled with filler in place", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		m := &fakeDiskMount{t: t}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		_, err := runDiskFixture(ctx, root, m.ops(),
			func(_ context.Context, f *diskFixture, _ *diskEvidence, dir diskDirection) error {
				if dir != healthyToFull {
					t.Fatalf("started %s after cancellation", dir)
				}
				if err := os.WriteFile(f.filler, nil, 0600); err != nil {
					return err
				}
				cancel()
				return nil
			})
		if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), string(fullToHealthy)) {
			t.Fatalf("missing canceled direction: %v", err)
		}
		assertFixtureRemoved(t, root)
	})
}
