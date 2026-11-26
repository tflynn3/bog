package backup

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const stageChildEnv = "BOG_TEST_STAGING_CHILD"

func TestBackupStagingChild(t *testing.T) {
	mode := os.Getenv(stageChildEnv)
	if mode == "" {
		return
	}
	if os.Getenv("BOG_TEST_STAGING_RESTRICTED") == "1" {
		defer syscall.Umask(syscall.Umask(0777))
	}
	park := func() error {
		if _, err := fmt.Fprintln(os.Stdout, "backup stage ready"); err != nil {
			return err
		}
		var b [1]byte
		_, err := io.ReadFull(os.Stdin, b[:])
		return fmt.Errorf("interruption helper was released: %v", err)
	}
	stopPublisher := func(ctx context.Context, destination string, fill func(*os.Root) error) error {
		if mode == "renamed" {
			return publishWithOps(ctx, destination, fill, publicationOps{
				rename:     renameStage,
				syncParent: func(*os.File) error { return park() },
			})
		}
		return publish(ctx, destination, func(root *os.Root) error {
			if err := fill(root); err != nil {
				return err
			}
			if err := syncDirectory(root); err != nil {
				return err
			}
			return park()
		})
	}
	dest := os.Getenv("BOG_TEST_STAGING_DEST")
	switch mode {
	case "guard":
		// Reproduce a publisher interrupted immediately after the exclusive
		// create, before it could use its writable descriptor to set mode.
		file, err := os.OpenFile(filepath.Join(dest, stagingGuard), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		must(t, err)
		defer func() { must(t, file.Close()) }()
		must(t, park())
	case "restore":
		must(t, restoreBackup(t.Context(), os.Getenv("BOG_TEST_STAGING_BUNDLE"), dest, stopPublisher))
	case "export", "renamed":
		must(t, exportBackup(t.Context(), newFixture(t), dest, stopPublisher))
	default:
		t.Fatalf("unknown helper mode %q", mode)
	}
	t.Fatal("backup helper exited without interruption")
}

func startInterruptedBackup(t *testing.T, mode, bundle, dest string, env ...string) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBackupStagingChild$")
	cmd.Env = append(cmd.Environ(), stageChildEnv+"="+mode,
		"BOG_TEST_STAGING_DEST="+dest, "BOG_TEST_STAGING_BUNDLE="+bundle)
	cmd.Env = append(cmd.Env, env...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	must(t, err)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		must(t, errors.Join(err, stdout.Close()))
	}
	if err := cmd.Start(); err != nil {
		must(t, errors.Join(err, stdin.Close(), stdout.Close()))
	}
	var once sync.Once
	kill := func() {
		t.Helper()
		once.Do(func() {
			killErr := cmd.Process.Kill()
			if errors.Is(killErr, os.ErrProcessDone) {
				killErr = nil
			}
			waitErr := cmd.Wait()
			must(t, killErr)
			var exit *exec.ExitError
			if !errors.As(waitErr, &exit) || exit.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
				t.Fatalf("interruption helper exited with %v\n%s", waitErr, stderr.String())
			}
		})
	}
	t.Cleanup(kill)
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || line != "backup stage ready\n" {
		t.Fatalf("staging barrier returned %q, %v", line, err)
	}
	return kill
}

func TestStagingGuardRejectsUnsafeEntries(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T, string)
	}{
		{"symlink", func(t *testing.T, path string) { must(t, os.Symlink("foreign", path)) }},
		{"hard link", func(t *testing.T, path string) {
			must(t, os.Link(filepath.Join(filepath.Dir(path), "foreign"), path))
		}},
		{"directory", func(t *testing.T, path string) { must(t, os.Mkdir(path, 0)) }},
		{"fifo", func(t *testing.T, path string) { must(t, syscall.Mkfifo(path, 0600)) }},
		{"public mode", func(t *testing.T, path string) {
			must(t, os.WriteFile(path, nil, 0600))
			must(t, os.Chmod(path, 0640))
		}},
		{"unexpected owner bits", func(t *testing.T, path string) {
			must(t, os.WriteFile(path, nil, 0600))
			must(t, os.Chmod(path, 0100))
		}},
		{"populated mode zero", func(t *testing.T, path string) {
			must(t, os.WriteFile(path, []byte("keep"), 0600))
			must(t, os.Chmod(path, 0))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			foreign := filepath.Join(dir, "foreign")
			must(t, os.WriteFile(foreign, []byte("keep"), 0600))
			path := filepath.Join(dir, stagingGuard)
			tc.setup(t, path)
			before, err := os.Lstat(path)
			must(t, err)
			err = publish(t.Context(), filepath.Join(dir, "out"), func(*os.Root) error {
				t.Error("unsafe guard admitted a publisher")
				return nil
			})
			if err == nil || !strings.Contains(err.Error(), "unsafe backup staging guard") {
				t.Fatalf("unsafe guard returned %v", err)
			}
			after, err := os.Lstat(path)
			must(t, err)
			if !os.SameFile(before, after) || before.Mode() != after.Mode() || before.Size() != after.Size() {
				t.Fatalf("guard changed from %v to %v", before, after)
			}
			data, err := os.ReadFile(foreign)
			must(t, err)
			if string(data) != "keep" {
				t.Fatal("publisher changed the unrelated file")
			}
			backupChildren(t, dir, stagingGuard, "foreign")
		})
	}
}

func TestStagingGuardRepairUsesPinnedParent(t *testing.T) {
	base := t.TempDir()
	path := filepath.Join(base, "parent")
	moved := filepath.Join(base, "moved")
	must(t, os.Mkdir(path, 0700))
	claim := testStageClaim(t, path, "out")
	must(t, claim.parent.WriteFile(stagingGuard, nil, 0))
	before, err := claim.parent.Lstat(stagingGuard)
	must(t, err)
	must(t, os.Rename(path, moved))
	must(t, os.Mkdir(path, 0700))
	replacement := filepath.Join(path, stagingGuard)
	must(t, os.WriteFile(replacement, nil, 0))

	g, err := openStagingGuard(t.Context(), claim.parent, claim.dir, claim.parentInfo)
	must(t, err)
	defer func() { must(t, g.file.Close()) }()
	assertGuardInode(t, moved, before)
	other, err := os.Lstat(replacement)
	must(t, err)
	if os.SameFile(before, other) || other.Mode().Perm() != 0 {
		t.Fatal("repair touched the replacement parent")
	}
}

func TestStagingGuardRejectsReplacementBeforeRepair(t *testing.T) {
	claim := testStageClaim(t, t.TempDir(), "out")
	must(t, claim.parent.WriteFile(stagingGuard, nil, 0))
	before, err := claim.parent.Lstat(stagingGuard)
	must(t, err)
	must(t, claim.parent.Rename(stagingGuard, "old"))
	must(t, claim.parent.WriteFile(stagingGuard, nil, 0))
	if err := repairStagingGuardMode(claim.parent, claim.dir, claim.parentInfo, before); err == nil {
		t.Fatal("repair accepted a replacement inode")
	}
	for _, name := range []string{stagingGuard, "old"} {
		info, err := claim.parent.Lstat(name)
		must(t, err)
		if info.Mode().Perm() != 0 {
			t.Fatalf("repair changed %s to %v", name, info.Mode())
		}
	}
}

func TestStagingGuardRefusesPublicationAfterReplacement(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "out")
	err := publish(t.Context(), dest, func(stage *os.Root) error {
		must(t, stage.WriteFile("data", []byte("keep staged"), 0600))
		must(t, os.Rename(filepath.Join(dir, stagingGuard), filepath.Join(dir, "moved")))
		return os.WriteFile(filepath.Join(dir, stagingGuard), nil, 0600)
	})
	if err == nil || !strings.Contains(err.Error(), "staging guard changed") {
		t.Fatalf("publish with replaced guard returned %v", err)
	}
	if _, err := os.Lstat(dest); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale guard published a destination: %v", err)
	}
	f := newFixture(t)
	must(t, Export(t.Context(), f, dest))
	assertBackupPoint(t, dest, f)
	backupChildren(t, dir, stagingGuard, "moved", "out")
}

func assertGuardInode(t *testing.T, dir string, expected os.FileInfo) os.FileInfo {
	t.Helper()
	got, err := os.Lstat(filepath.Join(dir, stagingGuard))
	must(t, err)
	if !got.Mode().IsRegular() || got.Mode().Perm() != 0600 || (expected != nil && !os.SameFile(expected, got)) {
		t.Fatalf("unexpected guard %v, expected regular mode 0600 on inode %v", got, expected)
	}
	return got
}

const nonRootGuardEnv = "BOG_TEST_NONROOT_GUARD"

func TestStagingGuardNonRoot(t *testing.T) {
	if os.Getenv(nonRootGuardEnv) == "1" {
		if os.Geteuid() == 0 {
			t.Fatal("guard regressions must run without root permissions")
		}
		t.Run("export", func(t *testing.T) { testRestrictedPublish(t, "export") })
		t.Run("restore", func(t *testing.T) { testRestrictedPublish(t, "restore") })
		t.Run("fresh concurrent opens", testConcurrentGuardBootstrap)
		t.Run("repair respects live lock", testRestrictedGuardCancellation)
		return
	}

	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	binary, err := os.Executable()
	must(t, err)
	var attr *syscall.SysProcAttr
	var childDir string
	if os.Geteuid() == 0 {
		const nobody = 65534
		childDir, err = os.MkdirTemp("", "backup-guard-test-")
		must(t, err)
		t.Cleanup(func() { must(t, os.RemoveAll(childDir)) })
		must(t, os.Chown(childDir, nobody, nobody))
		childBinary := filepath.Join(childDir, "backup.test")
		must(t, copyGuardTestBinary(binary, childBinary))
		binary = childBinary
		attr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: nobody, Gid: nobody}}
	}
	cmd := exec.CommandContext(ctx, binary, "-test.run=^TestStagingGuardNonRoot$")
	cmd.SysProcAttr = attr
	cmd.WaitDelay = time.Second
	cmd.Dir = childDir
	cmd.Env = append(cmd.Environ(), nonRootGuardEnv+"=1")
	if childDir != "" {
		cmd.Env = append(cmd.Env, "TMPDIR="+childDir)
	}
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("non-root staging guard subprocess: %v\n%s", err, output)
	}
}

func copyGuardTestBinary(source, target string) (err error) {
	src, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, src.Close()) }()
	dst, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0700)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, dst.Close()) }()
	if _, err := io.Copy(dst, src); err != nil {
		return err
	}
	return dst.Chmod(0755)
}

func testRestrictedPublish(t *testing.T, mode string) {
	f := newFixture(t)
	bundle := filepath.Join(t.TempDir(), "bundle")
	must(t, Export(t.Context(), f, bundle))
	for _, legacy := range []bool{false, true} {
		label := "fresh"
		if legacy {
			label = "interrupted guard creation"
		}
		t.Run(label, func(t *testing.T) {
			dir := t.TempDir()
			dest := filepath.Join(dir, "dest")
			must(t, os.Mkdir(dest, 0700))
			var guard os.FileInfo
			if legacy {
				startInterruptedBackup(t, "guard", "", dir, "BOG_TEST_STAGING_RESTRICTED=1")()
				var err error
				guard, err = os.Lstat(filepath.Join(dir, stagingGuard))
				must(t, err)
				if guard.Mode().Perm() != 0 {
					t.Fatalf("interrupted guard has mode %v", guard.Mode())
				}
			}
			// First publisher (or first repairer) is killed after staging a
			// complete image. The next process must reclaim that stage.
			kill := startInterruptedBackup(t, mode, bundle, dest, "BOG_TEST_STAGING_RESTRICTED=1")
			guard = assertGuardInode(t, dir, guard)
			entries, err := os.ReadDir(dir)
			must(t, err)
			if len(entries) != 5 {
				t.Fatalf("expected guard, destination, stage and two claims, got %v", entries)
			}
			backupChildren(t, dest)
			kill()
			doPublish := func(path string) error {
				if mode == "restore" {
					return Restore(t.Context(), bundle, path)
				}
				return Export(t.Context(), f, path)
			}
			func() {
				defer syscall.Umask(syscall.Umask(0777))
				must(t, doPublish(dest))
				assertBackupPoint(t, dest, f)
				if err := doPublish(dest); err == nil || errors.Is(err, ErrPublicationUncertain) {
					t.Fatalf("retry under restrictive umask returned %v", err)
				}
			}()
			before := stagingTree(t, dest)
			if err := doPublish(dest); err == nil || errors.Is(err, ErrPublicationUncertain) {
				t.Fatalf("retry after restrictive umask returned %v", err)
			}
			if after := stagingTree(t, dest); !reflect.DeepEqual(before, after) {
				t.Fatal("retry changed the published image")
			}
			other := filepath.Join(dir, "other")
			must(t, doPublish(other))
			assertBackupPoint(t, other, f)
			assertGuardInode(t, dir, guard)
			backupChildren(t, dir, stagingGuard, "dest", "other")
		})
	}
}

func testConcurrentGuardBootstrap(t *testing.T) {
	dir := t.TempDir()
	claim := testStageClaim(t, dir, "out")
	defer syscall.Umask(syscall.Umask(0777))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	start := make(chan struct{})
	type result struct {
		info os.FileInfo
		err  error
	}
	const count = 16
	results := make(chan result, count)
	var workers sync.WaitGroup
	for range count {
		workers.Go(func() {
			<-start
			guard, err := openStagingGuard(ctx, claim.parent, claim.dir, claim.parentInfo)
			if err != nil {
				results <- result{err: err}
				return
			}
			info, err := guard.file.Stat()
			results <- result{info, errors.Join(err, guard.file.Close())}
		})
	}
	close(start)
	workers.Wait()
	want := assertGuardInode(t, dir, nil)
	for range count {
		got := <-results
		must(t, got.err)
		if !os.SameFile(want, got.info) {
			t.Fatal("concurrent first opens used distinct guard inodes")
		}
	}
	backupChildren(t, dir, stagingGuard)
}

func testRestrictedGuardCancellation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, stagingGuard)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	must(t, err)
	defer func() { must(t, file.Close()) }()
	must(t, file.Chmod(0))
	must(t, tryLock(file))
	original, err := file.Stat()
	must(t, err)
	dest := filepath.Join(dir, "out")
	f := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	defer syscall.Umask(syscall.Umask(0777))
	if err := Export(ctx, f, dest); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("publisher waiting for the original guard lock returned %v", err)
	}
	backupChildren(t, dir, stagingGuard)
	assertGuardInode(t, dir, original)
	must(t, unlock(file))

	fillCtx, cancelFill := context.WithCancel(t.Context())
	defer cancelFill()
	err = publish(fillCtx, dest, func(stage *os.Root) error {
		if err := stage.WriteFile("partial", []byte("staged data"), 0600); err != nil {
			return err
		}
		cancelFill()
		return fillCtx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled fill returned %v", err)
	}
	backupChildren(t, dir, stagingGuard)
	must(t, Export(t.Context(), f, dest))
	assertBackupPoint(t, dest, f)
	backupChildren(t, dir, stagingGuard, "out")
}

func TestStagingGuardRepairsRestrictedOwnerModes(t *testing.T) {
	for _, mode := range []os.FileMode{0, 0200, 0400} {
		t.Run(fmt.Sprintf("%04o", mode), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, stagingGuard)
			must(t, os.WriteFile(path, nil, 0600))
			must(t, os.Chmod(path, mode))
			before, err := os.Lstat(path)
			must(t, err)
			f := newFixture(t)
			out := filepath.Join(dir, "out")
			must(t, Export(t.Context(), f, out))
			assertGuardInode(t, dir, before)
			assertBackupPoint(t, out, f)
			backupChildren(t, dir, stagingGuard, "out")
		})
	}
}
