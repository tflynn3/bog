package history

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"bog/internal/checkpoint"
)

const (
	pinTestDir  = "BOG_TEST_PIN_DIR"
	pinTestRuns = 300
)

// The helper intentionally leaves all references open so the parent can check
// both cross-process protection and the kernel's release on abrupt exit.
func TestGenerationPinProcess(t *testing.T) {
	dir := os.Getenv(pinTestDir)
	if dir == "" {
		t.Skip("subprocess helper")
	}
	var limit syscall.Rlimit
	requireOK(t, syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit))
	limit.Cur = 256
	requireOK(t, syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit))

	var cat Catalog
	_, err := cat.Replace(context.Background(), dir)
	requireOK(t, err)
	r, err := cat.AcquireCurrent()
	requireOK(t, err)
	requireOK(t, cat.Close())
	_, err = fmt.Fprintln(os.Stdout, "pinned")
	requireOK(t, err)

	input := bufio.NewScanner(os.Stdin)
	if !input.Scan() || input.Text() != "scan" {
		t.Fatalf("expected scan command, got %q (%v)", input.Text(), input.Err())
	}
	got := scanRows(t, r, all)
	if len(got) != pinTestRuns {
		t.Fatalf("old reader saw %d of %d rows", len(got), pinTestRuns)
	}
	for i, item := range got {
		if item != row(epoch, uint64(i), "old") {
			t.Fatalf("old row %d changed: %+v", i, item)
		}
	}
	_, err = fmt.Fprintln(os.Stdout, "scanned")
	requireOK(t, err)
	if input.Scan() {
		t.Fatalf("unexpected command %q", input.Text())
	}
	requireOK(t, input.Err())
}

func TestGenerationPinsAcrossProcessesAtDescriptorLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	runs := make([][]checkpoint.BlockRow, pinTestRuns)
	for i := range runs {
		runs[i] = []checkpoint.BlockRow{row(epoch, uint64(i), "old")}
	}
	dir, first := segmentFixture(t, runs)
	var cat Catalog
	cleanup(t, cat.Close)
	old, err := cat.Replace(ctx, dir)
	requireOK(t, err)

	exe, err := os.Executable()
	requireOK(t, err)
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestGenerationPinProcess$")
	cmd.Env = append(os.Environ(), pinTestDir+"="+dir)
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	requireOK(t, err)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		requireOK(t, errors.Join(err, stdin.Close()))
	}
	requireOK(t, cmd.Start())
	waited := false
	defer func() {
		if !waited {
			if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Error(err)
			}
			if err := cmd.Wait(); err != nil {
				var exited *exec.ExitError
				if !errors.As(err, &exited) {
					t.Error(err)
				}
			}
		}
	}()
	output := bufio.NewScanner(stdout)
	expect := func(want string) {
		t.Helper()
		if !output.Scan() || output.Text() != want {
			t.Fatalf("helper did not report %q, got %q (%v)", want, output.Text(), output.Err())
		}
	}
	expect("pinned")

	broken := first
	broken.Segments = slices.Clone(first.Segments)
	broken.Segments[0].SHA256 = strings.Repeat("0", 64)
	writeManifest(t, dir, broken)
	if _, err := cat.Replace(ctx, dir); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("failed replacement returned %v", err)
	}
	oldReader, err := cat.Acquire(old.ID)
	requireOK(t, err)
	cleanup(t, oldReader.Close)
	if got := scanRows(t, oldReader, all); len(got) != pinTestRuns {
		t.Fatalf("failed replacement changed the old snapshot: %d rows", len(got))
	}
	requireOK(t, oldReader.Close())

	next := first
	next.NextOrdinal = 2
	nextRow := row(epoch.Add(-time.Nanosecond), 1, "backfill")
	shared := first.Segments[0]
	next.Segments = []segmentMeta{installHistoryRun(t, dir, len(runs), nextRow), shared}
	writeManifest(t, dir, next)
	_, err = cat.Replace(ctx, dir)
	requireOK(t, err)
	_, err = cat.Acquire(old.ID)
	assertExpired(t, err, old.ID)
	requireOK(t, cat.Reclaim(ctx, dir))
	for _, s := range first.Segments {
		assertSegment(t, dir, s.Name, true)
	}

	_, err = fmt.Fprintln(stdin, "scan")
	requireOK(t, err)
	expect("scanned")
	requireOK(t, cmd.Process.Kill())
	err = cmd.Wait()
	waited = true
	var exited *exec.ExitError
	if !errors.As(err, &exited) || exited.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("expected abrupt helper exit, got %v", err)
	}

	requireOK(t, cat.Reclaim(ctx, dir))
	for _, s := range first.Segments[1:] {
		assertSegment(t, dir, s.Name, false)
	}
	assertSegment(t, dir, shared.Name, true)
	r, err := cat.AcquireCurrent()
	requireOK(t, err)
	cleanup(t, r.Close)
	if got := scanRows(t, r, all); !slices.Equal(got, []checkpoint.BlockRow{nextRow, runs[0][0]}) {
		t.Fatalf("successor rows after reclaim: %v", got)
	}
}

func TestReclaimFailsClosedOnMalformedLivePin(t *testing.T) {
	ctx := context.Background()
	dir, old := segmentFixture(t, [][]checkpoint.BlockRow{{row(epoch, 0, "old")}})
	next := old
	next.Segments = []segmentMeta{installHistoryRun(t, dir, 1, row(epoch, 0, "next"))}
	var cat Catalog
	cleanup(t, cat.Close)
	_, err := cat.Replace(ctx, dir)
	requireOK(t, err)
	writeManifest(t, dir, next)
	_, err = cat.Replace(ctx, dir)
	requireOK(t, err)

	name := pinPrefix + strings.Repeat("a", pinTokenSize)
	path := filepath.Join(dir, name)
	writeFile(t, path, []byte("incomplete"))
	staged := pinStagePrefix + strings.Repeat("b", pinTokenSize)
	writeFile(t, filepath.Join(dir, staged), nil)
	requireOK(t, os.Chmod(filepath.Join(dir, staged), 0))
	func() {
		f, err := os.Open(path)
		requireOK(t, err)
		defer func() { requireOK(t, f.Close()) }()
		requireOK(t, syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB))
		if err := cat.Reclaim(ctx, dir); !errors.Is(err, ErrIntegrity) {
			t.Fatalf("reclaim with malformed live record returned %v", err)
		}
		assertSegment(t, dir, old.Segments[0].Name, true)
	}()
	requireOK(t, cat.Reclaim(ctx, dir))
	for _, removed := range []string{name, staged, old.Segments[0].Name} {
		assertSegment(t, dir, removed, false)
	}
	assertSegment(t, dir, next.Segments[0].Name, true)
}

func TestRecoverPinsPreservesLiveGeneration(t *testing.T) {
	dir, manifest := segmentFixture(t, [][]checkpoint.BlockRow{{row(epoch, 0, "live")}})
	var catalog Catalog
	cleanup(t, catalog.Close)
	_, err := catalog.Replace(t.Context(), dir)
	requireOK(t, err)
	live := catalog.current.pin.Name()
	dead := pinPrefix + strings.Repeat("a", pinTokenSize)
	staged := pinStagePrefix + strings.Repeat("b", pinTokenSize)
	for _, name := range []string{dead, staged, "unrelated"} {
		writeFile(t, filepath.Join(dir, name), []byte("interrupted"))
	}
	pinned, _, err := openDirectory(dir)
	requireOK(t, err)
	cleanup(t, pinned.Close)

	for range 2 {
		requireOK(t, RecoverPins(t.Context(), pinned))
		assertSegment(t, dir, live, true)
		assertSegment(t, dir, manifest.Segments[0].Name, true)
		assertSegment(t, dir, "unrelated", true)
		assertSegment(t, dir, dead, false)
		assertSegment(t, dir, staged, false)
	}
}

func TestReleaseDuringExclusivePublicationDefersPinCleanup(t *testing.T) {
	ctx := context.Background()
	dir, first := segmentFixture(t, [][]checkpoint.BlockRow{{row(epoch, 0, "old")}})
	var cat Catalog
	cleanup(t, cat.Close)
	g, err := cat.Replace(ctx, dir)
	requireOK(t, err)
	r, err := cat.Acquire(g.ID)
	requireOK(t, err)
	cleanup(t, r.Close)
	pin := r.gen.pin.Name()

	second := first
	second.Segments = []segmentMeta{installHistoryRun(t, dir, 1, row(epoch, 0, "next"))}
	writeManifest(t, dir, second)
	_, err = cat.Replace(ctx, dir)
	requireOK(t, err)

	d, _, err := openDirectory(dir)
	requireOK(t, err)
	cleanup(t, d.Close)
	func() {
		lock, err := checkpoint.LockPublication(d, true)
		requireOK(t, err)
		defer func() { requireOK(t, lock.Close()) }()
		requireOK(t, r.Close())
		assertSegment(t, dir, pin, true)
	}()
	select {
	case <-g.Drained:
	default:
		t.Fatal("old generation did not drain during publication")
	}

	// A different inode now occupies the old name. The retired record is
	// eligible for cleanup, while the replacement inode is unknown to Catalog.
	name := first.Segments[0].Name
	requireOK(t, os.Rename(filepath.Join(dir, name), filepath.Join(dir, "moved-original")))
	replacement := filepath.Join(dir, name)
	writeFile(t, replacement, []byte("replacement"))
	requireOK(t, cat.Reclaim(ctx, dir))
	assertSegment(t, dir, pin, false)
	got, err := os.ReadFile(replacement)
	requireOK(t, err)
	if string(got) != "replacement" {
		t.Fatalf("unexpected replacement content: %q", got)
	}
}

func TestFailedPinRegistrationPreservesCurrentGeneration(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root may bypass directory write permissions")
	}
	ctx := context.Background()
	oldRow := row(epoch, 0, "old")
	dir, first := segmentFixture(t, [][]checkpoint.BlockRow{{oldRow}})
	var cat Catalog
	cleanup(t, cat.Close)
	old, err := cat.Replace(ctx, dir)
	requireOK(t, err)

	second := first
	second.Segments = []segmentMeta{installHistoryRun(t, dir, 1, row(epoch, 0, "next"))}
	writeManifest(t, dir, second)
	requireOK(t, os.Chmod(dir, 0500))
	defer func() { requireOK(t, os.Chmod(dir, 0700)) }()
	if g, err := cat.Replace(ctx, dir); !errors.Is(err, os.ErrPermission) || g != (Generation{}) {
		t.Fatalf("expected generation pin creation to fail, got generation %+v and %v", g, err)
	}
	r, err := cat.Acquire(old.ID)
	requireOK(t, err)
	cleanup(t, r.Close)
	if got := scanRows(t, r, all); !slices.Equal(got, []checkpoint.BlockRow{oldRow}) {
		t.Fatalf("failed pin registration changed the old snapshot: %v", got)
	}
}
