package fuzz

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	workerEnv       = "BOG_FUZZ_CORPUS_WORKER"
	corpusDeadline  = 45 * time.Second
	addressHeadroom = 256 << 20
	maxOutputFile   = 1 << 20
)

func TestMalformedArtifacts(t *testing.T) {
	for _, suite := range []string{"backup", "history"} {
		t.Run(suite, func(t *testing.T) { runCorpusWorker(t, suite) })
	}
}

func TestStorageRecovery(t *testing.T) {
	for _, suite := range []string{"wal-recovery", "checkpoint-publication"} {
		t.Run(suite, func(t *testing.T) { runCorpusWorker(t, suite) })
	}
}

func TestInterruptedBackupRetries(t *testing.T) {
	runCorpusWorker(t, "backup-retries")
}

func TestBackupRetryRestrictiveUmask(t *testing.T) {
	runCorpusWorker(t, "restrictive-umask")
}

func runCorpusWorker(t *testing.T, suite string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), corpusDeadline)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/proc/self/exe", "-test.run=^TestCorpusWorker$", "-test.count=1", "-test.v")
	cmd.WaitDelay = time.Second

	var temporaryRoot string
	if suite == "restrictive-umask" && os.Geteuid() == 0 {
		// Test under a normal effective UID even on root release runners.
		// Pass the executable by descriptor because go test may place it
		// under private root-owned directories.
		const uid, gid = 65534, 65534
		var err error
		temporaryRoot, err = os.MkdirTemp("/tmp", "recovery-umask-")
		must(t, err)
		t.Cleanup(func() { must(t, os.RemoveAll(temporaryRoot)) })
		must(t, os.Chown(temporaryRoot, uid, gid))
		executable, err := os.Open("/proc/self/exe")
		must(t, err)
		defer func() { must(t, executable.Close()) }()
		cmd.Path = "/proc/self/fd/3"
		cmd.ExtraFiles = []*os.File{executable}
		cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uid, Gid: gid}}
	} else {
		temporaryRoot = t.TempDir()
	}
	// The parent owns the temporary root even when killing the worker.
	cmd.Env = append(cmd.Environ(), workerEnv+"="+suite, "TMPDIR="+temporaryRoot)
	output, err := runBoundedReplayWorker(cmd)
	if ctx.Err() != nil {
		t.Fatalf("%s exceeded %s: %v\n%s", suite, corpusDeadline, ctx.Err(), output)
	}
	if err != nil {
		t.Fatalf("%s worker failed: %v\n%s", suite, err, output)
	}
	if testing.Verbose() {
		t.Logf("%s", output)
	}
}

func TestCorpusWorker(t *testing.T) {
	suite := os.Getenv(workerEnv)
	if suite == "" {
		t.Skip("started by the corpus tests")
	}
	setWorkerLimits(t)
	switch suite {
	case "backup":
		runBackupCorpus(t, newFixture(t))
	case "history":
		runHistoryCorpus(t, newFixture(t))
	case "wal-recovery":
		runWALRecovery(t)
	case "checkpoint-publication":
		runCheckpointCorpus(t)
	case "backup-retries":
		runBackupRetries(t)
	case "restrictive-umask":
		runRestrictiveUmask(t)
	default:
		t.Fatalf("unrecognized corpus %q", suite)
	}
}

func setWorkerLimits(t *testing.T) {
	t.Helper()
	setWorkerLimitsWithAddressHeadroom(t, addressHeadroom)
}

func setWorkerLimitsWithAddressHeadroom(t *testing.T, headroom uint64) {
	t.Helper()
	// The Go runtime, especially with -race, reserves substantial virtual
	// address space before TestMain. Budget further address-space growth
	// relative to the process's existing Linux mappings.
	status, err := os.ReadFile("/proc/self/status")
	must(t, err)
	for line := range bytes.Lines(status) {
		value, ok := strings.CutPrefix(string(line), "VmSize:")
		if !ok {
			continue
		}
		fields := strings.Fields(value)
		if len(fields) != 2 || fields[1] != "kB" {
			t.Fatalf("unexpected VmSize format %q", value)
		}
		kib, err := strconv.ParseUint(fields[0], 10, 64)
		must(t, err)
		setHardLimit(t, syscall.RLIMIT_AS, kib*1024+headroom)
		setHardLimit(t, syscall.RLIMIT_FSIZE, maxOutputFile)
		return
	}
	t.Fatal("missing VmSize in /proc/self/status")
}

func setHardLimit(t *testing.T, resource int, maximum uint64) {
	t.Helper()
	var current syscall.Rlimit
	if err := syscall.Getrlimit(resource, &current); err != nil {
		t.Fatal(fmt.Errorf("get resource limit %d: %w", resource, err))
	}
	maximum = min(current.Cur, current.Max, maximum)
	if err := syscall.Setrlimit(resource, &syscall.Rlimit{Cur: maximum, Max: maximum}); err != nil {
		t.Fatal(fmt.Errorf("set resource limit %d: %w", resource, err))
	}
}

func setRetainedWorkerLimits(t *testing.T) { setWorkerLimits(t) }
