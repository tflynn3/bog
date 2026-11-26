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
	exe, err := os.Executable()
	must(t, err)
	for _, suite := range []string{"backup", "history"} {
		t.Run(suite, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), corpusDeadline)
			defer cancel()
			cmd := exec.CommandContext(ctx, exe, "-test.run=^TestCorpusWorker$", "-test.count=1", "-test.v")
			// The parent owns the temporary root, even if it kills a stuck
			// worker before the testing package can clean up its files.
			cmd.Env = append(cmd.Environ(), workerEnv+"="+suite, "TMPDIR="+t.TempDir())
			cmd.WaitDelay = time.Second
			output, err := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("%s corpus exceeded %s: %v\n%s", suite, corpusDeadline, ctx.Err(), output)
			}
			if err != nil {
				t.Fatalf("%s corpus subprocess failed: %v\n%s", suite, err, output)
			}
			if testing.Verbose() {
				t.Logf("%s", output)
			}
		})
	}
}

func TestCorpusWorker(t *testing.T) {
	suite := os.Getenv(workerEnv)
	if suite == "" {
		t.Skip("started by TestMalformedArtifacts")
	}
	setWorkerLimits(t)
	f := newFixture(t)
	switch suite {
	case "backup":
		runBackupCorpus(t, f)
	case "history":
		runHistoryCorpus(t, f)
	default:
		t.Fatalf("unrecognized corpus %q", suite)
	}
}

func setWorkerLimits(t *testing.T) {
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
		setHardLimit(t, syscall.RLIMIT_AS, kib*1024+addressHeadroom)
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
