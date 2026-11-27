package fuzz

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"
)

const scanProcessTestEnv = "BOG_FUZZ_SCAN_PROCESS_TEST"

func TestScanWorkerProcessLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name, mode string
		outcome    scanWorkerOutcome
		ready      bool
		started    bool
		reaped     bool
	}{
		{"launch failed", "", scanWorkerStartup, false, false, false},
		{"bad ready", "malformed", scanWorkerStartup, false, false, true},
		{"startup deadline", "blocked", scanWorkerStartup, false, false, true},
		{"startup sanitizer", "sanitizer", scanWorkerSanitizer, false, false, true},
		{"admission failed", "denied", scanWorkerStartup, true, false, true},
		{"admission canceled", "cancel", scanWorkerStartup, true, false, true},
		{"workload failed", "budget", scanWorkerBudget, true, true, true},
		{"workload deadline", "slow", scanWorkerFailed, true, true, true},
		{"success", "pass", scanWorkerPassed, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer cancel()
			executable := "/proc/self/exe"
			if tc.mode == "" {
				executable = "/dev/null/no-executable"
			}
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestScanWorkerProcessHelper$", "-test.count=1")
			// Test the handshake and reaping independently of the race runtime's exit sleep.
			cmd.Env = append(cmd.Environ(), scanProcessTestEnv+"="+tc.mode, "GORACE=atexit_sleep_ms=0")
			deadline := scanStartupDeadline
			if tc.mode == "blocked" {
				deadline = 250 * time.Millisecond
			}
			denied := errors.New("isolation setup failed")
			admissions := 0
			evidence, output, err := startScanWorker(ctx, cmd, deadline, 8, func(pid int) error {
				admissions++
				if pid != cmd.Process.Pid {
					return fmt.Errorf("unexpected child pid %d", pid)
				}
				if tc.mode == "cancel" {
					cancel()
				}
				if tc.mode == "denied" {
					return denied
				}
				return nil
			})
			if (err == nil) != (tc.outcome == scanWorkerPassed) || evidence.Outcome != tc.outcome ||
				(evidence.Ready != nil) != tc.ready || evidence.Started != tc.started || evidence.Reaped != tc.reaped {
				t.Fatalf("evidence=%+v err=%v output=%q", evidence, err, output)
			}
			if tc.mode == "denied" && !errors.Is(err, denied) {
				t.Fatalf("lost admission error: %v", err)
			}
			if tc.mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost admission cancellation: %v", err)
			}
			if tc.mode == "blocked" && !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("lost startup deadline: %v", err)
			}
			if tc.mode == "slow" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("lost workload deadline: %v", err)
			}
			if (admissions == 1) != tc.ready || bytes.Contains(output, []byte("workload entered")) != tc.started {
				t.Fatalf("admission attempts=%d output=%q", admissions, output)
			}
		})
	}
}

func TestScanWorkerProcessHelper(t *testing.T) {
	mode := os.Getenv(scanProcessTestEnv)
	switch mode {
	case "":
		t.Skip("started by lifecycle test")
	case "blocked":
		// Keep the readiness pipe open without reporting readiness.
		_, err := io.Copy(io.Discard, os.Stdin)
		must(t, err)
		return
	case "malformed":
		f := os.NewFile(3, "invalid-ready")
		_, err := f.WriteString("{")
		must(t, errors.Join(err, f.Close()))
		return
	case "sanitizer":
		_, err := fmt.Fprintln(os.Stderr, "FATAL: ThreadSanitizer: unexpected memory mapping")
		must(t, err)
		t.Fatal("synthetic sanitizer startup failure")
	}
	must(t, announceScanWorker(8))
	_, err := fmt.Fprintln(os.Stdout, "workload entered")
	must(t, err)
	switch mode {
	case "budget":
		_, err := fmt.Fprintln(os.Stdout, scanBudgetMarker)
		must(t, err)
		t.Fatal("synthetic reader budget failure")
	case "slow":
		// A long timer simulates a reader that cannot respond to cancellation.
		// The supervising process deadline must terminate and reap it.
		timer := time.NewTimer(time.Minute)
		defer timer.Stop()
		<-timer.C
	}
}
