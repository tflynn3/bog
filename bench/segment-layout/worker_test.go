package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

var testCache cachePolicy = func(*os.File) (func() error, error) {
	return func() error { return nil }, nil
}
var testPeakRSS = func() (int64, error) { return 1024, nil }

func TestMain(m *testing.M) {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case workerArgument:
			if err := workerMain(os.Args[1:], os.Stdout, testCache, testPeakRSS); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			return
		case "--test-worker":
			if err := testWorker(os.Args[2:]); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
			return
		}
	}
	os.Exit(m.Run())
}

func assertEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("benchmark files remain in %s: %v, %v", dir, entries, err)
	}
}

func testExecutable(t *testing.T) string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return executable
}

func testWorker(args []string) error {
	switch args[0] {
	case "fail":
		return errors.New("injected worker failure")
	case "invalid":
		_, err := io.WriteString(os.Stdout, "{} trailing-data")
		return err
	case "oversized":
		_, err := io.WriteString(os.Stdout, strings.Repeat("x", maxWorkerOutput+1))
		return err
	case "block":
		marker := args[1]
		if err := os.WriteFile(marker+".ready", []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
			return err
		}
		if err := os.Rename(marker+".ready", marker); err != nil {
			return err
		}
		time.Sleep(time.Minute)
		return errors.New("worker was not canceled")
	default:
		return errors.New("unknown test worker behavior")
	}
}

func TestPreparedWorkerMatrix(t *testing.T) {
	parent, executable := t.TempDir(), testExecutable(t)
	err := withWorkspace(parent, func(dir string) error {
		cfg := config{batches: 4, runs: 1, profile: "mixed"}
		results, err := prepare(t.Context(), dir, cfg)
		if err != nil {
			return err
		}
		if err := collectWorkers(t.Context(), executable, dir, results, cfg.runs); err != nil {
			return err
		}
		for i, result := range results {
			if result.Name != layouts[i].name() || result.PeakWorkerRSSBytes <= 0 {
				t.Errorf("invalid layout identity or RSS: %s (%d bytes)", result.Name, result.PeakWorkerRSSBytes)
			}
			for j, phase := range phases {
				g := result.generation(j)
				if len(g.OracleSHA256) != 64 || g.OracleSHA256 != results[0].generation(j).OracleSHA256 {
					t.Errorf("%s/%s: missing or mismatched fixture fingerprint", result.Name, phase)
				}
				for _, m := range g.Queries {
					if m.Name == "bloom-false-positive" && (m.MatchingRows != 0 || m.Blocks < 1) {
						t.Errorf("%s/%s: false-positive probe did not decode and reject a candidate", result.Name, phase)
					}
					if !m.OrderedParity || (m.Blocks > 0 && m.DecodedBytes == 0) || m.DecodedBytes > m.BaselineDecodedBytes {
						t.Errorf("%s/%s/%s: invalid parity or decoded bytes", result.Name, phase, m.Name)
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertEmpty(t, parent)
}

func TestWorkerFailureCleansWorkspace(t *testing.T) {
	executable := testExecutable(t)
	for _, tc := range []struct {
		name, executable, want string
		args                   []string
	}{
		{"start", filepath.Join(t.TempDir(), "missing"), "no such file", nil},
		{"exit", executable, "injected worker failure", []string{"--test-worker", "fail"}},
		{"report", executable, "decode worker report", []string{"--test-worker", "invalid"}},
		{"oversized", executable, "worker output exceeded its size limit", []string{"--test-worker", "oversized"}},
		{"missing data", executable, "initial/all", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := t.TempDir()
			err := withWorkspace(parent, func(dir string) error {
				args := tc.args
				if tc.name == "missing data" {
					args = workerArgs(dir, layouts[0], 1)
				}
				_, err := invokeWorker(t.Context(), tc.executable, args)
				return err
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("worker error = %v, want %q", err, tc.want)
			}
			assertEmpty(t, parent)
		})
	}
}

func TestCancelWorkerReapsBeforeCleanup(t *testing.T) {
	parent, executable := t.TempDir(), testExecutable(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err := withWorkspace(parent, func(dir string) error {
		marker := filepath.Join(dir, "worker-pid")
		result := make(chan error, 1)
		go func() {
			_, err := invokeWorker(ctx, executable, []string{"--test-worker", "block", marker})
			result <- err
		}()

		startup, stop := context.WithTimeout(t.Context(), 5*time.Second)
		defer stop()
		pid, err := waitForWorkerPID(startup, marker)
		cancel()
		workerErr := <-result
		if err != nil {
			return errors.Join(err, workerErr)
		}
		if !errors.Is(workerErr, context.Canceled) {
			return fmt.Errorf("expected cancellation, got %v", workerErr)
		}
		if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
			return fmt.Errorf("worker PID %d still present after wait: %v", pid, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assertEmpty(t, parent)
}

func waitForWorkerPID(ctx context.Context, marker string) (int, error) {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		data, err := os.ReadFile(marker)
		if err == nil {
			return strconv.Atoi(string(data))
		}
		if !errors.Is(err, os.ErrNotExist) {
			return 0, err
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-tick.C:
		}
	}
}
