package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"bog/internal/checkpoint"
)

const liveVerificationReadyEnv = "BOG_TEST_LIVE_VERIFICATION_READY"

func TestCandidateCleansUpProcessGroupAfterEarlyExit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	binary := filepath.Join(dir, "candidate")
	// The child inherits the candidate's log pipe and keeps it open if
	// cleanup reaps only the leader.
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nsleep 15 &\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	err := withCandidate(t.Context(), binary, dir, func(liveService) error {
		return errors.New("candidate unexpectedly acquired listener")
	})
	if err == nil || !strings.Contains(err.Error(), "candidate exited before startup") ||
		errors.Is(err, exec.ErrWaitDelay) {
		t.Fatalf("withCandidate = %v, want early-exit error with completed group cleanup", err)
	}
}

func TestLiveVerificationWorkerParity(t *testing.T) {
	t.Parallel()
	for _, fixture := range []struct{ profile, scenario string }{
		{"mixed", "clustered"},
		{"entropy", "scattered"},
	} {
		t.Run(fixture.profile+"/"+fixture.scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			req := componentRequest{Dir: t.TempDir(), Profile: fixture.profile, Scenario: fixture.scenario, Batches: 4, Seed: 44}
			if _, err := measureComponent(ctx, req); err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				name, wantErr string
				batches       int
				seed          uint64
			}{
				{"exact", "", req.Batches, req.Seed},
				{"wrong cutoff", "published cutoff does not cover the fixture", req.Batches - 1, req.Seed},
				{"wrong rows", "disagrees with exact oracle", req.Batches, req.Seed + 1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					request := req
					request.Batches, request.Seed = tc.batches, tc.seed
					err := verifyLivePublication(ctx, request)
					if tc.wantErr == "" {
						if err != nil {
							t.Fatal(err)
						}
					} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
						t.Fatalf("verification = %v, want %q", err, tc.wantErr)
					}
					checkLiveVerificationLock(t, req.Dir, nil)
				})
			}
		})
	}
}

func TestLiveVerificationWorkerCancellation(t *testing.T) {
	trial, stopTrial := context.WithTimeout(t.Context(), 30*time.Second)
	defer stopTrial()
	req := componentRequest{Dir: t.TempDir(), Profile: "mixed", Scenario: "clustered", Batches: 1, Seed: seed}
	input, _, err := req.fixtureBackfill()
	if err != nil {
		t.Fatal(err)
	}
	if err := withSeededPublication(trial, req.Dir, input, nil); err != nil {
		t.Fatal(err)
	}
	ready := filepath.Join(req.Dir, "verification-ready")
	t.Setenv(liveVerificationReadyEnv, ready)
	ctx, cancel := context.WithCancel(trial)
	done := make(chan struct{})
	var verificationErr error
	go func() {
		defer close(done)
		verificationErr = verifyLivePublication(ctx, req)
	}()
	defer func() {
		cancel()
		<-done
	}()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		select {
		case <-ticker.C:
		case <-done:
			t.Fatalf("verification exited before acquiring lock: %v", verificationErr)
		case <-trial.Done():
			t.Fatalf("waiting for verification worker: %v", trial.Err())
		}
	}
	checkLiveVerificationLock(t, req.Dir, syscall.EWOULDBLOCK)
	cancel()
	<-done
	var exitErr *exec.ExitError
	if !errors.Is(verificationErr, context.Canceled) || !errors.As(verificationErr, &exitErr) {
		t.Fatalf("verification = %v, want cancellation and a reaped process", verificationErr)
	}
	if status := exitErr.Sys().(syscall.WaitStatus); !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("verification process status = %v, want SIGKILL", status)
	}
	checkLiveVerificationLock(t, req.Dir, nil)
}

// Hold the verifier's real directory lock until the parent cancels the worker.
func holdLiveVerificationLock(arg, ready string) (err error) {
	var req workerRequest
	if err := json.Unmarshal([]byte(arg), &req); err != nil {
		return err
	}
	if req.LiveVerification == nil {
		return errors.New("expected a live verification request")
	}
	legacy, err := checkpoint.OpenLegacy(req.LiveVerification.Dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, legacy.Close()) }()
	if err := os.WriteFile(ready, nil, 0600); err != nil {
		return err
	}
	time.Sleep(30 * time.Second)
	return errors.New("parent did not cancel live verification worker")
}

func checkLiveVerificationLock(t *testing.T, dir string, want error) {
	t.Helper()
	legacy, err := checkpoint.OpenLegacy(dir)
	if err == nil {
		if err := legacy.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if !errors.Is(err, want) {
		t.Fatalf("open checkpoint directory = %v, want %v", err, want)
	}
}
