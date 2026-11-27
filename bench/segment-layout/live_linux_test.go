package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

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
