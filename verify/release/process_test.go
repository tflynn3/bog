package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCandidateExitProvenance(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, script, want string
		stopSent           bool
	}{
		{"requested stop", "kill -HUP $$", "", true},
		{"independent hangup", "kill -HUP $$", "hangup", false},
		{"independent kill", "kill -KILL $$", "killed", false},
		{"kill despite stop request", "kill -KILL $$", "killed", true},
		{"independent success", "exit 0", "status 0", false},
		{"success despite stop request", "exit 0", "status 0", true},
		{"failed exit despite stop request", "exit 19", "exit status 19", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			waitErr := exec.Command("/bin/sh", "-c", tc.script).Run()
			err := candidateExitError(tc.stopSent, waitErr)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), tc.want) {
				t.Fatalf("exit error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestFixtureCleanupKeepsPrimaryDiagnostic(t *testing.T) {
	t.Parallel()
	probeErr := errors.New("first probe failed")
	cleanupErr := errors.New("fixture directory cannot be removed")
	for _, tc := range []struct {
		name          string
		probe, remove error
	}{
		{"both fail", probeErr, cleanupErr},
		{"cleanup alone fails", nil, cleanupErr},
		{"probe alone fails", probeErr, nil},
		{"both succeed", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const root = "/fixture"
			calls := 0
			err := addFixtureCleanup(tc.probe, root, func(got string) error {
				calls++
				if got != root {
					t.Errorf("remove %q, want %q", got, root)
				}
				return tc.remove
			})
			if calls != 1 {
				t.Fatalf("remove called %d times", calls)
			}
			if tc.probe == nil && tc.remove == nil {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if tc.probe != nil && (!errors.Is(err, tc.probe) || !strings.HasPrefix(err.Error(), tc.probe.Error())) {
				t.Errorf("probe error lost precedence: %v", err)
			}
			if tc.remove != nil && (!errors.Is(err, tc.remove) || !strings.Contains(err.Error(), "remove release fixture root")) {
				t.Errorf("missing cleanup failure: %v", err)
			}
		})
	}
}

func TestFixtureRootIsRemovedAfterFailure(t *testing.T) {
	t.Parallel()
	var root string
	probeErr := errors.New("abort probe")
	err := withFixtureRoot(func(dir string) error {
		root = dir
		return errors.Join(probeErr, os.WriteFile(filepath.Join(dir, "fixture"), []byte("data"), 0600))
	})
	if !errors.Is(err, probeErr) || err.Error() != probeErr.Error() {
		t.Fatalf("fixture returned %v", err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fixture root remained: stat = %v", err)
	}
}
