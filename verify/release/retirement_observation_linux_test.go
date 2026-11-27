package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

type retirementArtifact struct {
	name string
	info os.FileInfo
}

// observeRetirementDescriptor scans only the candidate's descriptors in procfs
// listing order. If several names identify the same file, the first wins.
// Empty strings and a nil error mean no matching descriptor was observed.
func observeRetirementDescriptor(ctx context.Context, pid int, artifacts []retirementArtifact) (artifact, descriptor string, err error) {
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	if pid <= 0 {
		return "", "", fmt.Errorf("invalid candidate PID %d", pid)
	}
	dir := fmt.Sprintf("/proc/%d/fd", pid)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", "", fmt.Errorf("inspect candidate descriptors: %w", err)
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return "", "", err
		}
		fd := filepath.Join(dir, entry.Name())
		// Follow the procfs link to compare device and inode, including for
		// files whose directory entries have already been removed.
		info, err := os.Stat(fd)
		if errors.Is(err, fs.ErrNotExist) {
			continue // A descriptor may close between listing and stat.
		}
		if err != nil {
			return "", "", fmt.Errorf("stat candidate descriptor %s: %w", fd, err)
		}
		for _, artifact := range artifacts {
			if os.SameFile(artifact.info, info) {
				if err := ctx.Err(); err != nil {
					return "", "", err
				}
				return artifact.name, fd, nil
			}
		}
	}
	return "", "", ctx.Err()
}

func TestObserveRetirementDescriptor(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "tail")
	unrelated := filepath.Join(dir, "unrelated")
	writeFixture(t, path, []byte("same contents"))
	writeFixture(t, unrelated, []byte("same contents"))
	original, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	other, err := os.Stat(unrelated)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTailFixture(t, file) })
	fd := fmt.Sprintf("/proc/%d/fd/%d", os.Getpid(), file.Fd())
	removeTailFixture(t, dir, "tail")
	// Reuse the pathname with the same contents but a different identity,
	// and keep that file open as well.
	writeFixture(t, path, []byte("same contents"))
	replacement, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTailFixture(t, replacement) })
	replacementInfo, err := replacement.Stat()
	if err != nil {
		t.Fatal(err)
	}
	replacementFD := fmt.Sprintf("/proc/%d/fd/%d", os.Getpid(), replacement.Fd())

	for _, tc := range []struct {
		name                 string
		artifacts            []retirementArtifact
		wantArtifact, wantFD string
	}{
		{
			name: "unlinked file with ordered aliases",
			artifacts: []retirementArtifact{
				{name: "unrelated", info: other},
				{name: "predecessor", info: original},
				{name: "alias", info: original},
			},
			wantArtifact: "predecessor", wantFD: fd,
		},
		{
			name:         "replacement at original path",
			artifacts:    []retirementArtifact{{name: "replacement", info: replacementInfo}},
			wantArtifact: "replacement", wantFD: replacementFD,
		},
		{
			name:      "no matching descriptor",
			artifacts: []retirementArtifact{{name: "unrelated", info: other}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			artifact, descriptor, err := observeRetirementDescriptor(t.Context(), os.Getpid(), tc.artifacts)
			if artifact != tc.wantArtifact || descriptor != tc.wantFD || err != nil {
				t.Fatalf("observation = (%q, %q, %v), want (%q, %q, nil)", artifact, descriptor, err, tc.wantArtifact, tc.wantFD)
			}
		})
	}
}

func TestObserveRetirementDescriptorErrors(t *testing.T) {
	t.Parallel()
	t.Run("canceled before inspection", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		artifact, fd, err := observeRetirementDescriptor(ctx, os.Getpid(), nil)
		if artifact != "" || fd != "" || err != context.Canceled {
			t.Fatalf("observation = (%q, %q, %v), want context.Canceled", artifact, fd, err)
		}
	})
	t.Run("invalid PID", func(t *testing.T) {
		artifact, fd, err := observeRetirementDescriptor(t.Context(), 0, nil)
		if artifact != "" || fd != "" || err == nil || err.Error() != "invalid candidate PID 0" {
			t.Fatalf("observation = (%q, %q, %v), want invalid candidate PID 0", artifact, fd, err)
		}
	})
	t.Run("descriptor directory missing", func(t *testing.T) {
		const pid = 1<<31 - 1 // Above Linux's PID limit.
		artifact, fd, err := observeRetirementDescriptor(t.Context(), pid, nil)
		var pathErr *fs.PathError
		if artifact != "" || fd != "" || !errors.Is(err, fs.ErrNotExist) ||
			!errors.As(err, &pathErr) || pathErr.Path != "/proc/2147483647/fd" {
			t.Fatalf("observation = (%q, %q, %v), want ENOENT for candidate descriptor directory", artifact, fd, err)
		}
	})
}
