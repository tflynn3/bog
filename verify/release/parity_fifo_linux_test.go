package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestTailParityReaderFIFODeadline(t *testing.T) {
	const childDir = "BOG_TEST_TAIL_PARITY_FIFO_DIR"
	const childArtifact = "BOG_TEST_TAIL_PARITY_FIFO_ARTIFACT"
	if dir := os.Getenv(childDir); dir != "" {
		artifact := os.Getenv(childArtifact)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		got, err := readTailParityImage(ctx, dir, "BOGBAK01")
		want := fmt.Sprintf("open tail parity image %q artifact %q: artifact is not a regular file", dir, artifact)
		if artifact == tailBackupName {
			want = fmt.Sprintf("read tail parity image %q: read backup magic: open artifact %q: artifact is not a regular file", dir, artifact)
		}
		if got != (tailParityPoint{}) || !errors.Is(err, errArtifactNonregular) || err.Error() != want {
			t.Fatalf("read = (%+v, %v), want zero point and %q", got, err, want)
		}
		return
	}

	t.Parallel()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, artifact := range []string{tailBackupName, "bog.checkpoint", "bog.wal.tail"} {
		t.Run(artifact, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			for name, data := range map[string][]byte{
				tailBackupName:   append([]byte("BOGBAK01"), make([]byte, 120)...),
				"bog.checkpoint": nil,
				"bog.wal.tail":   nil,
			} {
				path := filepath.Join(dir, name)
				if name == artifact {
					if err := syscall.Mkfifo(path, 0600); err != nil {
						t.Fatal(err)
					}
				} else {
					writeFixture(t, path, data)
				}
			}

			// Context cancellation cannot interrupt a blocking FIFO open.
			// The parent deadline kills and reaps the reader in that case.
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestTailParityReaderFIFODeadline$")
			cmd.Env = append(os.Environ(), childDir+"="+dir, childArtifact+"="+artifact)
			cmd.WaitDelay = time.Second
			output, err := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("reader did not reject FIFO %q before the deadline: %v\n%s", artifact, errors.Join(ctx.Err(), err), output)
			}
			if err != nil {
				t.Fatalf("reader subprocess failed: %v\n%s", err, output)
			}
		})
	}
}
