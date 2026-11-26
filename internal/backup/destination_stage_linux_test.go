package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

const destinationStageChildEnv = "BOG_TEST_DESTINATION_STAGE_CHILD"

func TestDestinationStageSIGKILLHelper(t *testing.T) {
	if os.Getenv(destinationStageChildEnv) != "1" {
		return
	}
	bundle := os.Getenv("BOG_TEST_DESTINATION_STAGE_SOURCE")
	target := os.Getenv("BOG_TEST_DESTINATION_STAGE_TARGET")
	ready := os.NewFile(3, "stage ready")
	if ready == nil {
		t.Fatal("missing readiness pipe")
	}
	must(t, publishDestinationStage(t.Context(), target, func(stage *os.Root) error {
		if err := copyDestinationStageFixture(t.Context(), bundle, stage); err != nil {
			return errors.Join(err, ready.Close())
		}
		_, err := ready.Write([]byte{'R'})
		if err := errors.Join(err, ready.Close()); err != nil {
			return err
		}
		// The parent kills the process after observing the filled stage.
		timer := time.NewTimer(time.Minute)
		defer timer.Stop()
		select {
		case <-timer.C:
			return errors.New("interruption test child timed out awaiting SIGKILL")
		case <-t.Context().Done():
			return t.Context().Err()
		}
	}))
}

func copyDestinationStageFixture(ctx context.Context, bundle string, stage *os.Root) (err error) {
	src, err := os.OpenRoot(bundle)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, src.Close()) }()
	img, err := inspect(src)
	if err != nil {
		return err
	}
	names := []string{metadataName, manifestName, tailName}
	for _, segment := range img.Segments {
		names = append(names, segment.Name)
	}
	for _, name := range names {
		if err := copyRegular(ctx, src, stage, name, 0, blockLimit); err != nil {
			return err
		}
	}
	_, _, err = verifyImage(ctx, stage)
	return err
}

func killDestinationStagePublisher(t *testing.T, bundle, target string, beforeKill func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	reader, writer, err := os.Pipe()
	must(t, err)
	defer func() { must(t, reader.Close()) }()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDestinationStageSIGKILLHelper$")
	cmd.Env = append(os.Environ(),
		destinationStageChildEnv+"=1",
		"BOG_TEST_DESTINATION_STAGE_SOURCE="+bundle,
		"BOG_TEST_DESTINATION_STAGE_TARGET="+target,
	)
	cmd.ExtraFiles = []*os.File{writer}
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	startErr := cmd.Start()
	closeErr := writer.Close()
	if startErr != nil {
		t.Fatal(errors.Join(startErr, closeErr))
	}
	waited := false
	defer func() {
		if !waited {
			killErr := cmd.Process.Kill()
			if killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
				t.Errorf("kill child during cleanup: %v", killErr)
			}
			if err := cmd.Wait(); err != nil {
				if _, ok := err.(*exec.ExitError); !ok {
					t.Errorf("wait for child during cleanup: %v", err)
				}
			}
		}
	}()
	must(t, closeErr)
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("missing child deadline")
	}
	must(t, reader.SetReadDeadline(deadline))
	var ready [1]byte
	_, err = io.ReadFull(reader, ready[:])
	must(t, err)
	if ready[0] != 'R' {
		t.Fatalf("unexpected readiness byte %q", ready[0])
	}
	beforeKill()
	must(t, cmd.Process.Kill())
	err = cmd.Wait()
	waited = true
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("expected a child killed by SIGKILL, got %v", err)
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("unexpected child exit status: %v", err)
	}
}

func TestDestinationStageRecoversRepeatedSIGKILL(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	input := newFixture(t)
	bundle := filepath.Join(t.TempDir(), "source")
	must(t, Export(ctx, input, bundle))

	parent := t.TempDir()
	target := filepath.Join(parent, "target")
	stage, lock := destinationStageTestNames(target)
	foreign := filepath.Join(parent, ".bog-backup-old-stage")
	must(t, os.Mkdir(foreign, 0700))
	must(t, os.WriteFile(filepath.Join(foreign, metadataName), []byte("keep"), 0600))
	for attempt := range 3 {
		killDestinationStagePublisher(t, bundle, target, func() {
			got, err := Verify(ctx, stage)
			must(t, err)
			if got != input.Point() {
				t.Fatalf("attempt %d staged point %+v, want %+v", attempt, got, input.Point())
			}
			wait, stop := context.WithTimeout(ctx, 50*time.Millisecond)
			defer stop()
			err = publishDestinationStage(wait, target, func(*os.Root) error {
				return errors.New("concurrent retry entered a live subprocess stage")
			})
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("attempt %d concurrent retry returned %v", attempt, err)
			}
		})
		entries, err := os.ReadDir(parent)
		must(t, err)
		want := map[string]bool{filepath.Base(stage): true, filepath.Base(lock): true, filepath.Base(foreign): true}
		for _, entry := range entries {
			if !want[entry.Name()] {
				t.Fatalf("attempt %d left unexpected entry %s", attempt, entry.Name())
			}
			delete(want, entry.Name())
		}
		if len(want) != 0 {
			t.Fatalf("attempt %d missing entries %v", attempt, want)
		}
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	var count sync.WaitGroup
	for range 2 {
		count.Go(func() {
			<-start
			results <- publishDestinationStage(ctx, target, func(root *os.Root) error {
				return copyDestinationStageFixture(ctx, bundle, root)
			})
		})
	}
	close(start)
	count.Wait()
	close(results)
	var successes int
	for err := range results {
		switch {
		case err == nil:
			successes++
		case err.Error() == "backup destination is not empty":
		default:
			t.Error(fmt.Errorf("retry after SIGKILL: %w", err))
		}
	}
	if successes != 1 {
		t.Fatalf("got %d successful retries, want one", successes)
	}
	got, err := Verify(ctx, target)
	must(t, err)
	if got != input.Point() {
		t.Fatalf("published point %+v, want %+v", got, input.Point())
	}
	requireNoDestinationStage(t, target)
	requireDestinationStageContents(t, filepath.Join(foreign, metadataName), "keep")
}
