package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 2 && os.Args[1] == outputWorkerArg {
		os.Exit(runOutputWorker())
	}
	os.Exit(m.Run())
}

func TestExportProcessHelper(t *testing.T) {
	if os.Getenv("BOG_EXPORT_TEST_PROCESS") == "1" {
		os.Args = append(os.Args[:1], os.Args[3:]...)
		main()
	}
}

func exportProcess(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, os.Args[0], append([]string{"-test.run=^TestExportProcessHelper$", "--"}, args...)...)
	cmd.Env = append(cmd.Environ(), "BOG_EXPORT_TEST_PROCESS=1", "GORACE=atexit_sleep_ms=0")
	cmd.WaitDelay = time.Second
	return cmd
}

func exitStatus(t *testing.T, err error) int {
	t.Helper()
	if err == nil {
		return exitOK
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() == -1 {
		t.Fatalf("process did not exit normally: %v", err)
	}
	return exitErr.ExitCode()
}

func TestExportProcessSIGTERM(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	continuation := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") == "1" {
			servePage(t, w, cursorFor(1), rowA)
			return
		}
		select {
		case continuation <- struct{}{}:
		case <-r.Context().Done():
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	cmd := exportProcess(ctx, argsFor(server.URL)...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-continuation:
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Error(err)
			cancel()
		}
	case <-ctx.Done():
		t.Error("child never requested the continuation")
	}
	status := exitStatus(t, cmd.Wait())
	assertResult(t, status, stdout.String(), stderr.String(), exitIncomplete, rowA, "terminated signal received")
}

func TestExportProcessBrokenPipe(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		servePage(t, w, "", rowA)
	}))
	defer server.Close()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := writer.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exportProcess(ctx, argsFor(server.URL)...)
	var stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = writer, &stderr
	if status := exitStatus(t, cmd.Run()); status != exitIncomplete || !strings.Contains(stderr.String(), "write stdout") {
		t.Errorf("exit = %d, stderr = %q", status, stderr.String())
	}
}

func TestExportProcessLargeRecord(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	row := `{"ts":"2025-01-01T00:00:02Z","line":"` + strings.Repeat("x", 8*outputChunkSize) + "\"}\n"
	want := rowA + row + row
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		servePage(t, w, "", want)
	}))
	defer server.Close()

	cmd := exportProcess(ctx, argsFor(server.URL)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("export: %v, stderr = %q", err, stderr.String())
	}
	if stdout.String() != want || stderr.Len() > 0 {
		t.Errorf("got %d stdout bytes, want %d exact bytes, stderr = %q", stdout.Len(), len(want), stderr.String())
	}
}
