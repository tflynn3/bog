//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestInheritedOutputBackpressure(t *testing.T) {
	t.Parallel()
	for _, stream := range []string{"stdout", "stderr"} {
		for _, stop := range []string{"deadline", "SIGTERM"} {
			t.Run(stream+"/"+stop, func(t *testing.T) {
				t.Parallel()
				checkInheritedBackpressure(t, stream, stop, exportProcess)
			})
		}
	}
}

func checkInheritedBackpressure(t *testing.T, stream, stop string, command func(context.Context, ...string) *exec.Cmd) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	blocked := newInheritedPipe(t)
	completed := rowA + rowB + rowB
	final := `{"ts":"2025-01-01T00:00:02Z","line":"` + strings.Repeat("x", 4<<20) + "\"}\n"
	responseSent := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if stream == "stdout" {
			servePage(t, w, "", completed+final)
			return
		}
		if r.URL.Query().Get("page") == "1" {
			servePage(t, w, cursorFor(1), completed)
			return
		}
		http.Error(w, "fixture query failure", http.StatusBadRequest)
		close(responseSent)
	}))
	defer server.Close()

	timeout := "2s"
	if stop == "SIGTERM" {
		timeout = "30s"
	}
	cmd := command(ctx, argsFor(server.URL, "-timeout", timeout)...)
	var stdout, stderr bytes.Buffer
	var filler int
	if stream == "stdout" {
		cmd.Stdout, cmd.Stderr = blocked.writer, &stderr
	} else {
		filler = blocked.fill(t)
		cmd.Stdout, cmd.Stderr = &stdout, blocked.writer
	}

	flags := descriptorFlags(t, blocked.writer)
	started := time.Now()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = cmd.Wait()
		close(waited)
	}()
	defer func() {
		cancel()
		<-waited
	}()

	var prefix []byte
	if stream == "stdout" {
		// Read the completed records and the start of the final record.
		// Then keep the reader open without accepting the rest.
		prefix = make([]byte, len(completed)+256)
		if err := blocked.reader.SetReadDeadline(time.Now().Add(4 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(blocked.reader, prefix); err != nil {
			t.Fatalf("read initial stdout: %v", err)
		}
	} else {
		select {
		case <-responseSent:
		case <-ctx.Done():
			t.Fatal("exporter never fetched the failing page")
		}
	}

	// The process must remain blocked while the reader stays open. This
	// also gives stderr's worker time to reach its write after the 400.
	select {
	case <-waited:
		t.Fatalf("process exited before cancellation trigger: %v", waitErr)
	case <-time.After(120 * time.Millisecond):
	}
	if got := descriptorFlags(t, blocked.writer); got != flags {
		t.Errorf("inherited descriptor flags during write = %#x, want %#x", got, flags)
	}
	if stop == "SIGTERM" {
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
	}

	<-waited // CommandContext supplies the parent deadline and kills on overrun.
	if ctx.Err() != nil {
		t.Fatalf("parent watchdog expired: %v (wait: %v)", ctx.Err(), waitErr)
	}
	if status := exitStatus(t, waitErr); status != exitIncomplete {
		t.Errorf("exit = %d, want %d", status, exitIncomplete)
	}
	if stop == "deadline" && time.Since(started) < 1900*time.Millisecond {
		t.Error("process exited before the export deadline")
	}
	if got := descriptorFlags(t, blocked.writer); got != flags {
		t.Errorf("inherited descriptor flags after exit = %#x, want %#x", got, flags)
	}
	output := append(prefix, blocked.drain(t)...)
	if stream == "stdout" {
		assertCompletePrefix(t, output, completed, final)
		if diagnostic := stderr.String(); !strings.Contains(diagnostic, "write stdout (page 1, row 4)") {
			t.Errorf("stderr = %q, want interrupted-row diagnostic", diagnostic)
		}
	} else {
		assertCompletePrefix(t, stdout.Bytes(), completed, "")
		if !bytes.Equal(output, bytes.Repeat([]byte{'!'}, filler)) {
			t.Errorf("full stderr pipe changed: got %d bytes, want %d filler bytes", len(output), filler)
		}
	}
}

func assertCompletePrefix(t *testing.T, data []byte, completed, final string) {
	t.Helper()
	before, after, ok := bytes.Cut(data, []byte(completed))
	if !ok || len(before) != 0 {
		t.Fatalf("stdout did not start with the expected %d ordered bytes and duplicates", len(completed))
	}
	for line := range strings.SplitSeq(strings.TrimSuffix(completed, "\n"), "\n") {
		if !json.Valid([]byte(line)) {
			t.Fatalf("completed output row is invalid: %q", line)
		}
	}
	if final == "" {
		if len(after) != 0 {
			t.Errorf("stdout has %d unexpected trailing bytes", len(after))
		}
		return
	}
	if len(after) == 0 || len(after) >= len(final) || !bytes.HasPrefix([]byte(final), after) {
		t.Errorf("stdout has %d bytes after complete rows, want a strict prefix of the final record", len(after))
	}
}

type inheritedPipe struct {
	reader, writer *os.File
	writerClosed   bool
}

func newInheritedPipe(t *testing.T) *inheritedPipe {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	// Go-created pipes may be nonblocking until Fd exposes the write
	// descriptor. Pass realistic blocking stdout/stderr to the exporter.
	writer.Fd()
	pipe := &inheritedPipe{reader: reader, writer: writer}
	t.Cleanup(func() {
		if !pipe.writerClosed {
			if err := writer.Close(); err != nil {
				t.Error(err)
			}
		}
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	})
	return pipe
}

func descriptorFlags(t *testing.T, file *os.File) uintptr {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var flags uintptr
	var flagErr syscall.Errno
	if err := raw.Control(func(fd uintptr) {
		flags, _, flagErr = syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFL, 0)
	}); err != nil {
		t.Fatal(err)
	}
	if flagErr != 0 {
		t.Fatal(flagErr)
	}
	// Darwin also reports FWASWRITTEN after normal writes. Compare the
	// inherited access mode and mutable write flags, which setup must preserve.
	return flags & (syscall.O_ACCMODE | syscall.O_NONBLOCK | syscall.O_APPEND | syscall.O_ASYNC | syscall.O_SYNC)
}

func (pipe *inheritedPipe) fill(t *testing.T) int {
	t.Helper()
	fd := int(pipe.writer.Fd())
	if err := syscall.SetNonblock(fd, true); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := syscall.SetNonblock(fd, false); err != nil {
			t.Error(err)
		}
	}()
	filler := bytes.Repeat([]byte{'!'}, 4096)
	full := 0
	for {
		n, err := syscall.Write(fd, filler)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.EAGAIN) {
			if len(filler) == 1 {
				return full
			}
			filler = filler[:1]
			continue
		}
		if err != nil || n <= 0 {
			t.Fatalf("fill inherited pipe: n=%d, err=%v", n, err)
		}
		full += n
		if full > 8<<20 {
			t.Fatal("inherited pipe never filled")
		}
	}
}

func (pipe *inheritedPipe) drain(t *testing.T) []byte {
	t.Helper()
	pipe.writerClosed = true
	if err := pipe.writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pipe.reader.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(pipe.reader)
	if err != nil {
		t.Fatalf("drain pipe after exporter exit: %v", err)
	}
	return data
}

func TestAlreadyCanceledExportWithFullStderr(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()

	blocked := newInheritedPipe(t)
	filler := blocked.fill(t)
	cmd := exportProcess(ctx, argsFor(server.URL, "-timeout", "300ms")...)
	var stdout bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, blocked.writer
	if status := exitStatus(t, cmd.Run()); status != exitIncomplete || stdout.Len() != 0 {
		t.Errorf("exit = %d, stdout = %q", status, stdout.String())
	}
	if output := blocked.drain(t); !bytes.Equal(output, bytes.Repeat([]byte{'!'}, filler)) {
		t.Errorf("full stderr pipe changed: got %d bytes, want %d filler bytes", len(output), filler)
	}
}
