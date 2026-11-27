package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestExportProcessDeadlineOnInheritedPipe(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		large := `{"ts":"2025-01-01T00:00:02Z","line":"` + strings.Repeat("x", 2<<20) + "\"}\n"
		servePage(t, w, "", rowA+large)
	}))
	defer server.Close()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range []*os.File{reader, writer} {
		defer func() {
			if err := file.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	cmd := exportProcess(ctx, argsFor(server.URL, "-timeout", "1s")...)
	var stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = writer, &stderr
	// Leave the reader open and undrained so an inherited blocking stdout
	// would hang until the test's process watchdog killed it.
	if status := exitStatus(t, cmd.Run()); status != exitIncomplete || !strings.Contains(stderr.String(), "write stdout") {
		t.Errorf("exit = %d, stderr = %q", status, stderr.String())
	}
}
