package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"bog/internal/client"
)

func TestCheckpointStartAndResumeValidation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "progress")
	out := openAppendSink(t, filepath.Join(dir, "rows"))
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		servePage(t, w, "", rowA)
	}))
	defer server.Close()
	args := argsFor(server.URL, "-checkpoint", path)
	for _, tc := range []struct {
		name, diagnostic string
		extra            []string
		wantStatus       int
	}{
		{"missing", "checkpoint does not exist", []string{"-resume"}, exitIncomplete},
		{"start", "", nil, exitOK},
		{"existing", "checkpoint already exists", nil, exitIncomplete},
		{"changed query", client.ErrExportMismatch.Error(), []string{"-resume", "-limit", "25"}, exitIncomplete},
		{"complete", "", []string{"-resume"}, exitOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stderr bytes.Buffer
			status := run(t.Context(), append(args, tc.extra...), out, &stderr)
			assertResult(t, status, "", stderr.String(), tc.wantStatus, "", tc.diagnostic)
		})
	}
	if requests.Load() != 1 {
		t.Errorf("requests = %d, want one terminal query", requests.Load())
	}
	assertSinkRows(t, out.Name(), rowA)
}

func TestCheckpointOutputValidation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	appendFile := openAppendSink(t, filepath.Join(dir, "append"))
	plainFile, err := os.Create(filepath.Join(dir, "plain"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := plainFile.Close(); err != nil {
			t.Error(err)
		}
	}()
	alias := filepath.Join(dir, "alias")
	if err := os.Link(appendFile.Name(), alias); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, path string
		out        io.Writer
		diagnostic string
	}{
		{"memory sink", filepath.Join(dir, "progress"), &bytes.Buffer{}, "stdout opened for append"},
		{"overwrite sink", filepath.Join(dir, "progress"), plainFile, "stdout opened for append"},
		{"checkpoint aliases sink", alias, appendFile, "separate from stdout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			progress, err := openCheckpoint(tc.path, false, tc.out)
			if progress != nil {
				err = errors.Join(err, progress.close())
			}
			if err == nil || !strings.Contains(err.Error(), tc.diagnostic) {
				t.Errorf("open checkpoint = %v, want %q", err, tc.diagnostic)
			}
		})
	}
}

func TestCheckpointRetriesFetchesPerPage(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	next := cursorFor(25)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		q := r.URL.Query()
		if attempt <= 2 && (q.Get("page") != "1" || q.Has("cursor")) ||
			attempt >= 3 && (q.Has("page") || q.Get("cursor") != next) {
			t.Errorf("wrong query on attempt %d: %s", attempt, r.URL)
		}
		switch attempt {
		case 1:
			http.Error(w, "busy", http.StatusServiceUnavailable)
		case 2:
			servePage(t, w, next, rowA)
		case 3:
			w.Header().Set("Content-Length", fmt.Sprint(len(rowB)+len(rowC)))
			servePage(t, w, "", rowB)
		case 4:
			servePage(t, w, "", rowB+rowC)
		default:
			t.Errorf("unexpected query attempt %d", attempt)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	dir := t.TempDir()
	checkpoint := filepath.Join(dir, "progress")
	out := openAppendSink(t, filepath.Join(dir, "rows"))
	var stderr bytes.Buffer
	args := argsFor(server.URL, "-checkpoint", checkpoint, "-max-retries", "1", "-retry-delay", "0s", "-max-pages", "2")
	status := run(t.Context(), args, out, &stderr)
	assertResult(t, status, "", stderr.String(), exitOK, "", "")
	assertSinkRows(t, out.Name(), rowA+rowB+rowC)
	if attempts.Load() != 4 {
		t.Errorf("attempts = %d, want 4", attempts.Load())
	}
}

func TestRetryableExportFetch(t *testing.T) {
	t.Parallel()
	busy := &client.HTTPError{StatusCode: 503, QueryRetryable: true}
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{nil, false},
		{client.ErrPageInterrupted, true},
		{busy, true},
		{errors.Join(busy, context.Canceled), false},
		{errors.Join(busy, context.DeadlineExceeded), false},
		{errors.Join(busy, client.ErrMalformedPage), false},
		{errors.Join(busy, client.ErrPageBytes), false},
		{client.ErrCaptureExpired, false},
		{io.ErrShortWrite, false},
	} {
		if got := retryableExportFetch(tc.err); got != tc.want {
			t.Errorf("retryableExportFetch(%v) = %t, want %t", tc.err, got, tc.want)
		}
	}
}

func openAppendSink(t *testing.T, path string) *os.File {
	t.Helper()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Error(err)
		}
	})
	return file
}

func assertSinkRows(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != want {
		t.Errorf("sink contains %q, want %q", data, want)
	}
}

func TestCheckpointRejectsLegacyStageLinkedToStdout(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	original := filepath.Join(dir, "Progress")
	seed := filepath.Join(dir, "seed")
	store, err := client.NewFileExportProgressStore(seed)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(t.Context(), client.ExportProgress{
		Version: client.ExportProgressVersion, AcknowledgedPages: 1, Complete: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(seed, original); err != nil {
		t.Fatal(err)
	}
	alias := checkpointCaseAlias(t, original)
	out := openAppendSink(t, filepath.Join(dir, "rows"))
	stage := filepath.Join(dir, fmt.Sprintf(".export-progress-stage-%x", sha256.Sum256([]byte("Progress"))))
	if err := os.Link(out.Name(), stage); err != nil {
		t.Fatal(err)
	}
	progress, err := openCheckpoint(alias, true, out)
	if progress != nil {
		err = errors.Join(err, progress.close())
	}
	if err == nil || !strings.Contains(err.Error(), "separate from stdout") {
		t.Fatalf("open checkpoint with a stage linked to stdout: %v", err)
	}
	if _, err := os.Lstat(stage); err != nil {
		t.Fatalf("stage changed during stdout validation: %v", err)
	}
}

func checkpointCaseAlias(t *testing.T, path string) string {
	t.Helper()
	alias := filepath.Join(filepath.Dir(path), strings.ToLower(filepath.Base(path)))
	info, err := os.Lstat(alias)
	if errors.Is(err, os.ErrNotExist) {
		return path
	}
	if err != nil {
		t.Fatal(err)
	}
	orig, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(info, orig) {
		return alias
	}
	return path
}
