//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"bog/internal/client"
)

func TestResumeAcrossPageAcknowledgment(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		holdSync       bool
		wantCheckpoint int
		wantRows       string
		wantResumes    int32
	}{
		{name: "before acknowledgment", holdSync: true, wantRows: rowA + rowA + rowB, wantResumes: 1},
		{name: "after acknowledgment", wantCheckpoint: 1, wantRows: rowA + rowB, wantResumes: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			dir := t.TempDir()
			checkpoint := filepath.Join(dir, "Progress")
			log := filepath.Join(dir, "workers.txt")
			marker := filepath.Join(dir, "synced")
			sink := openAppendSink(t, filepath.Join(dir, "rows.ndjson"))
			continuation := make(chan struct{}, 1)
			var openings, resumes atomic.Int32
			next := cursorFor(19)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch q := r.URL.Query(); {
				case q.Get("page") == "1" && !q.Has("cursor"):
					openings.Add(1)
					servePage(t, w, next, rowA)
				case !q.Has("page") && q.Get("cursor") == next:
					if resumes.Add(1) == 1 && !tc.holdSync {
						continuation <- struct{}{}
						<-r.Context().Done()
						return
					}
					servePage(t, w, "", rowB)
				default:
					t.Errorf("unexpected query: %s", r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()

			args := argsFor(server.URL, "-checkpoint", checkpoint)
			cmd := exportProcess(ctx, args...)
			cmd.Env = append(cmd.Env, testWorkerLog+"="+log)
			if tc.holdSync {
				cmd.Env = append(cmd.Env, testWorkerMode+"=hold sync", testSyncMarker+"="+marker)
			}
			var stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = sink, &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			var gateErr error
			if tc.holdSync {
				gateErr = waitForFile(ctx, marker)
			} else {
				select {
				case <-continuation:
				case <-ctx.Done():
					gateErr = ctx.Err()
				}
			}
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				gateErr = errors.Join(gateErr, err)
				cancel()
			}
			waitErr := cmd.Wait()
			if gateErr != nil {
				t.Fatalf("interrupt export: %v (wait: %v, stderr: %s)", gateErr, waitErr, stderr.String())
			}
			if status := exitStatus(t, waitErr); status != exitIncomplete || !strings.Contains(stderr.String(), "terminated signal received") {
				t.Fatalf("interrupted export = %d, stderr = %q", status, stderr.String())
			}
			assertWorkersReaped(t, log, 2)
			assertSinkRows(t, sink.Name(), rowA)
			saved := loadCheckpoint(t, ctx, checkpoint)
			if saved.AcknowledgedPages != tc.wantCheckpoint || saved.Complete {
				t.Fatalf("checkpoint after interruption = %+v", saved)
			}
			if tc.holdSync && (saved.Opening == nil || saved.Opening.NextCursor != next || saved.Cursor != "") {
				t.Fatalf("opening page was not preserved: %+v", saved)
			}
			if !tc.holdSync && (saved.Opening != nil || saved.Cursor != next) {
				t.Fatalf("continuation was not preserved: %+v", saved)
			}

			resumeArgs := argsFor(server.URL, "-checkpoint", checkpointCaseAlias(t, checkpoint), "-resume")
			for _, run := range []string{"resume", "already complete"} {
				cmd := exportProcess(ctx, resumeArgs...)
				cmd.Stdout, cmd.Stderr = sink, &stderr
				stderr.Reset()
				if err := cmd.Run(); err != nil || stderr.Len() != 0 {
					t.Fatalf("%s: %v, stderr = %q", run, err, stderr.String())
				}
				assertSinkRows(t, sink.Name(), tc.wantRows)
			}
			saved = loadCheckpoint(t, ctx, checkpoint)
			if !saved.Complete || saved.AcknowledgedPages != 2 || saved.Cursor != "" || saved.Opening != nil {
				t.Errorf("terminal checkpoint = %+v", saved)
			}
			if openings.Load() != 1 || resumes.Load() != tc.wantResumes {
				t.Errorf("opening requests = %d, continuation requests = %d", openings.Load(), resumes.Load())
			}
		})
	}
}

func loadCheckpoint(t *testing.T, ctx context.Context, path string) client.ExportProgress {
	t.Helper()
	store, err := client.NewFileExportProgressStore(path)
	if err != nil {
		t.Fatal(err)
	}
	progress, found, err := store.Load(ctx, client.DefaultMaxPage)
	if err != nil || !found {
		t.Fatalf("load checkpoint: found=%t, err=%v", found, err)
	}
	return progress
}

func waitForFile(ctx context.Context, path string) error {
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-poll.C:
		}
	}
}

func TestCheckpointExcludesConcurrentExport(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	dir := t.TempDir()
	path := filepath.Join(dir, "Progress")
	probe := filepath.Join(dir, "Probe")
	if err := os.WriteFile(probe, nil, 0600); err != nil {
		t.Fatal(err)
	}
	competingPath := path
	if checkpointCaseAlias(t, probe) != probe {
		competingPath = filepath.Join(dir, "progress")
	}
	out := openAppendSink(t, filepath.Join(dir, "rows.ndjson"))
	requested := make(chan struct{})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) != 1 {
			t.Error("second export made an HTTP request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		close(requested)
		<-r.Context().Done()
	}))
	defer server.Close()
	args := argsFor(server.URL, "-checkpoint", path)
	first := exportProcess(ctx, args...)
	var firstStderr, secondStderr bytes.Buffer
	first.Stdout, first.Stderr = out, &firstStderr
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			cancel()
			if err := first.Wait(); err == nil {
				t.Error("interrupted first export unexpectedly succeeded")
			}
		}
	}()
	select {
	case <-requested:
	case <-ctx.Done():
		t.Fatal("first export never reached the query")
	}
	second := exportProcess(ctx, argsFor(server.URL, "-checkpoint", competingPath)...)
	second.Stdout, second.Stderr = out, &secondStderr
	if status := exitStatus(t, second.Run()); status != exitIncomplete || !strings.Contains(secondStderr.String(), "lock export checkpoint") {
		t.Errorf("second export exit = %d, stderr = %q", status, secondStderr.String())
	}
	if err := first.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	err := first.Wait()
	waited = true
	if status := exitStatus(t, err); status != exitIncomplete {
		t.Errorf("first export exit = %d, stderr = %q", status, firstStderr.String())
	}
}
