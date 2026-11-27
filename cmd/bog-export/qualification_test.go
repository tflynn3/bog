//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"bog/internal/client"
)

const (
	exportEarly    = `{"ts":"2025-01-01T00:00:00Z","line":"match + early"}` + "\n"
	exportFirst    = exportEarly + rowA
	exportSecond   = rowB + rowA
	exportExpected = exportFirst + exportSecond + rowC
	// Append in a different order from the query result. The repeated row
	// straddles the page boundary, with a different tied row in between.
	exportSeed = rowC + rowA + rowB + exportEarly + rowA +
		`{"ts":"2025-01-01T00:00:01Z","line":"match-but-no-literal"}` + "\n" +
		`{"ts":"2025-01-01T00:00:10Z","line":"match + outside"}` + "\n"
	exportBackfill = `{"ts":"2025-01-01T00:00:00Z","line":"match + backfill"}` + "\n"
)

type exportInterruption struct {
	name, diagnostic, workerMode string
	interruptRequest             int
	interruptSync                bool
	wantAcknowledged             int
	wantOutput                   string
}

var exportInterruptions = []exportInterruption{
	{name: "during page delivery", diagnostic: "terminated signal received", interruptRequest: 2, wantAcknowledged: 1, wantOutput: exportFirst},
	{name: "partial output record", diagnostic: "write stdout (page 2, row 2)", workerMode: "partial fourth record",
		wantAcknowledged: 1, wantOutput: exportFirst + rowB + rowA[:partialRecordBytes]},
	{name: "failed output sync", diagnostic: "sync stdout (page 2)", workerMode: "fail sync",
		wantAcknowledged: 1, wantOutput: exportFirst + exportSecond},
	{name: "after output sync", diagnostic: "terminated signal received", workerMode: "hold sync",
		interruptSync: true, wantAcknowledged: 1, wantOutput: exportFirst + exportSecond},
	{name: "after checkpoint publication", diagnostic: "terminated signal received", interruptRequest: 3,
		wantAcknowledged: 2, wantOutput: exportFirst + exportSecond},
}

func TestCheckpointInterruptionQualification(t *testing.T) {
	for _, tc := range exportInterruptions {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first, second := cursorFor(81), cursorFor(82)
			backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Query().Get("cursor") {
				case "":
					servePage(t, w, first, exportFirst)
				case first:
					servePage(t, w, second, exportSecond)
				case second:
					servePage(t, w, "", rowC)
				default:
					t.Error("unexpected cursor")
					http.Error(w, "invalid cursor", http.StatusBadRequest)
				}
			})
			qualifyExportInterruption(t, tc, backend, nil)
		})
	}
}

type exportQueryTrace struct {
	mu      sync.Mutex
	cursors []string
}

func (s *exportQueryTrace) handler(t *testing.T, backend http.Handler, interruptRequest int, gate chan<- struct{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		token := q.Get("cursor")
		want := url.Values{"start": {start}, "end": {end}, "limit": {"2"}, "contains": {"match + "}}
		if token == "" {
			want.Set("page", "1")
		} else {
			want.Set("cursor", token)
		}
		if r.Method != http.MethodGet || r.URL.Path != "/query" || q.Encode() != want.Encode() {
			t.Errorf("unexpected export request: %s %s", r.Method, r.URL)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.cursors = append(s.cursors, token)
		attempt := len(s.cursors)
		s.mu.Unlock()

		if attempt != interruptRequest {
			backend.ServeHTTP(w, r)
			return
		}
		if attempt == 3 {
			// Requesting page 3 proves page 2's checkpoint was published.
			close(gate)
			<-r.Context().Done()
			return
		}
		// Obtain page 2 from the issuer and put a complete row plus part
		// of the next row on the wire. Neither can be delivered to the sink.
		page := httptest.NewRecorder()
		backend.ServeHTTP(page, r)
		if page.Code != http.StatusOK || page.Body.String() != exportSecond {
			t.Errorf("second page before interruption: status %d, body %q", page.Code, page.Body.String())
			http.Error(w, "invalid fixture page", http.StatusInternalServerError)
			return
		}
		for key, values := range page.Header() {
			w.Header()[key] = values
		}
		w.Header().Set("Content-Length", fmt.Sprint(page.Body.Len()))
		if _, err := w.Write(page.Body.Bytes()[:len(rowB)+5]); err != nil {
			t.Errorf("write interrupted page: %v", err)
			return
		}
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("flush interrupted page: %v", err)
			return
		}
		close(gate)
		<-r.Context().Done()
	})
}

func (s *exportQueryTrace) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.cursors)
}

type exportQualification struct {
	path   string
	output *os.File
	base   string
}

func newExportQualification(t *testing.T, base string) exportQualification {
	t.Helper()
	dir := t.TempDir()
	output := openAppendSink(t, filepath.Join(dir, "rows.ndjson"))
	if err := output.Sync(); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(parent.Sync(), parent.Close()); err != nil {
		t.Fatal(err)
	}
	return exportQualification{path: filepath.Join(dir, "progress"), output: output, base: base}
}

func (f exportQualification) args(extra ...string) []string {
	args := argsFor(f.base, "-checkpoint", f.path, "-limit", "2", "-contains", "match + ", "-retry-delay", "0s")
	return append(args, extra...)
}

func (f exportQualification) run(t *testing.T, ctx context.Context, wantStatus int, diagnostic string, extra ...string) {
	t.Helper()
	var stderr bytes.Buffer
	cmd := exportProcess(ctx, f.args(extra...)...)
	cmd.Stdout, cmd.Stderr = f.output, &stderr
	if code := exitStatus(t, cmd.Run()); code != wantStatus ||
		(diagnostic == "" && stderr.Len() != 0) ||
		(diagnostic != "" && !strings.Contains(stderr.String(), diagnostic)) {
		t.Fatalf("export %v: exit %d, stderr %q, want exit %d and diagnostic %q", extra, code, stderr.String(), wantStatus, diagnostic)
	}
}

func (f exportQualification) interrupt(t *testing.T, ctx context.Context, tc exportInterruption, requestGate <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	dir := filepath.Dir(f.path)
	log := filepath.Join(dir, "workers")
	marker := filepath.Join(dir, "sync-gate")
	cmd := exportProcess(ctx, f.args("-max-pages", "3")...)
	cmd.Env = append(cmd.Env, testWorkerMode+"="+tc.workerMode, testSyncNumber+"=2",
		testSyncMarker+"="+marker, testWorkerLog+"="+log)
	var stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = f.output, &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	gateCtx, stop := context.WithTimeout(ctx, 5*time.Second)
	var gateErr error
	switch {
	case tc.interruptRequest != 0:
		select {
		case <-requestGate:
		case <-gateCtx.Done():
			gateErr = gateCtx.Err()
		}
	case tc.interruptSync:
		gateErr = waitForFile(gateCtx, marker)
	}
	stop()
	var checkpoint []byte
	if gateErr == nil && (tc.interruptRequest != 0 || tc.interruptSync) {
		checkpoint, gateErr = os.ReadFile(f.path)
		if gateErr == nil {
			gateErr = cmd.Process.Signal(syscall.SIGTERM)
		}
	}
	if gateErr != nil {
		cancel()
	}
	waitErr := cmd.Wait()
	if gateErr != nil {
		t.Fatalf("export interruption gate: %v (wait: %v, stderr: %s)", gateErr, waitErr, stderr.String())
	}
	if code := exitStatus(t, waitErr); code != exitIncomplete || !strings.Contains(stderr.String(), tc.diagnostic) {
		t.Fatalf("interrupted export: exit %d, stderr %q, want %q", code, stderr.String(), tc.diagnostic)
	}
	assertWorkersReaped(t, log, 2)
	if checkpoint != nil {
		assertExportArtifact(t, f.path, checkpoint)
	}
}

func qualifyExportInterruption(t *testing.T, tc exportInterruption, backend http.Handler, afterInterrupt func(context.Context)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	var trace exportQueryTrace
	var gate chan struct{}
	if tc.interruptRequest != 0 {
		gate = make(chan struct{})
	}
	front := httptest.NewServer(trace.handler(t, backend, tc.interruptRequest, gate))
	defer front.Close()
	f := newExportQualification(t, front.URL)
	f.interrupt(t, ctx, tc, gate)
	requests := trace.snapshot()
	if len(requests) != max(2, tc.interruptRequest) || requests[0] != "" || requests[1] == "" {
		t.Fatalf("unexpected interrupted request trace %q", requests)
	}
	saved := loadCheckpoint(t, ctx, f.path)
	if saved.AcknowledgedPages != tc.wantAcknowledged || saved.Complete || saved.Opening != nil ||
		saved.CycleAnchor != requests[1] || saved.Cursor == "" ||
		(len(requests) == 3 && saved.Cursor != requests[2]) ||
		(saved.Cursor == requests[1]) != (tc.wantAcknowledged == 1) {
		t.Fatalf("checkpoint at interruption = %+v", saved)
	}
	assertSinkRows(t, f.output.Name(), tc.wantOutput)
	committed := exportFirst
	if tc.wantAcknowledged == 2 {
		committed += exportSecond
	}
	if tc.wantOutput != committed {
		// Exercise the operator workflow using a previously recorded
		// committed byte boundary. Do not infer it from NDJSON framing.
		if err := f.output.Truncate(int64(len(committed))); err != nil {
			t.Fatal(err)
		}
		if err := f.output.Sync(); err != nil {
			t.Fatal(err)
		}
		assertSinkRows(t, f.output.Name(), committed)
	}
	if afterInterrupt != nil {
		afterInterrupt(ctx)
	}
	f.run(t, ctx, exitOK, "", "-resume", "-max-pages", "3")
	assertSinkRows(t, f.output.Name(), exportExpected)
	resumed := trace.snapshot()[len(requests):]
	if len(resumed) != 3-tc.wantAcknowledged || resumed[0] != saved.Cursor ||
		slices.Contains(resumed, "") || (len(resumed) == 2 && resumed[0] == resumed[1]) {
		t.Fatalf("resumed request trace = %q, want replay from saved cursor across remaining pages", resumed)
	}
	done := loadCheckpoint(t, ctx, f.path)
	if !done.Complete || done.AcknowledgedPages != 3 || done.Cursor != "" || done.Opening != nil {
		t.Fatalf("terminal checkpoint = %+v", done)
	}
	finalBytes := readExportArtifact(t, f.path)
	f.run(t, ctx, exitOK, "", "-resume", "-max-pages", "3")
	assertSinkRows(t, f.output.Name(), exportExpected)
	assertExportArtifact(t, f.path, finalBytes)
	if got := trace.snapshot(); len(got) != len(requests)+len(resumed) {
		t.Errorf("completed checkpoint made requests: %q", got)
	}
}

func readExportArtifact(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func assertExportArtifact(t *testing.T, path string, want []byte) {
	t.Helper()
	if got := readExportArtifact(t, path); !bytes.Equal(got, want) {
		t.Fatalf("%s changed (got %d bytes, expected %d)", path, len(got), len(want))
	}
}

func seedExportIssuer(t *testing.T, ctx context.Context, base, data string) {
	t.Helper()
	c, err := client.New(base, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseIdleConnections()
	if outcome, err := c.Ingest(ctx, []byte(data)); err != nil || outcome != client.IngestCommitted {
		t.Fatalf("seed issuer: outcome %v, error %v", outcome, err)
	}
}

func TestCheckpointContinuationRejectionQualification(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		err  error
	}{
		{"expired capture", http.StatusGone, client.ErrCaptureExpired},
		{"invalid cursor", http.StatusBadRequest, client.ErrInvalidCursor},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			var rejected atomic.Bool
			first, second := cursorFor(81), cursorFor(82)
			backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Query().Get("cursor") {
				case "":
					servePage(t, w, first, exportFirst)
				case first:
					if rejected.Load() {
						http.Error(w, tc.err.Error(), tc.code)
						return
					}
					servePage(t, w, second, exportSecond)
				default:
					t.Error("unexpected cursor")
					http.Error(w, "invalid cursor", http.StatusBadRequest)
				}
			})
			issuer := httptest.NewServer(backend)
			defer issuer.Close()
			qualifyRejectedContinuation(t, ctx, issuer.URL, backend, func(string) {
				rejected.Store(true)
			}, tc.code, tc.err)
		})
	}
}

func qualifyRejectedContinuation(t *testing.T, ctx context.Context, issuer string, backend http.Handler,
	invalidate func(string), status int, expected error,
) {
	t.Helper()
	var trace exportQueryTrace
	front := httptest.NewServer(trace.handler(t, backend, 0, nil))
	defer front.Close()
	f := newExportQualification(t, front.URL)
	f.run(t, ctx, exitIncomplete, "after 1 acknowledged pages", "-max-pages", "1")
	saved := loadCheckpoint(t, ctx, f.path)
	if saved.AcknowledgedPages != 1 || saved.Complete || saved.Opening != nil || saved.Cursor == "" {
		t.Fatalf("checkpoint before rejection: %+v", saved)
	}
	checkpointBytes := readExportArtifact(t, f.path)
	assertSinkRows(t, f.output.Name(), exportFirst)
	c, q := qualificationClient(t, issuer)
	page, err := c.FetchPage(ctx, q, saved.Cursor)
	if err != nil || page.NextCursor == "" || len(page.Rows) != 2 {
		t.Fatalf("continuation before invalidation: %d rows, has successor %t, error %v",
			len(page.Rows), page.NextCursor != "", err)
	}
	invalidate(saved.Cursor)
	checkQualificationRejection(t, ctx, issuer, saved.Cursor, status, expected)

	for range 2 {
		f.run(t, ctx, exitIncomplete, fmt.Sprintf("Bog HTTP %d: %s", status, expected), "-resume", "-max-pages", "3")
		assertSinkRows(t, f.output.Name(), exportFirst)
		assertExportArtifact(t, f.path, checkpointBytes)
	}
	if got := trace.snapshot(); !slices.Equal(got, []string{"", saved.Cursor, saved.Cursor}) {
		t.Errorf("rejected request trace = %q, want one opening and one attempt per resume with the saved cursor", got)
	}
}

func qualificationClient(t *testing.T, base string) (*client.Client, client.Query) {
	t.Helper()
	c, err := client.New(base, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseIdleConnections)
	opts, err := parseOptions(argsFor(base, "-limit", "2", "-contains", "match + "))
	if err != nil {
		t.Fatal(err)
	}
	return c, opts.query
}

func checkQualificationRejection(t *testing.T, ctx context.Context, base, cursor string, code int, expected error) {
	t.Helper()
	c, q := qualificationClient(t, base)
	page, err := c.FetchPage(ctx, q, cursor)
	var status *client.HTTPError
	if !errors.Is(err, expected) || !errors.As(err, &status) || status.StatusCode != code ||
		status.Body != expected.Error()+"\n" || status.Retryable() ||
		status.CursorRejected != (expected == client.ErrInvalidCursor) ||
		errors.Is(err, client.ErrCaptureExpired) != (expected == client.ErrCaptureExpired) ||
		page.Rows != nil || page.NextCursor != "" {
		t.Fatalf("rejected continuation = %+v, %v, want HTTP %d and %v", page, err, code, expected)
	}
}
