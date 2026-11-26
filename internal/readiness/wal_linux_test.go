package readiness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/internal/wal"
)

const testBatch = `{"ts":"2025-01-01T00:00:00Z","line":"private row"}`

type appendFunc func([]byte, []store.Row) error

func (f appendFunc) Append(payload []byte, rows []store.Row) error {
	return f(payload, rows)
}

func request(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}

func assertReady(t *testing.T, h http.Handler, code int, want status) {
	t.Helper()
	rec := request(h, http.MethodGet, "/ready", "")
	var got status
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode /ready: %v (%q)", err, rec.Body.String())
	}
	if rec.Code != code || got != want {
		t.Fatalf("/ready = %d %+v, want %d %+v", rec.Code, got, code, want)
	}
	if rec.Header().Get("Content-Type") != "application/json" || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("unexpected /ready headers: %v", rec.Header())
	}
}

func TestReadyRouteWithAppendResults(t *testing.T) {
	s := &store.Store{}
	dir := filepath.Join(t.TempDir(), "data")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	w, err := wal.Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	})
	privateErr := fmt.Errorf("private path: %w", syscall.ENOSPC)
	var failure error
	log := appendFunc(func(payload []byte, rows []store.Row) error {
		if failure != nil {
			return failure
		}
		return w.Append(payload, rows)
	})
	m := &monitor{sample: sample{at: time.Now(), headroom: true}}
	h := handlerForMonitor(m, log, func(w Appender) http.Handler { return httpapi.New(s, w) })
	assertReady(t, h, http.StatusOK, status{Ready: true, Reason: "ready"})

	failure = privateErr
	if rec := request(h, http.MethodPost, "/ingest", testBatch); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("failed ingest = %d %s", rec.Code, rec.Body)
	}
	assertReady(t, h, http.StatusServiceUnavailable, status{Reason: "wal_rollback", Category: "enospc"})
	if rec := request(h, http.MethodPost, "/ingest", "invalid batch"); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid ingest during rollback = %d %s", rec.Code, rec.Body)
	}
	assertReady(t, h, http.StatusServiceUnavailable, status{Reason: "wal_rollback", Category: "enospc"})
	rec := request(h, http.MethodGet, "/ready", "")
	if bytes.Contains(rec.Body.Bytes(), []byte("private")) {
		t.Fatalf("status leaked input: %s", rec.Body)
	}
	if rec := request(h, http.MethodGet, "/healthz", ""); rec.Code != http.StatusOK {
		t.Fatalf("liveness during rollback = %d", rec.Code)
	}

	failure = nil
	if rec := request(h, http.MethodPost, "/ingest", testBatch); rec.Code != http.StatusNoContent {
		t.Fatalf("recovery ingest = %d %s", rec.Code, rec.Body)
	}
	assertReady(t, h, http.StatusOK, status{Ready: true, Reason: "ready"})
	if rec := request(h, http.MethodPost, "/ready", ""); rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET" {
		t.Fatalf("POST /ready = %d, headers %v", rec.Code, rec.Header())
	}
	if rec := request(h, http.MethodGet, "/ready/extra", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("GET /ready/extra = %d", rec.Code)
	}

	failure = fmt.Errorf("private path: %w", wal.ErrUncertain)
	// Exercise the adapter directly since the ingest handler exits the
	// process on an uncertain commit.
	if err := (&observedAppender{wal: log, monitor: m}).Append(nil, nil); !errors.Is(err, wal.ErrUncertain) {
		t.Fatalf("uncertainty lost: %v", err)
	}
	assertReady(t, h, http.StatusServiceUnavailable, status{Reason: "recovery_required", Category: "other"})
}

func TestReadyRecoversWithQueuedAppend(t *testing.T) {
	rollbackStarted := make(chan struct{})
	releaseRollback := make(chan struct{})
	durableStarted := make(chan struct{})
	releaseDurable := make(chan struct{})
	log := appendFunc(func(payload []byte, _ []store.Row) error {
		if string(payload) == "rollback" {
			close(rollbackStarted)
			<-releaseRollback
			return syscall.ENOSPC
		}
		close(durableStarted)
		<-releaseDurable
		return nil
	})
	var appender Appender
	h := handlerForMonitor(&monitor{}, log, func(a Appender) http.Handler {
		appender = a
		return http.NotFoundHandler()
	})
	rollbackDone := make(chan error, 1)
	go func() { rollbackDone <- appender.Append([]byte("rollback"), nil) }()
	<-rollbackStarted

	queued := make(chan struct{})
	durableDone := make(chan error, 1)
	go func() {
		close(queued)
		durableDone <- appender.Append([]byte("durable"), nil)
	}()
	<-queued
	var overlapped bool
	select {
	case <-durableStarted:
		overlapped = true
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseRollback)
	rollbackErr := <-rollbackDone
	before := request(h, http.MethodGet, "/ready", "")
	close(releaseDurable)
	if err := <-durableDone; err != nil {
		t.Fatal(err)
	}
	if !errors.Is(rollbackErr, syscall.ENOSPC) || before.Code != http.StatusServiceUnavailable {
		t.Fatalf("rollback error = %v, /ready = %d %s", rollbackErr, before.Code, before.Body)
	}
	assertReady(t, h, http.StatusOK, status{Ready: true, Reason: "ready"})
	if overlapped {
		t.Fatal("fallback WAL calls overlapped, allowing outcomes to be observed out of order")
	}
}

type outcomeAppender struct {
	latest func() wal.CommitOutcome
}

func (*outcomeAppender) Append([]byte, []store.Row) error {
	return errors.New("pre-admission failure")
}

func (w *outcomeAppender) LastCommitOutcome() wal.CommitOutcome {
	return w.latest()
}

func TestReadyRouteWithWALOutcomeSnapshot(t *testing.T) {
	var commit wal.CommitOutcome
	w := &outcomeAppender{latest: func() wal.CommitOutcome { return commit }}
	m := &monitor{sample: sample{at: time.Now(), headroom: true}}
	h := handlerForMonitor(m, w, func(appender Appender) http.Handler {
		if appender != w {
			t.Fatal("WAL with an outcome snapshot was wrapped")
		}
		return http.NotFoundHandler()
	})
	assertReady(t, h, http.StatusOK, status{Ready: true, Reason: "ready"})
	tests := []struct {
		state    wal.CommitState
		category string
		code     int
		want     status
	}{
		{wal.CommitRolledBack, "enospc", http.StatusServiceUnavailable, status{Reason: "wal_rollback", Category: "enospc"}},
		{wal.CommitDurable, "", http.StatusOK, status{Ready: true, Reason: "ready"}},
		{wal.CommitUncertain, "private diagnostic", http.StatusServiceUnavailable, status{Reason: "recovery_required", Category: "other"}},
		{wal.CommitDurable, "", http.StatusServiceUnavailable, status{Reason: "recovery_required", Category: "other"}},
	}
	for _, tt := range tests {
		commit = wal.CommitOutcome{State: tt.state, At: time.Now(), Category: tt.category}
		assertReady(t, h, tt.code, tt.want)
	}
}

func TestHeadroom(t *testing.T) {
	for _, tt := range []struct {
		name string
		fs   syscall.Statfs_t
		want bool
	}{
		{"two available blocks and one free inode", syscall.Statfs_t{Bavail: 2, Files: 10, Ffree: 1}, true},
		{"reserved blocks are unavailable", syscall.Statfs_t{Bavail: 1, Bfree: 10}, false},
		{"no free inodes", syscall.Statfs_t{Bavail: 2, Files: 10}, false},
		{"inode count unreported", syscall.Statfs_t{Bavail: 2}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasHeadroom(tt.fs); got != tt.want {
				t.Errorf("hasHeadroom(%+v) = %t, want %t", tt.fs, got, tt.want)
			}
		})
	}
}

func TestReadySamplesOpenedDirectoryAfterRetarget(t *testing.T) {
	active := filepath.Join(t.TempDir(), "active")
	if err := os.Mkdir(active, 0700); err != nil {
		t.Fatal(err)
	}
	var activeFS, targetFS syscall.Statfs_t
	if err := syscall.Statfs(active, &activeFS); err != nil {
		t.Fatal(err)
	}
	// procfs reports zero allocatable blocks without requiring a mount or
	// filling another filesystem to distinguish the two readiness results.
	if err := syscall.Statfs("/proc", &targetFS); err != nil {
		t.Skipf("procfs unavailable: %v", err)
	}
	if activeFS.Type == targetFS.Type || !hasHeadroom(activeFS) || hasHeadroom(targetFS) {
		t.Skip("test requires a writable temp filesystem and procfs with no allocatable blocks")
	}
	link := filepath.Join(t.TempDir(), "data")
	if err := os.Symlink(active, link); err != nil {
		t.Fatal(err)
	}
	log, err := wal.Open(link, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := log.Close(); err != nil {
			t.Error(err)
		}
	})
	dir, err := os.Open(link)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := dir.Close(); err != nil {
			t.Error(err)
		}
	})
	replacement := link + ".new"
	if err := os.Symlink("/proc", replacement); err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		newAPI := func(appender Appender) http.Handler {
			if appender != log {
				t.Fatal("WAL with typed outcomes was wrapped")
			}
			return http.NotFoundHandler()
		}
		pinned := NewHandlerForDirectory(ctx, dir, log, newAPI)
		byPath := NewHandler(ctx, link, log, newAPI)
		synctest.Wait()
		ready := status{Ready: true, Reason: "ready"}
		assertReady(t, pinned, http.StatusOK, ready)
		assertReady(t, byPath, http.StatusOK, ready)

		if err := os.Rename(replacement, link); err != nil {
			t.Fatal(err)
		}
		time.Sleep(maxEvidenceAge + sampleInterval)
		synctest.Wait()
		assertReady(t, pinned, http.StatusOK, ready)
		assertReady(t, byPath, http.StatusServiceUnavailable, status{Reason: "low_disk_headroom"})
	})
}

func TestReadyReportsClosedDirectory(t *testing.T) {
	dir, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := dir.Close(); err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		h := NewHandlerForDirectory(t.Context(), dir, appendFunc(nil), func(Appender) http.Handler { return http.NotFoundHandler() })
		synctest.Wait()
		assertReady(t, h, http.StatusServiceUnavailable, status{Reason: "sample_unavailable"})
	})
}

func TestHTTPDuringBlockedProbeAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		release := make(chan struct{}, 1)
		defer close(release)
		defer cancel()
		var calls atomic.Int32
		h := newHandler(ctx, func(fs *syscall.Statfs_t) error {
			calls.Add(1)
			<-release
			fs.Bavail = 2
			return nil
		}, &outcomeAppender{latest: func() wal.CommitOutcome { return wal.CommitOutcome{} }}, func(Appender) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})
		})
		synctest.Wait()
		if got := calls.Load(); got != 1 {
			t.Fatalf("initial probe calls = %d, want 1", got)
		}
		pending := status{Reason: "sampling_pending"}
		assertReady(t, h, http.StatusServiceUnavailable, pending)
		if got := request(h, http.MethodGet, "/healthz", "").Code; got != http.StatusNoContent {
			t.Fatalf("delegated HTTP status = %d", got)
		}
		cancel()
		assertReady(t, h, http.StatusServiceUnavailable, pending)
		release <- struct{}{}
		time.Sleep(2 * sampleInterval)
		synctest.Wait()
		if got := calls.Load(); got != 1 {
			t.Fatalf("probe calls after cancellation = %d, want 1", got)
		}
		assertReady(t, h, http.StatusServiceUnavailable, pending)
	})
}

func TestConcurrentWALObservationsKeepUncertainty(t *testing.T) {
	now := time.Now()
	var calls atomic.Int32
	captured := make(chan struct{})
	release := make(chan struct{}, 1)
	defer close(release)
	w := &outcomeAppender{latest: func() wal.CommitOutcome {
		if calls.Add(1) == 1 {
			close(captured)
			<-release
			return wal.CommitOutcome{State: wal.CommitUncertain, At: now.Add(-time.Second), Category: "eio"}
		}
		return wal.CommitOutcome{State: wal.CommitDurable, At: now}
	}}
	h := handlerForMonitor(&monitor{}, w, func(Appender) http.Handler { return http.NotFoundHandler() })
	uncertainResponse := make(chan *httptest.ResponseRecorder, 1)
	go func() { uncertainResponse <- request(h, http.MethodGet, "/ready", "") }()
	<-captured
	assertReady(t, h, http.StatusOK, status{Ready: true, Reason: "ready"})

	var readers sync.WaitGroup
	for range 32 {
		readers.Go(func() { request(h, http.MethodGet, "/ready", "") })
	}
	release <- struct{}{}
	if rec := <-uncertainResponse; rec.Code != http.StatusServiceUnavailable {
		t.Errorf("uncertain outcome returned %d %s", rec.Code, rec.Body)
	}
	readers.Wait()
	assertReady(t, h, http.StatusServiceUnavailable, status{Reason: "recovery_required", Category: "eio"})
}
