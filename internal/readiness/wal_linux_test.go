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
	"syscall"
	"testing"
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

type commitState33 string

type commitOutcome33 struct {
	State     commitState33
	At        time.Time
	Operation string
	Category  string
}

type wal33 struct {
	commit commitOutcome33
}

func (*wal33) Append([]byte, []store.Row) error {
	return errors.New("pre-admission failure")
}

func (w *wal33) LastCommitOutcome() commitOutcome33 {
	return w.commit
}

func TestReadyRouteWithWALOutcomeSnapshot(t *testing.T) {
	w := &wal33{}
	m := &monitor{sample: sample{at: time.Now(), headroom: true}}
	h := handlerForMonitor(m, w, func(appender Appender) http.Handler {
		if appender != w {
			t.Fatal("WAL with an outcome snapshot was wrapped")
		}
		return http.NotFoundHandler()
	})
	assertReady(t, h, http.StatusOK, status{Ready: true, Reason: "ready"})
	tests := []struct {
		state    commitState33
		category string
		code     int
		want     status
	}{
		{"rolled_back", "enospc", http.StatusServiceUnavailable, status{Reason: "wal_rollback", Category: "enospc"}},
		{"durable", "", http.StatusOK, status{Ready: true, Reason: "ready"}},
		{"uncertain", "private diagnostic", http.StatusServiceUnavailable, status{Reason: "recovery_required", Category: "other"}},
		{"durable", "", http.StatusServiceUnavailable, status{Reason: "recovery_required", Category: "other"}},
	}
	for _, tt := range tests {
		w.commit = commitOutcome33{State: tt.state, At: time.Now(), Category: tt.category}
		assertReady(t, h, tt.code, tt.want)
	}
}

func TestDiskHeadroom(t *testing.T) {
	dir := t.TempDir()
	if _, err := diskHeadroom(dir); err != nil {
		t.Fatalf("stat active data directory: %v", err)
	}
	if _, err := diskHeadroom(dir + "/missing"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat missing data directory: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if h := NewHandler(ctx, dir, appendFunc(nil), func(Appender) http.Handler { return http.NotFoundHandler() }); h == nil {
		t.Fatal("missing handler")
	}
}
