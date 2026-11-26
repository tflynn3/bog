package readiness

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/internal/wal"
)

const testBatch = `{"ts":"2025-01-01T00:00:00Z","line":"private row"}`

func request(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}

func assertReady(t *testing.T, h http.Handler, code int, want status) {
	t.Helper()
	rec := request(h, http.MethodGet, "/ready", "")
	var got status
	body := rec.Body.String()
	decoder := json.NewDecoder(strings.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&got); err != nil {
		t.Fatalf("decode /ready: %v (%q)", err, body)
	}
	if rec.Code != code || got != want {
		t.Fatalf("/ready = %d %+v, want %d %+v", rec.Code, got, code, want)
	}
	if rec.Header().Get("Content-Type") != "application/json" || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("unexpected /ready headers: %v", rec.Header())
	}
}

func TestReadyWithRealWAL(t *testing.T) {
	s := &store.Store{}
	dir := filepath.Join(t.TempDir(), "data")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	log, err := wal.Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := log.Close(); err != nil {
			t.Error(err)
		}
	})
	h := NewHandler(t.Context(), dir, log, func(appender Appender) http.Handler {
		if appender != log {
			t.Fatal("API did not receive the original WAL")
		}
		return httpapi.New(s, appender)
	})
	assertIngest := func(t *testing.T, body string, code int) {
		t.Helper()
		if rec := request(h, http.MethodPost, "/ingest", body); rec.Code != code {
			t.Fatalf("POST /ingest = %d %s, want %d", rec.Code, rec.Body, code)
		}
	}
	assertIngest(t, testBatch, http.StatusNoContent)
	assertReady(t, h, http.StatusOK, status{Ready: true, Reason: "ready"})

	t.Run("rollback and recovery", func(t *testing.T) {
		// The open WAL remains writable while creating a new commit marker
		// fails before rename, allowing the append group to roll back.
		if err := os.Chmod(dir, 0500); err != nil {
			t.Fatal(err)
		}
		restorePermissions := func() {
			t.Helper()
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
		}
		t.Cleanup(restorePermissions)
		const writeAccess = 2 // Linux access(2) W_OK.
		if err := syscall.Access(dir, writeAccess); err == nil {
			t.Skip("process can write to a read-only directory")
		} else if !errors.Is(err, syscall.EACCES) {
			t.Fatalf("check directory write access: %v", err)
		}
		assertIngest(t, testBatch, http.StatusServiceUnavailable)
		assertReady(t, h, http.StatusServiceUnavailable, status{Reason: "wal_rollback", Category: "other"})
		assertIngest(t, "invalid batch", http.StatusBadRequest)
		assertReady(t, h, http.StatusServiceUnavailable, status{Reason: "wal_rollback", Category: "other"})
		if rec := request(h, http.MethodGet, "/healthz", ""); rec.Code != http.StatusOK {
			t.Fatalf("liveness during rollback = %d", rec.Code)
		}

		restorePermissions()
		assertReady(t, h, http.StatusServiceUnavailable, status{Reason: "wal_rollback", Category: "other"})
		assertIngest(t, testBatch, http.StatusNoContent)
		assertReady(t, h, http.StatusOK, status{Ready: true, Reason: "ready"})
	})
	t.Run("uncertainty", func(t *testing.T) {
		marker := filepath.Join(dir, "bog.commit")
		if err := os.Remove(marker); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(marker, 0700); err != nil {
			t.Fatal(err)
		}
		payload := []byte(testBatch)
		rows, err := store.Decode(payload)
		if err != nil {
			t.Fatal(err)
		}
		// Replacing a directory with a regular marker fails during rename.
		// Append directly because /ingest terminates on an uncertain commit.
		for range 2 {
			if err := log.Append(payload, rows); !errors.Is(err, wal.ErrUncertain) {
				t.Fatalf("append with unusable commit marker = %v, want ErrUncertain", err)
			}
			assertReady(t, h, http.StatusServiceUnavailable, status{Reason: "recovery_required", Category: "other"})
		}
	})

	if rec := request(h, http.MethodPost, "/ready", ""); rec.Code != http.StatusMethodNotAllowed ||
		rec.Header().Get("Allow") != http.MethodGet || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("POST /ready = %d, headers %v", rec.Code, rec.Header())
	}
	if rec := request(h, http.MethodGet, "/ready/extra", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("GET /ready/extra = %d", rec.Code)
	}
}

type outcomeAppender struct {
	append func([]byte, []store.Row) error
	commit wal.CommitOutcome
}

func (a *outcomeAppender) Append(payload []byte, rows []store.Row) error {
	return a.append(payload, rows)
}

func (a *outcomeAppender) LastCommitOutcome() wal.CommitOutcome {
	return a.commit
}

func TestReadyAllowsConcurrentAppends(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan string, 2)
		release := make(chan struct{}, 2)
		defer close(release)
		log := &outcomeAppender{append: func(payload []byte, _ []store.Row) error {
			entered <- string(payload)
			<-release
			return nil
		}}
		h := handlerForMonitor(&monitor{}, log, func(a Appender) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := a.Append([]byte(r.URL.Path), nil); err != nil {
					http.Error(w, err.Error(), http.StatusServiceUnavailable)
					return
				}
				w.WriteHeader(http.StatusNoContent)
			})
		})
		responses := make(chan int, 2)
		for _, path := range []string{"/first", "/second"} {
			go func() { responses <- request(h, http.MethodPost, path, "").Code }()
			select {
			case got := <-entered:
				if got != path {
					t.Fatalf("appender entered with %q, want %q", got, path)
				}
			case <-time.After(time.Second):
				t.Fatalf("appender did not enter for %s", path)
			}
		}
		assertReady(t, h, http.StatusServiceUnavailable, status{Reason: "sampling_pending"})
		// Let both requests complete after proving that they were in flight
		// together. Each receives its own release before the channel closes.
		for range 2 {
			release <- struct{}{}
			select {
			case code := <-responses:
				if code != http.StatusNoContent {
					t.Fatalf("append response = %d", code)
				}
			case <-time.After(time.Second):
				t.Fatal("append did not complete after release")
			}
		}
	})
}

func TestReadyReadsWALOutcome(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		log := &outcomeAppender{}
		m := &monitor{sample: sample{at: time.Now(), headroom: true}}
		h := handlerForMonitor(m, log, func(Appender) http.Handler { return http.NotFoundHandler() })
		assertReady(t, h, http.StatusOK, status{Ready: true, Reason: "ready"})
		tests := []struct {
			name   string
			commit wal.CommitOutcome
			code   int
			want   status
		}{
			{"space exhausted", wal.CommitOutcome{State: "rolled_back", Category: "enospc"}, http.StatusServiceUnavailable, status{Reason: "wal_rollback", Category: "enospc"}},
			{"no new outcome", wal.CommitOutcome{}, http.StatusServiceUnavailable, status{Reason: "wal_rollback", Category: "enospc"}},
			{"I/O error", wal.CommitOutcome{State: "rolled_back", Category: "eio"}, http.StatusServiceUnavailable, status{Reason: "wal_rollback", Category: "eio"}},
			{"unknown state", wal.CommitOutcome{State: "unknown", Category: "other"}, http.StatusServiceUnavailable, status{Reason: "wal_rollback", Category: "eio"}},
			{"other failure", wal.CommitOutcome{State: "rolled_back", Category: "other"}, http.StatusServiceUnavailable, status{Reason: "wal_rollback", Category: "other"}},
			{"unexpected category", wal.CommitOutcome{State: "rolled_back", Category: "private diagnostic"}, http.StatusServiceUnavailable, status{Reason: "wal_rollback", Category: "other"}},
			{"durable recovery", wal.CommitOutcome{State: "durable", Category: "enospc"}, http.StatusOK, status{Ready: true, Reason: "ready"}},
			{"uncertain with unknown category", wal.CommitOutcome{State: "uncertain", Category: "private diagnostic"}, http.StatusServiceUnavailable, status{Reason: "recovery_required", Category: "other"}},
			{"sticky uncertainty", wal.CommitOutcome{State: "durable"}, http.StatusServiceUnavailable, status{Reason: "recovery_required", Category: "other"}},
		}
		for _, tt := range tests {
			t.Log(tt.name)
			time.Sleep(time.Millisecond)
			log.commit = tt.commit
			if log.commit.State != "" {
				log.commit.At = time.Now()
			}
			assertReady(t, h, tt.code, tt.want)
		}
	})
}

func TestDiskHeadroom(t *testing.T) {
	dir := t.TempDir()
	if _, err := diskHeadroom(dir); err != nil {
		t.Fatalf("stat active data directory: %v", err)
	}
	if _, err := diskHeadroom(filepath.Join(dir, "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat missing data directory: %v", err)
	}
}
