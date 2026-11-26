package httpapi

import (
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"bog/internal/store"
	"bog/internal/wal"
)

func openHTTPTestWAL(t *testing.T, dir string, s *store.Store) *wal.Log {
	t.Helper()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatalf("set test data directory permissions: %v", err)
	}
	w, err := wal.Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	})
	return w
}

func TestIngestAtomicAndQuery(t *testing.T) {
	s := &store.Store{}
	w := openHTTPTestWAL(t, t.TempDir(), s)
	h := New(s, w)
	good := `{"ts":"2025-01-01T00:00:00Z","line":"a"}`
	for _, tc := range []struct {
		body   string
		status int
	}{{good + "\n{}", 400}, {good + "\n" + good, 204}} {
		r := httptest.NewRequest("POST", "/ingest", strings.NewReader(tc.body))
		out := httptest.NewRecorder()
		h.ServeHTTP(out, r)
		if out.Code != tc.status {
			t.Fatalf("status %d", out.Code)
		}
	}
	r := httptest.NewRequest("GET", "/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z", nil)
	out := httptest.NewRecorder()
	h.ServeHTTP(out, r)
	if out.Code != 200 || strings.Count(out.Body.String(), "\n") != 2 {
		t.Fatalf("query %d %s", out.Code, out.Body.String())
	}
}

func TestTruncatedBodyDoesNotCommit(t *testing.T) {
	s := &store.Store{}
	w := openHTTPTestWAL(t, t.TempDir(), s)
	h := New(s, w)
	body := `{"ts":"2025-01-01T00:00:00Z","line":"a"}`
	r := httptest.NewRequest("POST", "/ingest", strings.NewReader(body))
	r.ContentLength++
	out := httptest.NewRecorder()
	h.ServeHTTP(out, r)
	if out.Code != 400 {
		t.Fatalf("status %d", out.Code)
	}
	out = httptest.NewRecorder()
	h.ServeHTTP(out, httptest.NewRequest("GET", "/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z", nil))
	if out.Body.Len() != 0 {
		t.Fatal("truncated request committed")
	}
}

func TestUnicodeIngestAndLegacyReplay(t *testing.T) {
	dir := t.TempDir()
	s := &store.Store{}
	log := openHTTPTestWAL(t, dir, s)
	// Append through the historical decoder to create an old-format committed record.
	legacy := []byte(`{"ts":"2025-01-01T00:00:00Z","line":"old\ud800"}`)
	rows, err := store.Decode(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Append(legacy, rows); err != nil {
		t.Fatal(err)
	}
	h := New(s, log)
	good := `{"ts":"2025-01-01T00:00:00Z","line":"ok"}`
	for _, bad := range []string{
		`{"ts":"2025-01-01T00:00:00Z","line":"\ud800"}`,
		`{"ts":"2025-01-01T00:00:00Z","line":"\udc00"}`,
		`{"ts":"2025-01-01T00:00:00Z","line":"\ud800\u0041"}`,
		`{"ts":"2025-01-01T00:00:00Z","line":"\ud800\ud800\udc00"}`,
		`{"ts":"2025-01-01T00:00:00Z","li\ud800ne":"x"}`,
	} {
		out := httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequest("POST", "/ingest", strings.NewReader(good+"\n"+bad)))
		if out.Code != 400 {
			t.Fatalf("bad %s: status %d", bad, out.Code)
		}
	}
	accepted := `{"ts":"2025-01-01T00:00:00Z","line":"\ud83d\ude00 � \\ud800 \ufffd"}`
	out := httptest.NewRecorder()
	h.ServeHTTP(out, httptest.NewRequest("POST", "/ingest", strings.NewReader(accepted)))
	if out.Code != 204 {
		t.Fatalf("valid status %d", out.Code)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	replay := &store.Store{}
	log = openHTTPTestWAL(t, dir, replay)
	out = httptest.NewRecorder()
	New(replay, log).ServeHTTP(out, httptest.NewRequest("GET", "/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z", nil))
	if out.Code != 200 || strings.Count(out.Body.String(), "\n") != 2 || !strings.Contains(out.Body.String(), "old�") || !strings.Contains(out.Body.String(), "😀 �") || !strings.Contains(out.Body.String(), `\\ud800`) {
		t.Fatalf("replay: %d %s", out.Code, out.Body.String())
	}
}

func TestSnapshotPagination(t *testing.T) {
	s := &store.Store{}
	dir := t.TempDir()
	log := openHTTPTestWAL(t, dir, s)
	h := New(s, log)
	ingest := func(body string) {
		t.Helper()
		out := httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequest("POST", "/ingest", strings.NewReader(body)))
		if out.Code != 204 {
			t.Fatal(out.Code, out.Body.String())
		}
	}
	row := `{"ts":"2025-01-01T00:00:01Z","line":"same"}`
	ingest(row + "\n" + row + "\n" + row + "\n" + `{"ts":"2025-01-01T00:00:02Z","line":"last"}`)
	query := func(raw string) *httptest.ResponseRecorder {
		t.Helper()
		out := httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequest("GET", "/query?"+raw, nil))
		return out
	}
	bounds := "start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z"
	first := query(bounds + "&page=1&limit=1")
	token := first.Header().Get("Bog-Next-Cursor")
	if first.Code != 200 || token == "" || strings.Count(first.Body.String(), "\n") != 1 {
		t.Fatal(first)
	}
	ingest(`{"ts":"2025-01-01T00:00:00Z","line":"backfill"}`)
	equivalent := "start=2024-12-31T19%3A00%3A00-05%3A00&end=2025-01-01T19%3A00%3A00-05%3A00"
	second := query(equivalent + "&cursor=" + token + "&limit=2")
	next := second.Header().Get("Bog-Next-Cursor")
	if second.Code != 200 || next == "" || strings.Count(second.Body.String(), "\n") != 2 || strings.Count(second.Body.String(), `"same"`) != 2 {
		t.Fatal(second.Code, second.Body.String())
	}
	last := query(bounds + "&cursor=" + next + "&limit=1")
	if last.Code != 200 || last.Header().Get("Bog-Next-Cursor") != "" || !strings.Contains(last.Body.String(), "last") {
		t.Fatal(last.Code, last.Body.String())
	}
	for _, params := range []string{"page=0", "page=1&cursor=x", "cursor=", "cursor=" + token + "x", "cursor=" + token + "&contains=x"} {
		if out := query(bounds + "&" + params); out.Code != 400 {
			t.Fatalf("%s: %d", params, out.Code)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	replay := &store.Store{}
	log = openHTTPTestWAL(t, dir, replay)
	h = New(replay, log)
	if out := query(bounds + "&cursor=" + token); out.Code != 400 {
		t.Fatalf("old token: %d", out.Code)
	}
	fresh := query(bounds + "&page=1&limit=10")
	if fresh.Code != 200 || strings.Count(fresh.Body.String(), "\n") != 5 || fresh.Header().Get("Bog-Next-Cursor") != "" {
		t.Fatal(fresh.Code, fresh.Body.String())
	}
}
