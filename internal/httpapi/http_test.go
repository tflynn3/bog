package httpapi

import (
	"bog/internal/store"
	"bog/internal/wal"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIngestAtomicAndQuery(t *testing.T) {
	s := &store.Store{}
	w, err := wal.Open(t.TempDir(), s)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
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
	w, err := wal.Open(t.TempDir(), s)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
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
	log, err := wal.Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
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
	log, err = wal.Open(dir, replay)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	out = httptest.NewRecorder()
	New(replay, log).ServeHTTP(out, httptest.NewRequest("GET", "/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z", nil))
	if out.Code != 200 || strings.Count(out.Body.String(), "\n") != 2 || !strings.Contains(out.Body.String(), "old�") || !strings.Contains(out.Body.String(), "😀 �") || !strings.Contains(out.Body.String(), `\\ud800`) {
		t.Fatalf("replay: %d %s", out.Code, out.Body.String())
	}
}
