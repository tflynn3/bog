package httpapi

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"bog/internal/store"
	"bog/internal/wal"
)

func openHTTPTestWAL(t *testing.T, dir string, s *store.Store) *wal.Log {
	t.Helper()
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

func readHTTPTestBody(t *testing.T, body io.ReadCloser) []byte {
	t.Helper()
	data, err := io.ReadAll(body)
	if err := errors.Join(err, body.Close()); err != nil {
		t.Fatal(err)
	}
	return data
}

func TestCursorAuthenticationBindsFilter(t *testing.T) {
	a := newAPI(nil, nil, DefaultQueryMaxResponseBytes, DefaultQueryWorkBudget)
	filter := PageFilter{Contains: "needle"}
	token := a.encodeCursor(cursorData{capture: 1, deadline: time.Now(), after: "position"}, a.filterDigest(filter))
	if _, err := a.decodeCursor(token, a.filterDigest(filter)); err != nil {
		t.Fatal(err)
	}
	for _, wrong := range [][sha256.Size]byte{{}, a.filterDigest(PageFilter{Contains: "other"})} {
		if _, err := a.decodeCursor(token, wrong); !errors.Is(err, store.ErrInvalidQueryCursor) {
			t.Fatalf("cursor authenticated with the wrong filter: %v", err)
		}
	}
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

func TestPaginationLargeFilterHTTP(t *testing.T) {
	s := &store.Store{}
	stamp, err := store.ParseTime("2025-01-01T00:00:01Z")
	if err != nil {
		t.Fatal(err)
	}
	filter := strings.Repeat("a", 600000)
	s.Publish([]store.Row{{TS: stamp, Line: filter}, {TS: stamp, Line: filter}})
	server := httptest.NewUnstartedServer(New(s, nil))
	server.Config.MaxHeaderBytes = 1 << 20
	server.Start()
	defer server.Close()
	values := url.Values{"start": {"2025-01-01T00:00:00Z"}, "end": {"2025-01-01T00:01:00Z"}, "contains": {filter}, "limit": {"1"}, "page": {"1"}}
	get := func() (*http.Response, []byte) {
		t.Helper()
		resp, err := server.Client().Get(server.URL + "/query?" + values.Encode())
		if err != nil {
			t.Fatal(err)
		}
		return resp, readHTTPTestBody(t, resp.Body)
	}
	first, body := get()
	token := first.Header.Get("Bog-Next-Cursor")
	if first.StatusCode != 200 || token == "" || len(token) > 2048 || !bytes.Contains(body, []byte(filter)) {
		t.Fatalf("first status=%d cursor length=%d", first.StatusCode, len(token))
	}
	values.Del("page")
	values.Set("cursor", token)
	last, body := get()
	if last.StatusCode != 200 || last.Header.Get("Bog-Next-Cursor") != "" || !bytes.Contains(body, []byte(filter)) {
		t.Fatalf("last status=%d", last.StatusCode)
	}
	values.Del("cursor")
	values.Set("page", "1")
	values.Set("contains", strings.Repeat("a", 1045000))
	tooLarge, _ := get()
	if tooLarge.StatusCode != 400 {
		t.Fatalf("oversized first page: %d", tooLarge.StatusCode)
	}
}

func TestPaginationRawQueryCharactersHTTP(t *testing.T) {
	s := &store.Store{}
	stamp, err := store.ParseTime("2025-01-01T00:00:01Z")
	if err != nil {
		t.Fatal(err)
	}
	filter := strings.Repeat(":", 400000)
	s.Publish([]store.Row{{TS: stamp, Line: filter}, {TS: stamp, Line: filter}})
	server := httptest.NewUnstartedServer(New(s, nil))
	server.Config.MaxHeaderBytes = 1 << 20
	server.Start()
	defer server.Close()
	base := server.URL + "/query?start=2025-01-01T00:00:00Z&end=2025-01-01T00:01:00Z&limit=1&contains=" + filter
	first, err := server.Client().Get(base + "&page=1")
	if err != nil {
		t.Fatal(err)
	}
	readHTTPTestBody(t, first.Body)
	token := first.Header.Get("Bog-Next-Cursor")
	if first.StatusCode != 200 || token == "" {
		t.Fatalf("first: %d", first.StatusCode)
	}
	last, err := server.Client().Get(base + "&cursor=" + token)
	if err != nil {
		t.Fatal(err)
	}
	readHTTPTestBody(t, last.Body)
	if last.StatusCode != 200 || last.Header.Get("Bog-Next-Cursor") != "" {
		t.Fatalf("last: %d", last.StatusCode)
	}
}

func TestQueryResponseByteLimits(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	rows := []store.Row{{TS: stamp, Line: "<&\""}, {TS: stamp, Line: "second"}}
	first, second := jsonRow(t, rows[0]), jsonRow(t, rows[1])
	bounds := "/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z"
	for _, tc := range []struct {
		name string
		cap  int
		want int
		body string
	}{
		{"exact first", len(first), 200, first},
		{"exact both", len(first) + len(second), 200, first + second},
		{"escaped row too large", len(first) - 1, 422, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &store.Store{}
			s.Publish(rows)
			out := httptest.NewRecorder()
			NewWithQueryMaxResponseBytes(s, nil, tc.cap).ServeHTTP(out, httptest.NewRequest("GET", bounds, nil))
			if out.Code != tc.want || (tc.want == 200 && out.Body.String() != tc.body) || (tc.want == 422 && (out.Header().Get("Bog-Next-Cursor") != "" || strings.Contains(out.Header().Get("Content-Type"), "ndjson"))) {
				t.Fatalf("status=%d headers=%v body=%q", out.Code, out.Header(), out.Body.String())
			}
		})
	}
}

func TestSnapshotByteCutPreservesOrdinal(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	row := store.Row{TS: stamp, Line: "same"}
	size := len(jsonRow(t, row))
	s := &store.Store{}
	s.Publish([]store.Row{row, row, row, row, row})
	oneAPI := newAPI(s, nil, size, DefaultQueryWorkBudget)
	one := oneAPI.handler(DefaultQueryConcurrency)
	oneOut := httptest.NewRecorder()
	one.ServeHTTP(oneOut, httptest.NewRequest("GET", "/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&page=1&limit=5", nil))
	oneCursor, err := decodeTestCursor(oneAPI, oneOut.Header().Get("Bog-Next-Cursor"), boundsForDay(t))
	if oneOut.Code != 200 || oneOut.Body.Len() != size || err != nil || oneCursor.Ordinal != 0 {
		t.Fatalf("one-row cut: %d cursor=%+v error=%v", oneOut.Code, oneCursor, err)
	}
	a := newAPI(s, nil, size*2, DefaultQueryWorkBudget)
	h := a.handler(DefaultQueryConcurrency)
	bounds := "/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z"
	query := func(params string) *httptest.ResponseRecorder {
		out := httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequest("GET", bounds+params, nil))
		return out
	}
	first := query("&page=1&limit=5")
	token := first.Header().Get("Bog-Next-Cursor")
	if first.Code != 200 || first.Body.Len() != size*2 || token == "" {
		t.Fatalf("first: %d %q", first.Code, first.Body.String())
	}
	c, err := decodeTestCursor(a, token, boundsForDay(t))
	if err != nil || c.Ordinal != 1 {
		t.Fatalf("cursor: %+v %v", c, err)
	}
	s.Publish([]store.Row{{TS: stamp.Add(-1), Line: "backfill"}, row})
	second := query("&cursor=" + token + "&limit=5")
	token = second.Header().Get("Bog-Next-Cursor")
	if second.Code != 200 || second.Body.Len() != size*2 || token == "" {
		t.Fatalf("second: %d %q", second.Code, second.Body.String())
	}
	c, err = decodeTestCursor(a, token, boundsForDay(t))
	if err != nil || c.Ordinal != 3 {
		t.Fatalf("cursor: %+v %v", c, err)
	}
	last := query("&cursor=" + token + "&limit=5")
	if last.Code != 200 || last.Body.Len() != size || last.Header().Get("Bog-Next-Cursor") != "" {
		t.Fatalf("last: %d %q", last.Code, last.Body.String())
	}
	tiny := NewWithQueryMaxResponseBytes(s, nil, size-1)
	out := httptest.NewRecorder()
	tiny.ServeHTTP(out, httptest.NewRequest("GET", bounds+"&page=1", nil))
	if out.Code != 422 || out.Header().Get("Bog-Next-Cursor") != "" {
		t.Fatalf("oversized: %d %v", out.Code, out.Header())
	}
}

func TestQueryCursorIssuerIsolation(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	first := store.Row{TS: stamp, Line: "same filter, first"}
	last := store.Row{TS: stamp, Line: "same filter, last"}
	s := new(store.Store)
	s.Publish([]store.Row{first, last})
	issuer, other := New(s, nil), New(s, nil)
	query := func(h http.Handler, params string) *httptest.ResponseRecorder {
		out := httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequest(http.MethodGet, queryBounds+"&contains=filter&limit=1"+params, nil))
		return out
	}
	for _, pair := range [][2]http.Handler{{issuer, other}, {other, issuer}} {
		page := query(pair[0], "&page=1")
		cursor := page.Header().Get("Bog-Next-Cursor")
		if page.Code != http.StatusOK || page.Body.String() != jsonRow(t, first) || cursor == "" {
			t.Fatalf("first page: status=%d headers=%v body=%q", page.Code, page.Header(), page.Body.String())
		}
		path := "&cursor=" + cursor
		assertQueryRejection(t, query(pair[1], path), http.StatusBadRequest, "invalid cursor")
		resumed := query(pair[0], path)
		if resumed.Code != http.StatusOK || resumed.Body.String() != jsonRow(t, last) || resumed.Header().Get("Bog-Next-Cursor") != "" {
			t.Fatalf("continuation: status=%d headers=%v body=%q", resumed.Code, resumed.Header(), resumed.Body.String())
		}
	}
}

func TestMethodNotAllowedAdvertisesSupportedMethod(t *testing.T) {
	h := New(new(store.Store), nil)
	for _, tc := range []struct {
		path, method, allow string
	}{
		{"/healthz", http.MethodHead, http.MethodGet},
		{"/query", http.MethodPost, http.MethodGet},
		{"/ingest", http.MethodGet, http.MethodPost},
	} {
		t.Run(tc.path, func(t *testing.T) {
			out := httptest.NewRecorder()
			h.ServeHTTP(out, httptest.NewRequest(tc.method, tc.path, nil))
			if out.Code != http.StatusMethodNotAllowed || out.Header().Get("Allow") != tc.allow {
				t.Fatalf("status=%d Allow=%q", out.Code, out.Header().Get("Allow"))
			}
		})
	}
	for _, path := range []string{"/healthz", queryBounds, queryBounds + "&page=1"} {
		out := httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequest(http.MethodGet, path, nil))
		if out.Code != http.StatusOK || out.Body.Len() != 0 || out.Header().Get("Content-Length") != "0" {
			t.Fatalf("GET %s: status=%d headers=%v body=%q", path, out.Code, out.Header(), out.Body.String())
		}
	}
}
