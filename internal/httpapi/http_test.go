package httpapi

import (
	"bog/internal/store"
	"bog/internal/wal"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestCursorAuthenticationDomainSeparation(t *testing.T) {
	payload, err := json.Marshal(cursorData{Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	digest := containsDigest(string(payload))
	forged := base64.RawURLEncoding.EncodeToString(append(payload, digest[:]...))
	if _, err := decodeCursor(forged); err == nil {
		t.Fatal("filter digest authenticated a cursor payload")
	}
	token, err := encodeCursor(cursorData{Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeCursor(token); err != nil {
		t.Fatal(err)
	}
}

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

func TestSnapshotPagination(t *testing.T) {
	s := &store.Store{}
	dir := t.TempDir()
	log, err := wal.Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
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
	log, err = wal.Open(dir, replay)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	h = New(replay, log)
	// Simulate a fresh process key while retaining the recovered WAL.
	oldKey := cursorKey
	cursorKey = make([]byte, 32)
	defer func() { cursorKey = oldKey }()
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
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		return resp, body
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
	io.Copy(io.Discard, first.Body)
	first.Body.Close()
	token := first.Header.Get("Bog-Next-Cursor")
	if first.StatusCode != 200 || token == "" {
		t.Fatalf("first: %d", first.StatusCode)
	}
	last, err := server.Client().Get(base + "&cursor=" + token)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, last.Body)
	last.Body.Close()
	if last.StatusCode != 200 || last.Header.Get("Bog-Next-Cursor") != "" {
		t.Fatalf("last: %d", last.StatusCode)
	}
}

func TestQueryResponseByteLimits(t *testing.T) {
	stamp, _ := store.ParseTime("2025-01-01T00:00:01Z")
	rows := []store.Row{{TS: stamp, Line: "<&\""}, {TS: stamp, Line: "second"}}
	encoded := func(row store.Row) string {
		b, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		return string(b) + "\n"
	}
	first, second := encoded(rows[0]), encoded(rows[1])
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
	stamp, _ := store.ParseTime("2025-01-01T00:00:01Z")
	row := store.Row{TS: stamp, Line: "same"}
	b, _ := json.Marshal(row)
	size := len(b) + 1
	s := &store.Store{}
	s.Publish([]store.Row{row, row, row, row, row})
	one := NewWithQueryMaxResponseBytes(s, nil, size)
	oneOut := httptest.NewRecorder()
	one.ServeHTTP(oneOut, httptest.NewRequest("GET", "/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&page=1&limit=5", nil))
	oneCursor, err := decodeCursor(oneOut.Header().Get("Bog-Next-Cursor"))
	if oneOut.Code != 200 || oneOut.Body.Len() != size || err != nil || oneCursor.After.Ordinal != 0 {
		t.Fatalf("one-row cut: %d cursor=%+v error=%v", oneOut.Code, oneCursor, err)
	}
	h := NewWithQueryMaxResponseBytes(s, nil, size*2)
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
	c, err := decodeCursor(token)
	if err != nil || c.After.Ordinal != 1 || c.Count != 5 {
		t.Fatalf("cursor: %+v %v", c, err)
	}
	s.Publish([]store.Row{{TS: stamp.Add(-1), Line: "backfill"}, row})
	second := query("&cursor=" + token + "&limit=5")
	token = second.Header().Get("Bog-Next-Cursor")
	if second.Code != 200 || second.Body.Len() != size*2 || token == "" {
		t.Fatalf("second: %d %q", second.Code, second.Body.String())
	}
	c, err = decodeCursor(token)
	if err != nil || c.After.Ordinal != 3 || c.Count != 5 {
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
