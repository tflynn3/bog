package httpapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"bog/internal/store"
)

func readHTTPTestBody(t *testing.T, body io.ReadCloser) []byte {
	t.Helper()
	data, err := io.ReadAll(body)
	if err := errors.Join(err, body.Close()); err != nil {
		t.Fatal(err)
	}
	return data
}

func decodeMemoryCursor(a *API, token string) (int, store.Position, error) {
	cursor, err := a.decodeCursor(token)
	if err != nil {
		return 0, store.Position{}, err
	}
	count, err := strconv.Atoi(cursor.Capture)
	if err != nil {
		return 0, store.Position{}, err
	}
	var pos store.Position
	err = json.Unmarshal([]byte(cursor.After), &pos)
	return count, pos, err
}

func TestCursorAuthenticationDomainSeparation(t *testing.T) {
	a := newAPI(nil, nil, DefaultQueryMaxResponseBytes, DefaultQueryWorkBudget)
	payload, err := json.Marshal(cursorData{Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := base64.RawURLEncoding.DecodeString(a.containsDigest(string(payload)))
	if err != nil {
		t.Fatal(err)
	}
	forged := base64.RawURLEncoding.EncodeToString(append(payload, digest...))
	if _, err := a.decodeCursor(forged); err == nil {
		t.Fatal("filter digest authenticated a cursor payload")
	}
	token, err := a.encodeCursor(cursorData{Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.decodeCursor(token); err != nil {
		t.Fatal(err)
	}
	if _, err := a.decodeCursor(token[:1] + "\n" + token[1:]); !errors.Is(err, store.ErrInvalidQueryCursor) {
		t.Fatalf("cursor with embedded newline: %v", err)
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
	_, oneCursor, err := decodeMemoryCursor(oneAPI, oneOut.Header().Get("Bog-Next-Cursor"))
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
	count, pos, err := decodeMemoryCursor(a, token)
	if err != nil || pos.Ordinal != 1 || count != 5 {
		t.Fatalf("cursor: count=%d position=%+v error=%v", count, pos, err)
	}
	s.Publish([]store.Row{{TS: stamp.Add(-1), Line: "backfill"}, row})
	second := query("&cursor=" + token + "&limit=5")
	token = second.Header().Get("Bog-Next-Cursor")
	if second.Code != 200 || second.Body.Len() != size*2 || token == "" {
		t.Fatalf("second: %d %q", second.Code, second.Body.String())
	}
	count, pos, err = decodeMemoryCursor(a, token)
	if err != nil || pos.Ordinal != 3 || count != 5 {
		t.Fatalf("cursor: count=%d position=%+v error=%v", count, pos, err)
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
