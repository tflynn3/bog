package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bog/internal/store"
)

func jsonRow(t *testing.T, row store.Row) string {
	t.Helper()
	data, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	return string(data) + "\n"
}

func TestQueryResponseEncoding(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 12300, time.FixedZone("offset", 90*60))
	for _, tc := range []struct {
		name string
		line string
	}{
		{"empty", ""},
		{"ASCII", "ordinary log record"},
		{"control characters", "\x00\x01\b\t\n\v\f\r\x1f\x7f"},
		{"HTML and JSON delimiters", `<>&"\/`},
		{"Unicode", "é界😀\u2028\u2029\ufffd"},
		{"malformed UTF-8", "\xe0\x80\xbf\xff"},
		{"two byte rune across chunk", strings.Repeat("a", queryStringChunk-1) + "é"},
		{"three byte rune across chunk", strings.Repeat("a", queryStringChunk-2) + "\u2028"},
		{"four byte rune across chunk", strings.Repeat("a", queryStringChunk-3) + "😀"},
		{"malformed rune across chunk", strings.Repeat("a", queryStringChunk-2) + "\xf4\x9f\xba\xbf"},
		{"many chunks", strings.Repeat("界😀\x08<&\u2029", queryStringChunk)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := store.Row{TS: stamp, Line: tc.line}
			want := jsonRow(t, row)
			for _, difference := range []int{-1, 0, 1} {
				b := queryResponse{maxBytes: len(want) + difference}
				fits, err := b.appendRow(row, nil)
				if err != nil || fits != (difference >= 0) {
					t.Fatalf("byte limit %+d: fits=%v error=%v", difference, fits, err)
				}
				if cap(b.body) > b.maxBytes {
					t.Errorf("capacity %d exceeds cap %d", cap(b.body), b.maxBytes)
				}
				if fits && string(b.body) != want {
					t.Errorf("encoding differs from encoding/json")
				}
				if !fits && len(b.body) != 0 {
					t.Errorf("rejected row left %d bytes", len(b.body))
				}
			}
		})
	}
}

func TestQueryResponseRollback(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	first := store.Row{TS: stamp, Line: "first"}
	want := jsonRow(t, first)
	large := store.Row{TS: stamp, Line: strings.Repeat("<", 2*queryStringChunk)}
	for _, tc := range []struct {
		name   string
		row    store.Row
		limit  int
		budget uint64
		want   error
	}{
		{"raw length", large, 100, 0, nil},
		{"escape expansion", large, 8 * queryStringChunk, uint64(len(large.Line)), nil},
		{"partial encoding", large, 16 * queryStringChunk, queryStringChunk, store.ErrQueryWorkExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := queryResponse{maxBytes: len(want) + tc.limit}
			if fits, err := b.appendRow(first, nil); err != nil || !fits {
				t.Fatalf("first row: fits=%v error=%v", fits, err)
			}
			fits, err := b.appendRow(tc.row, store.NewQueryWork(t.Context(), tc.budget))
			if fits || !errors.Is(err, tc.want) || string(b.body) != want || cap(b.body) > b.maxBytes {
				t.Fatalf("rollback: fits=%v error=%v bytes=%d capacity=%d", fits, err, len(b.body), cap(b.body))
			}
			if fits, err := b.appendRow(first, nil); err != nil || !fits || string(b.body) != want+want {
				t.Fatalf("append after rollback: fits=%v error=%v bytes=%d", fits, err, len(b.body))
			}
		})
	}
}

func TestOversizeQueryRecordAfterByteLimitedPage(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	first := store.Row{TS: stamp, Line: "small"}
	oversized := store.Row{TS: stamp, Line: strings.Repeat("<", queryStringChunk)}
	s := new(store.Store)
	s.Publish([]store.Row{first, oversized})

	query := func(maxBytes int, params string) *httptest.ResponseRecorder {
		out := httptest.NewRecorder()
		NewWithQueryLimits(s, nil, maxBytes, 1, DefaultQueryWorkBudget).ServeHTTP(out, httptest.NewRequest(http.MethodGet, queryBounds+params, nil))
		return out
	}
	// The unescaped line could fit, but its escaped form cannot.
	cap := len(oversized.Line) + len(jsonRow(t, first))
	page := query(cap, "&page=1")
	token := page.Header().Get("Bog-Next-Cursor")
	if page.Code != http.StatusOK || page.Body.String() != jsonRow(t, first) || token == "" {
		t.Fatalf("page: status=%d headers=%v bytes=%d", page.Code, page.Header(), page.Body.Len())
	}
	path := "&cursor=" + token
	assertQueryRejection(t, query(cap, path), http.StatusUnprocessableEntity, "query record exceeds response byte limit")
	want := jsonRow(t, oversized)
	retry := query(len(want), path)
	if retry.Code != http.StatusOK || retry.Body.String() != want || retry.Header().Get("Bog-Next-Cursor") != "" {
		t.Fatalf("retry: status=%d headers=%v bytes=%d", retry.Code, retry.Header(), retry.Body.Len())
	}
}

func TestLiveQueryChargesEscapingAndNewlineForDelivery(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	for _, line := range []string{"", "plain", "<\x00\u2028\n"} {
		t.Run(line, func(t *testing.T) {
			row := store.Row{TS: stamp, Line: line}
			want := jsonRow(t, row)
			s := new(store.Store)
			s.Publish([]store.Row{row})
			const scanOverhead = 3 // Run, seek, and candidate.
			minimum := scanOverhead + 2*uint64(len(line)) + uint64(len(want))
			for _, tc := range []struct {
				limit   int
				budget  uint64
				status  int
				message string
			}{
				{len(want), minimum - 1, http.StatusServiceUnavailable, "query work budget exhausted"},
				{len(want) - 1, minimum, http.StatusUnprocessableEntity, "query record exceeds response byte limit"},
				{len(want), minimum, http.StatusOK, ""},
			} {
				h := NewWithQueryLimits(s, nil, tc.limit, 1, tc.budget)
				out := httptest.NewRecorder()
				h.ServeHTTP(out, httptest.NewRequest(http.MethodGet, queryBounds, nil))
				if tc.status != http.StatusOK {
					assertQueryRejection(t, out, tc.status, tc.message)
				} else if out.Code != http.StatusOK || out.Body.String() != want {
					t.Fatalf("status=%d body=%q, want %q", out.Code, out.Body.String(), want)
				}
			}
		})
	}
}

func TestQueryResponseCancellationBetweenChunks(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	work := store.NewQueryWork(&queryScanContext{Context: ctx, cancel: cancel, cancelAt: 3}, DefaultQueryWorkBudget)
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	row := store.Row{TS: stamp, Line: strings.Repeat("x", 2*queryStringChunk)}
	b := queryResponse{maxBytes: DefaultQueryMaxResponseBytes}
	if fits, err := b.appendRow(row, work); fits || !errors.Is(err, context.Canceled) || len(b.body) != 0 {
		t.Fatalf("cancelled row: fits=%v error=%v bytes=%d", fits, err, len(b.body))
	}
}
