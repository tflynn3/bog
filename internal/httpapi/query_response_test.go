package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
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
				fits, required, err := b.appendRow(row, nil)
				if err != nil || fits != (difference >= 0) {
					t.Fatalf("byte limit %+d: fits=%v error=%v", difference, fits, err)
				}
				if !fits && required != len(want) {
					t.Errorf("required bytes = %d, want %d", required, len(want))
				}
				if fits && (b.size != len(want) || cap(b.rows[0]) != len(want)) {
					t.Errorf("size = %d, allocated %d, want %d", b.size, cap(b.rows[0]), len(want))
				}
				if fits && string(bytes.Join(b.rows, nil)) != want {
					t.Errorf("encoding differs from encoding/json")
				}
				if !fits && b.size != 0 {
					t.Errorf("rejected row left %d bytes", b.size)
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
			if fits, _, err := b.appendRow(first, nil); err != nil || !fits {
				t.Fatalf("first row: fits=%v error=%v", fits, err)
			}
			fits, required, err := b.appendRow(tc.row, store.NewQueryWork(t.Context(), tc.budget))
			if fits || !errors.Is(err, tc.want) || string(bytes.Join(b.rows, nil)) != want || required != 0 {
				t.Fatalf("rollback: fits=%v required=%d error=%v bytes=%d", fits, required, err, b.size)
			}
			if fits, _, err := b.appendRow(first, nil); err != nil || !fits || string(bytes.Join(b.rows, nil)) != want+want {
				t.Fatalf("append after rollback: fits=%v error=%v bytes=%d", fits, err, b.size)
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

	// The unescaped line could fit, but its escaped form cannot.
	capBytes := len(oversized.Line) + len(jsonRow(t, first))
	h := NewWithQueryMaxResponseBytes(s, nil, capBytes)
	query := func(params string) *httptest.ResponseRecorder {
		out := httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequest(http.MethodGet, queryBounds+params, nil))
		return out
	}
	page := query("&page=1")
	token := page.Header().Get("Bog-Next-Cursor")
	if page.Code != http.StatusOK || page.Body.String() != jsonRow(t, first) || token == "" ||
		page.Header().Get("Content-Length") != strconv.Itoa(page.Body.Len()) {
		t.Fatalf("page: status=%d headers=%v bytes=%d", page.Code, page.Header(), page.Body.Len())
	}
	required := len(jsonRow(t, oversized))
	message := fmt.Sprintf("query record exceeds response byte limit (cap %d bytes, requires %d bytes)", capBytes, required)
	for range 2 {
		assertQueryRejection(t, query("&cursor="+token), http.StatusUnprocessableEntity, message)
	}
}

func TestLiveQueryChargesEscapingAndNewlineForDelivery(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	emptyBytes := len(jsonRow(t, store.Row{TS: stamp}))
	// This record ends one byte past a delivery unit boundary. Removing the
	// terminating newline would make it cost one less unit.
	newlineBoundary := strings.Repeat("x", queryDeliveryBytesPerUnit-emptyBytes%queryDeliveryBytesPerUnit+1)
	for _, line := range []string{"", "plain", "<\x00\u2028\n", newlineBoundary} {
		t.Run(line, func(t *testing.T) {
			row := store.Row{TS: stamp, Line: line}
			want := jsonRow(t, row)
			s := new(store.Store)
			s.Publish([]store.Row{row})
			const scanOverhead = 3 // Run, seek, and candidate.
			delivery := (len(want) + queryDeliveryBytesPerUnit - 1) / queryDeliveryBytesPerUnit
			minimum := scanOverhead + uint64(2*len(line)+delivery)
			for _, tc := range []struct {
				limit   int
				budget  uint64
				status  int
				message string
			}{
				{len(want), minimum - 1, http.StatusServiceUnavailable, "query work budget exhausted"},
				{len(want) - 1, minimum, http.StatusUnprocessableEntity, fmt.Sprintf("query record exceeds response byte limit (cap %d bytes, requires %d bytes)", len(want)-1, len(want))},
				{len(want), minimum, http.StatusOK, ""},
			} {
				h := NewWithQueryLimits(s, nil, tc.limit, 1, tc.budget)
				out := httptest.NewRecorder()
				h.ServeHTTP(out, httptest.NewRequest(http.MethodGet, queryBounds, nil))
				if tc.status != http.StatusOK {
					assertQueryRejection(t, out, tc.status, tc.message)
				} else if out.Code != http.StatusOK || out.Body.String() != want || out.Header().Get("Content-Length") != strconv.Itoa(len(want)) {
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
	if fits, _, err := b.appendRow(row, work); fits || !errors.Is(err, context.Canceled) || b.size != 0 {
		t.Fatalf("cancelled row: fits=%v error=%v bytes=%d", fits, err, b.size)
	}
}

func TestOversizeQueryRecordAllocation(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	line := strings.Repeat("<", store.MaxBatch-128)
	row := store.Row{TS: stamp, Line: line}
	wantSize := len(jsonRow(t, store.Row{TS: stamp})) + 6*len(line)
	s := new(store.Store)
	s.Publish([]store.Row{row})
	const capBytes = 1024
	h := NewWithQueryMaxResponseBytes(s, nil, capBytes)
	req := httptest.NewRequest(http.MethodGet, queryBounds+"&page=1", nil)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	out := httptest.NewRecorder()
	h.ServeHTTP(out, req)
	runtime.ReadMemStats(&after)

	assertQueryRejection(t, out, http.StatusUnprocessableEntity,
		fmt.Sprintf("query record exceeds response byte limit (cap %d bytes, requires %d bytes)", capBytes, wantSize))
	// Allow slack for HTTP and runtime bookkeeping. The input and its escaped
	// representation are both much larger than this allocation bound.
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 256<<10 {
		t.Fatalf("oversized query allocated %d bytes with a %d byte response cap", allocated, capBytes)
	}
}
