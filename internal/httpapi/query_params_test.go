package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"bog/internal/store"
)

func TestQueryParameterDiagnostics(t *testing.T) {
	const start = "start=2025-01-01T00:00:00Z"
	const end = "end=2025-01-02T00:00:00Z"
	for _, tt := range []struct {
		name, params, message string
	}{
		{"missing start", end, "missing start"},
		{"empty start", "start=&" + end, "invalid start: expected an RFC3339 timestamp"},
		{"malformed start", "start=secret-start&" + end, "invalid start: expected an RFC3339 timestamp"},
		{"unescaped positive offset", "start=2025-01-01T05:30:00+05:30&" + end, "invalid start: expected an RFC3339 timestamp"},
		{"missing end", start, "missing end"},
		{"empty end", start + "&end=", "invalid end: expected an RFC3339 timestamp"},
		{"malformed end", start + "&end=secret-end", "invalid end: expected an RFC3339 timestamp"},
		{"invalid zone", start + "&end=2025-01-02T00:00:00%2B24:00", "invalid end: expected an RFC3339 timestamp"},
		{"reversed bounds", start + "&end=2024-12-31T00:00:00Z", "invalid range: end precedes start"},
		{"empty limit", start + "&" + end + "&limit=", "invalid limit: expected decimal digits"},
		{"alphabetic limit", start + "&" + end + "&limit=secret-limit", "invalid limit: expected decimal digits"},
		{"fractional limit", start + "&" + end + "&limit=1.5", "invalid limit: expected decimal digits"},
		{"signed limit", start + "&" + end + "&limit=%2B1", "invalid limit: expected decimal digits"},
		{"zero limit", start + "&" + end + "&limit=0", "invalid limit: must be between 1 and 1000"},
		{"negative limit", start + "&" + end + "&limit=-1", "invalid limit: expected decimal digits"},
		{"over limit", start + "&" + end + "&limit=1001", "invalid limit: must be between 1 and 1000"},
		{"overflowing limit", start + "&" + end + "&limit=18446744073709551616", "invalid limit: must be between 1 and 1000"},
		{"duplicate parameter", start + "&" + start + "&" + end, "invalid start: must appear only once"},
		{"unknown parameter", start + "&" + end + "&extra=secret", "invalid parameter"},
		{"malformed query encoding", start + "&" + end + "&contains=%zz-secret", "invalid query"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := New(nil, nil)
			out := httptest.NewRecorder()
			h.ServeHTTP(out, httptest.NewRequest(http.MethodGet, "/query?"+tt.params, nil))
			assertQueryRejection(t, out, http.StatusBadRequest, tt.message)
		})
	}
}

func TestQueryPositiveOffset(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	first := store.Row{TS: start, Line: "at start"}
	second := store.Row{TS: start.Add(time.Nanosecond), Line: "second"}
	end := store.Row{TS: start.Add(24 * time.Hour), Line: "at end"}
	s := new(store.Store)
	s.Publish([]store.Row{first, second, end})
	h := New(s, nil)
	query := func(path string) *httptest.ResponseRecorder {
		out := httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequest(http.MethodGet, path, nil))
		return out
	}
	const positive = "/query?start=2025-01-01T05:30:00%2B05:30&end=2025-01-02T05:30:00%2B05:30"
	out := query(positive + "&limit=01000")
	if out.Code != http.StatusOK || out.Body.String() != jsonRow(t, first)+jsonRow(t, second) {
		t.Fatalf("ordinary query: status=%d body=%q", out.Code, out.Body.String())
	}
	out = query(positive + "&page=1&limit=1")
	token := out.Header().Get("Bog-Next-Cursor")
	if out.Code != http.StatusOK || out.Body.String() != jsonRow(t, first) || token == "" {
		t.Fatalf("first: status=%d body=%q headers=%v", out.Code, out.Body.String(), out.Header())
	}
	out = query(queryBounds + "&cursor=" + token + "&limit=1")
	if out.Code != http.StatusOK || out.Body.String() != jsonRow(t, second) || out.Header().Get("Bog-Next-Cursor") != "" {
		t.Fatalf("continuation: status=%d body=%q headers=%v", out.Code, out.Body.String(), out.Header())
	}
}
