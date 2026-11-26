package httpapi

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"bog/internal/store"
)

type ingestDiagnosticsAppender struct {
	store   store.Store
	calls   int
	payload []byte
}

func (a *ingestDiagnosticsAppender) Append(payload []byte, rows []store.Row) error {
	a.calls++
	a.payload = bytes.Clone(payload)
	a.store.Publish(rows)
	return nil
}

func TestIngestDiagnosticsRejectWholeBatch(t *testing.T) {
	const good = `{"ts":"2025-01-01T00:00:00Z","line":"valid prefix"}`
	for _, tc := range []struct {
		name, row, reason string
	}{
		{"syntax", `{"ts":secret-json}`, "invalid JSON"},
		{"object", `["secret-array"]`, "expected JSON object"},
		{"unknown", `{"secret-key":"secret-value"}`, "unknown field: expected ts or line"},
		{"duplicate escaped key", `{"ts":"secret-ts","t\u0073":"secret-other","line":"x"}`, "duplicate ts field"},
		{"missing line", `{"ts":"2025-01-01T00:00:00Z"}`, "missing line field"},
		{"missing ts", `{"line":"secret-text"}`, "missing ts field"},
		{"type", `{"ts":"2025-01-01T00:00:00Z","line":["secret-text"]}`, "line field must be a string"},
		{"null", `{"ts":null,"line":"secret-text"}`, "ts field must be a string"},
		{"timestamp", `{"ts":"2025-02-30T00:00:00Z","line":"secret-text"}`, "invalid ts: expected an RFC3339 timestamp"},
		{"reserved instant", `{"ts":"9999-12-31T23:59:59.999999999Z","line":"secret-text"}`, "timestamp reserved for exclusive query end"},
		{"trailing data", good + ` "secret-extra"`, "trailing data after JSON object"},
		{"surrogate", `{"ts":"2025-01-01T00:00:00Z","line":"secret\uD800"}`, "unpaired Unicode surrogate escape"},
		{"UTF-8", strings.Replace(good, "valid prefix", "secret\xff", 1), "invalid UTF-8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			appender := new(ingestDiagnosticsAppender)
			h := New(&appender.store, appender)
			post := func(body string) *httptest.ResponseRecorder {
				out := httptest.NewRecorder()
				h.ServeHTTP(out, httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(body)))
				return out
			}

			valid := "\r\n" + good + "\r\n \t\r\n"
			out := post(valid + tc.row + "\n" + good)
			assertQueryRejection(t, out, http.StatusBadRequest, "line 4: "+tc.reason)
			if appender.calls != 0 {
				t.Fatalf("invalid body reached Append %d times", appender.calls)
			}
			out = requestCapturePage(h, "")
			if out.Code != http.StatusOK || out.Body.Len() != 0 || out.Header().Get("Content-Length") != "0" {
				t.Fatalf("query after rejection: status=%d headers=%v body=%q", out.Code, out.Header(), out.Body.String())
			}

			// Retry with the bad row removed and preserve the accepted WAL payload.
			out = post(valid)
			if out.Code != http.StatusNoContent || out.Body.Len() != 0 || appender.calls != 1 || string(appender.payload) != valid {
				t.Fatalf("corrected batch: status=%d body=%q append calls=%d payload=%q",
					out.Code, out.Body.String(), appender.calls, appender.payload)
			}
			out = requestCapturePage(h, "")
			if out.Code != http.StatusOK || out.Body.String() != good+"\n" {
				t.Fatalf("query after correction: status=%d body=%q", out.Code, out.Body.String())
			}
		})
	}
}

func TestIngestDiagnosticsEmptyBatch(t *testing.T) {
	for _, body := range []string{"", "\r\n \t\n"} {
		appender := new(ingestDiagnosticsAppender)
		out := httptest.NewRecorder()
		New(&appender.store, appender).ServeHTTP(out, httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader(body)))
		assertQueryRejection(t, out, http.StatusBadRequest, "empty batch")
		if appender.calls != 0 {
			t.Fatalf("empty body reached Append %d times", appender.calls)
		}
	}
}
