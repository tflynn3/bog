package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	start = "2025-01-01T00:00:00Z"
	end   = "2025-01-01T00:00:10Z"
	rowA  = `{"ts":"2025-01-01T00:00:01Z","line":"match + A\n\"界\""}` + "\n"
	rowB  = `{"ts":"2025-01-01T00:00:01Z","line":"match + B"}` + "\n"
	rowC  = `{"ts":"2025-01-01T00:00:02.000000001Z","line":"match + C"}` + "\n"
)

func argsFor(base string, extra ...string) []string {
	return append([]string{"-url", base, "-start", start, "-end", end}, extra...)
}

func cursorFor(b byte) string {
	return base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{b}, 40))
}

func servePage(t *testing.T, w http.ResponseWriter, next, body string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/x-ndjson")
	if next != "" {
		w.Header().Set("Bog-Next-Cursor", next)
	}
	if _, err := io.WriteString(w, body); err != nil {
		t.Errorf("write fixture page: %v", err)
	}
}

func assertResult(t *testing.T, status int, stdout, stderr string, wantStatus int, wantRows, diagnostic string) {
	t.Helper()
	if status != wantStatus || stdout != wantRows {
		t.Errorf("exit = %d, stdout = %q, stderr = %q; want exit %d and stdout %q", status, stdout, stderr, wantStatus, wantRows)
	}
	if diagnostic == "" && stderr != "" || !strings.Contains(stderr, diagnostic) {
		t.Errorf("stderr = %q, want %q", stderr, diagnostic)
	}
}

func TestExportPages(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		pages []string
	}{
		{"empty", []string{""}},
		{"single page", []string{rowA}},
		{"ties and duplicates across pages", []string{rowA + rowB, rowB + rowC}},
		{"empty terminal page", []string{rowB, ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				i := int(requests.Add(1)) - 1
				if i >= len(tc.pages) {
					t.Errorf("unexpected request %d", i+1)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				q := r.URL.Query()
				if r.Method != http.MethodGet || r.URL.EscapedPath() != "/proxy%2Fbog/query" ||
					q.Get("start") != start || q.Get("end") != end || q.Get("limit") != "2" || q.Get("contains") != "match +" {
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
				}
				if i == 0 && (q.Get("page") != "1" || q.Has("cursor")) ||
					i > 0 && (q.Has("page") || q.Get("cursor") != cursorFor(byte(i))) {
					t.Errorf("unexpected pagination on request %d: %s", i+1, r.URL)
				}
				var next string
				if i+1 < len(tc.pages) {
					next = cursorFor(byte(i + 1))
				}
				servePage(t, w, next, tc.pages[i])
			}))
			defer server.Close()
			// Each Write must receive exactly one complete record, even when
			// the text includes quotes and a logical newline.
			var stdout, stderr bytes.Buffer
			out := writerFunc(func(p []byte) (int, error) {
				if bytes.Count(p, []byte{'\n'}) != 1 || !bytes.HasSuffix(p, []byte{'\n'}) {
					t.Errorf("write is not exactly one NDJSON record: %q", p)
				}
				return stdout.Write(p)
			})
			args := argsFor(server.URL+"/proxy%2Fbog/", "-limit", "2", "-contains", "match +", "-max-pages", fmt.Sprint(len(tc.pages)))
			status := run(t.Context(), args, out, &stderr)
			assertResult(t, status, stdout.String(), stderr.String(), exitOK, strings.Join(tc.pages, ""), "")
			if got := int(requests.Load()); got != len(tc.pages) {
				t.Errorf("requests = %d, want %d", got, len(tc.pages))
			}
		})
	}
}

func TestExportIncompletePage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		flags      []string
		status     int
		body       string
		diagnostic string
		requests   int32
	}{
		{"page budget", []string{"-max-pages", "1"}, 0, "", "query page budget exhausted", 1},
		{"expired capture", nil, 410, "query capture expired\n", "query capture expired", 2},
		{"later malformed page", nil, 200, rowB + "not JSON\n", "malformed query page", 2},
		{"later oversized page", []string{"-max-page-bytes", fmt.Sprint(len(rowA))}, 200, rowB + rowC, "response exceeds size limit", 2},
		{"retry exhaustion", []string{"-max-retries", "2", "-retry-delay", "0s"}, 503, "busy\n", "query page retries exhausted", 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			cursor := cursorFor(42)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if requests.Add(1) == 1 {
					servePage(t, w, cursor, rowA)
					return
				}
				if r.URL.Query().Get("cursor") != cursor {
					t.Errorf("continuation changed: %s", r.URL)
				}
				switch tc.status {
				case 0:
					t.Error("requested a page beyond the page budget")
					w.WriteHeader(http.StatusBadRequest)
				case http.StatusOK:
					servePage(t, w, "", tc.body)
				default:
					http.Error(w, strings.TrimSuffix(tc.body, "\n"), tc.status)
				}
			}))
			defer server.Close()
			var stdout, stderr bytes.Buffer
			status := run(t.Context(), argsFor(server.URL, tc.flags...), &stdout, &stderr)
			assertResult(t, status, stdout.String(), stderr.String(), exitIncomplete, rowA, tc.diagnostic)
			if got := requests.Load(); got != tc.requests {
				t.Errorf("requests = %d, want %d", got, tc.requests)
			}
		})
	}
}

func TestExportRetriesBeforeWritingPage(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	cursor := cursorFor(42)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := requests.Add(1)
		if i == 1 {
			servePage(t, w, cursor, rowA)
			return
		}
		if r.URL.Query().Get("cursor") != cursor {
			t.Errorf("continuation changed on attempt %d: %s", i, r.URL)
		}
		switch i {
		case 2:
			http.Error(w, "busy", http.StatusServiceUnavailable)
		case 3:
			// The HTTP body ends prematurely, even though one row was delivered.
			w.Header().Set("Content-Length", fmt.Sprint(len(rowB)+len(rowC)))
			servePage(t, w, "", rowB)
		case 4:
			servePage(t, w, "", rowB+rowC)
		default:
			t.Errorf("unexpected attempt %d", i)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	var stdout, stderr bytes.Buffer
	status := run(t.Context(), argsFor(server.URL, "-max-pages", "2", "-retry-delay", "0s"), &stdout, &stderr)
	assertResult(t, status, stdout.String(), stderr.String(), exitOK, rowA+rowB+rowC, "")
	if got := requests.Load(); got != 4 {
		t.Errorf("requests = %d, want 4", got)
	}
}

func TestExportCancellation(t *testing.T) {
	t.Parallel()
	t.Run("during later page", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var requests atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if requests.Add(1) == 1 {
				servePage(t, w, cursorFor(1), rowA)
				return
			}
			cancel()
			<-r.Context().Done()
		}))
		defer server.Close()
		var stdout, stderr bytes.Buffer
		status := run(ctx, argsFor(server.URL), &stdout, &stderr)
		assertResult(t, status, stdout.String(), stderr.String(), exitIncomplete, rowA, "context canceled")
		if got := requests.Load(); got != 2 {
			t.Errorf("requests = %d, want 2", got)
		}
	})
	t.Run("during output", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			servePage(t, w, "", rowA+rowB)
		}))
		defer server.Close()
		var stdout, stderr bytes.Buffer
		out := writerFunc(func(p []byte) (int, error) {
			defer cancel()
			return stdout.Write(p)
		})
		status := run(ctx, argsFor(server.URL), out, &stderr)
		assertResult(t, status, stdout.String(), stderr.String(), exitIncomplete, rowA, "context canceled")
	})
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func TestExportOutputFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		partial    int
		err        error
		diagnostic string
	}{
		{"failure", 0, errors.New("disk full"), "disk full"},
		{"partial error", 5, errors.New("reader closed"), "reader closed"},
		{"short write", 5, nil, "short write"},
		{"no progress", 0, nil, "short write"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				servePage(t, w, cursorFor(1), rowA+rowB+rowC)
			}))
			defer server.Close()
			var stdout, stderr bytes.Buffer
			writes := 0
			out := writerFunc(func(p []byte) (int, error) {
				writes++
				if writes == 2 {
					n, err := stdout.Write(p[:tc.partial])
					return n, errors.Join(err, tc.err)
				}
				return stdout.Write(p)
			})
			status := run(t.Context(), argsFor(server.URL), out, &stderr)
			assertResult(t, status, stdout.String(), stderr.String(), exitIncomplete, rowA+rowB[:tc.partial], tc.diagnostic)
			if writes != 2 || requests.Load() != 1 || !strings.Contains(stderr.String(), "stdout (page 1, row 2)") {
				t.Errorf("writes = %d, requests = %d, stderr = %q", writes, requests.Load(), stderr.String())
			}
		})
	}
}

func TestExportBlockedOutput(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"deadline", "cancellation"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			reader, writer, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			closeFile := func(f *os.File) {
				if err := f.Close(); err != nil {
					t.Error(err)
				}
			}
			defer closeFile(reader)
			defer closeFile(writer)
			large := `{"ts":"2025-01-01T00:00:02Z","line":"` + strings.Repeat("x", 1<<20) + "\"}\n"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				servePage(t, w, "", rowA+large)
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			timeout := "1s"
			if name == "cancellation" {
				timeout = "10s"
			}
			var stderr bytes.Buffer
			result := make(chan int, 1)
			go func() { result <- run(ctx, argsFor(server.URL, "-timeout", timeout), writer, &stderr) }()
			// Wait until writing has started. Drain only the first complete row.
			readErr := reader.SetReadDeadline(time.Now().Add(5 * time.Second))
			first := make([]byte, len(rowA))
			if readErr == nil {
				_, readErr = io.ReadFull(reader, first)
			}
			if name == "cancellation" {
				cancel()
			}
			select {
			case status := <-result:
				if status != exitIncomplete || !strings.Contains(stderr.String(), "write stdout") {
					t.Errorf("exit = %d, stderr = %q", status, stderr.String())
				}
			case <-time.After(3 * time.Second):
				t.Error("blocked stdout did not stop")
				// Wake an incorrectly implemented writer before the test returns.
				if err := writer.SetWriteDeadline(time.Now()); err != nil {
					t.Fatal(err)
				}
				<-result
			}
			if readErr != nil || string(first) != rowA {
				t.Errorf("first complete row = %q, read error = %v", first, readErr)
			}
		})
	}
}
