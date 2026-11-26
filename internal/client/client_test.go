package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSnapshotRetriesWithoutAdvancing(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		q := r.URL.Query()
		if q.Get("contains") != "a & b" || q.Get("start") != "2025-01-01T00:00:00Z" || q.Get("end") != "2025-01-02T00:00:00Z" {
			t.Errorf("query changed: %v", q)
		}
		switch calls {
		case 1:
			if q.Get("page") != "1" {
				t.Errorf("missing page: %v", q)
			}
			w.Header().Set("Bog-Next-Cursor", "opaque+/=")
			fmt.Fprintln(w, `{"ts":"2025-01-01T00:00:00.123456789Z","line":"first"}`)
		case 2, 3, 4, 5:
			if q.Get("cursor") != "opaque+/=" || q.Has("page") {
				t.Errorf("cursor changed: %v", q)
			}
			switch calls {
			case 2:
				http.Error(w, "busy", 503)
			case 3:
				w.Header().Set("Bog-Next-Cursor", "wrong")
				fmt.Fprint(w, `{"ts":"2025-01-01T00:00:00Z","line":`)
			case 4:
				w.Header().Set("Content-Length", "100")
				fmt.Fprintln(w, `{"ts":"2025-01-01T00:00:00Z","line":"truncated"}`)
			case 5:
				fmt.Fprintln(w, `{"ts":"2025-01-01T00:00:00.123456789Z","line":"first"}`)
			}
		default:
			t.Error("unexpected request")
		}
	}))
	defer server.Close()
	c, err := New(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	start, _ := time.Parse(time.RFC3339, "2025-01-01T00:00:00Z")
	s, err := c.Snapshot(Query{Start: start, End: start.Add(24 * time.Hour), Contains: "a & b"})
	if err != nil {
		t.Fatal(err)
	}
	rows, done, err := s.Next(context.Background())
	if err != nil || done || len(rows) != 1 || rows[0].TS.Nanosecond() != 123456789 {
		t.Fatalf("first page: %v %v %v", rows, done, err)
	}
	for i := 0; i < 3; i++ {
		rows, done, err = s.Next(context.Background())
		if err == nil || done || len(rows) != 0 {
			t.Fatalf("failed page %d: %v %v %v", i, rows, done, err)
		}
		if i == 0 {
			var he *HTTPError
			if !errors.As(err, &he) || he.StatusCode != 503 {
				t.Fatalf("status: %v", err)
			}
		}
	}
	rows, done, err = s.Next(context.Background())
	if err != nil || !done || len(rows) != 1 || rows[0].Line != "first" {
		t.Fatalf("last page: %v %v %v", rows, done, err)
	}
}

func TestIngestOutcomesAndNoReplay(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		outcome IngestOutcome
	}{{"committed", 204, IngestCommitted}, {"rejected", 400, IngestRejected}, {"large", 413, IngestRejected}, {"busy", 503, IngestUncommitted}, {"redirect", 307, IngestUnknown}, {"server error", 500, IngestUnknown}, {"lost response", 0, IngestUnknown}} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Header.Get("Idempotency-Key") != "" || r.Header.Get("X-Idempotency-Key") != "" {
					t.Error("unexpected idempotency key")
				}
				if _, err := io.ReadAll(r.Body); err != nil {
					t.Errorf("read ingest: %v", err)
				}
				if tc.status == 0 {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					conn.Close()
					return
				}
				if tc.status == 307 {
					w.Header().Set("Location", "/ingest")
				}
				if tc.status == 503 {
					http.Error(w, "busy", tc.status)
					return
				}
				w.WriteHeader(tc.status)
			}))
			defer server.Close()
			c, _ := New(server.URL, server.Client())
			outcome, err := c.Ingest(context.Background(), []byte("batch"))
			if outcome != tc.outcome || (err == nil) != (tc.status == 204) || calls != 1 {
				t.Fatalf("outcome=%v error=%v calls=%d", outcome, err, calls)
			}
		})
	}
}

func TestPageValidationAndStatuses(t *testing.T) {
	for _, body := range []string{`{"ts":"2025-01-01T00:00:00Z","line":"x"}`, "{}\n", "\n", strings.Repeat("x", 20)} {
		t.Run(fmt.Sprintf("%q", body), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))
			defer server.Close()
			c, _ := New(server.URL, server.Client())
			c.MaxPageBytes = 128
			s, _ := c.Snapshot(Query{})
			if rows, _, err := s.Next(context.Background()); err == nil || len(rows) != 0 {
				t.Fatalf("rows=%v err=%v", rows, err)
			}
		})
	}
	for _, status := range []int{400, 422} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "failure", status) }))
		c, _ := New(server.URL, server.Client())
		s, _ := c.Snapshot(Query{})
		_, _, err := s.Next(context.Background())
		var he *HTTPError
		if !errors.As(err, &he) || he.StatusCode != status {
			t.Errorf("status %d: %v", status, err)
		}
		server.Close()
	}
}

func TestEscapedExpandedQueryRow(t *testing.T) {
	line := strings.Repeat(`\u003c`, 700000)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(Row{TS: time.Date(2025, 1, 1, 0, 0, 0, 123, time.UTC), Line: line})
	}))
	defer server.Close()
	c, _ := New(server.URL, server.Client())
	s, _ := c.Snapshot(Query{})
	rows, done, err := s.Next(context.Background())
	if err != nil || !done || len(rows) != 1 || rows[0].Line != line {
		t.Fatalf("large row: count=%d done=%v err=%v", len(rows), done, err)
	}
}

func TestCloseDelimitedPageDoesNotAdvance(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Query().Get("page") != "1" {
			t.Error("first page cursor advanced")
		}
		if calls == 1 {
			conn, buf, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			fmt.Fprint(buf, "HTTP/1.1 200 OK\r\nConnection: close\r\nBog-Next-Cursor: partial\r\n\r\n{\"ts\":\"2025-01-01T00:00:00Z\",\"line\":\"partial\"}\n")
			buf.Flush()
			conn.Close()
			return
		}
		fmt.Fprintln(w, `{"ts":"2025-01-01T00:00:00Z","line":"complete"}`)
	}))
	defer server.Close()
	c, _ := New(server.URL, server.Client())
	s, _ := c.Snapshot(Query{})
	if rows, _, err := s.Next(context.Background()); err == nil || len(rows) != 0 {
		t.Fatalf("partial page accepted: %v %v", rows, err)
	}
	rows, done, err := s.Next(context.Background())
	if err != nil || !done || len(rows) != 1 || rows[0].Line != "complete" {
		t.Fatalf("retry: %v %v %v", rows, done, err)
	}
}

type cancelOnEOF struct {
	io.Reader
	cancel context.CancelFunc
}

func (b cancelOnEOF) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	if err == io.EOF {
		b.cancel()
	}
	return n, err
}
func (b cancelOnEOF) Close() error { return nil }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCancellationBeforeAdvance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	body := "{\"ts\":\"2025-01-01T00:00:00Z\",\"line\":\"x\"}\n"
	calls := 0
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.Query().Get("page") != "1" {
			t.Error("cursor advanced")
		}
		var reader io.ReadCloser = io.NopCloser(strings.NewReader(body))
		if calls == 1 {
			reader = cancelOnEOF{strings.NewReader(body), cancel}
		}
		return &http.Response{StatusCode: 200, ProtoMajor: 1, ContentLength: int64(len(body)), Header: http.Header{"Bog-Next-Cursor": {"next"}}, Body: reader}, nil
	})}
	c, _ := New("http://example.com", hc)
	s, _ := c.Snapshot(Query{})
	if rows, _, err := s.Next(ctx); !errors.Is(err, context.Canceled) || len(rows) != 0 {
		t.Fatalf("canceled: %v %v", rows, err)
	}
	if _, _, err := s.Next(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestQueryBoundsUseUTC(t *testing.T) {
	zone := time.FixedZone("odd", 30)
	start := time.Date(2025, 1, 1, 0, 0, 0, 123456789, zone)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("start") != "2024-12-31T23:59:30.123456789Z" || q.Get("end") != "2025-01-01T00:00:30.123456789Z" {
			t.Errorf("incorrect bounds: %v", q)
		}
	}))
	defer server.Close()
	c, _ := New(server.URL, server.Client())
	s, _ := c.Snapshot(Query{Start: start, End: start.Add(time.Minute)})
	if _, _, err := s.Next(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestIngest503RequiresCompleteBogRejection(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		truncated  bool
		outcome    IngestOutcome
	}{
		{"busy", "busy\n", false, IngestUncommitted},
		{"wal", "WAL unavailable\n", false, IngestUncommitted},
		{"gateway", "upstream unavailable\n", false, IngestUnknown},
		{"truncated", "busy\n", true, IngestUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				if tc.truncated {
					w.Header().Set("Content-Length", "50")
				}
				w.WriteHeader(503)
				fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			c, _ := New(server.URL, server.Client())
			out, err := c.Ingest(context.Background(), []byte("batch"))
			var he *HTTPError
			if out != tc.outcome || !errors.As(err, &he) || he.StatusCode != 503 {
				t.Fatalf("outcome=%v err=%v", out, err)
			}
			if tc.truncated && he.Cause == nil {
				t.Fatal("missing read cause")
			}
		})
	}
}

func TestIngestCancellationAfterDispatchIsUnknown(t *testing.T) {
	received := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(received)
		<-release
	}))
	defer server.Close()
	c, _ := New(server.URL, server.Client())
	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		outcome IngestOutcome
		err     error
	}
	resultCh := make(chan result, 1)
	go func() { outcome, err := c.Ingest(ctx, []byte("batch")); resultCh <- result{outcome, err} }()
	<-received
	cancel()
	got := <-resultCh
	close(release)
	if got.outcome != IngestUnknown || !errors.Is(got.err, context.Canceled) {
		t.Fatalf("outcome=%v err=%v", got.outcome, got.err)
	}
}
