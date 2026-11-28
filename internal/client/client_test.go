package client

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var fixtureCursor = base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("a", 33)))
var fixtureNext = base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("b", 33)))

func TestPageFraming(t *testing.T) {
	row := "{\"ts\":\"2025-01-01T00:00:00.123456789Z\",\"line\":\"a\"}\n"
	tests := []struct {
		name, response string
		ok             bool
	}{
		{"comma joined cursor", fmt.Sprintf("Content-Length: %d\r\n\r\n%s", len(row), row), false},
		{"length", fmt.Sprintf("Content-Length: %d\r\n\r\n%s", len(row), row), true},
		{"truncated boundary", fmt.Sprintf("Content-Length: %d\r\n\r\n%s", len(row)+1, row), false},
		{"chunked", fmt.Sprintf("Transfer-Encoding: chunked\r\n\r\n%x\r\n%s\r\n0\r\n\r\n", len(row), row), true},
		{"broken chunk", fmt.Sprintf("Transfer-Encoding: chunked\r\n\r\n%x\r\n%s\r\n", len(row), row), false},
		{"close delimited", "\r\n" + row, false},
		{"missing newline", fmt.Sprintf("Content-Length: %d\r\n\r\n%s", len(row)-1, strings.TrimSuffix(row, "\n")), false},
		{"malformed", "Content-Length: 2\r\n\r\n{\n", false},
		{"malformed after row", fmt.Sprintf("Content-Length: %d\r\n\r\n%s{\n", len(row)+2, row), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := ln.Close(); err != nil {
					t.Error(err)
				}
			}()
			done := make(chan error, 1)
			go func() {
				conn, e := ln.Accept()
				if e != nil {
					done <- e
					return
				}
				defer func() { done <- errors.Join(e, conn.Close()) }()
				e = conn.SetDeadline(time.Now().Add(3 * time.Second))
				if e != nil {
					return
				}
				var req *http.Request
				req, e = http.ReadRequest(bufio.NewReader(conn))
				if e == nil {
					e = req.Body.Close()
				}
				if e == nil {
					cursor := fixtureCursor
					if tt.name == "comma joined cursor" {
						cursor += ", " + fixtureNext
					}
					_, e = io.WriteString(conn, "HTTP/1.1 200 OK\r\nBog-Next-Cursor: "+cursor+"\r\n"+tt.response)
				}
			}()
			c, err := New("http://"+ln.Addr().String(), nil)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			p, err := c.FetchPage(ctx, Query{}, "")
			if (err == nil) != tt.ok {
				t.Fatalf("page=%+v err=%v", p, err)
			}
			if tt.name == "truncated boundary" || tt.name == "broken chunk" {
				var he *HTTPError
				if !errors.As(err, &he) || he.StatusCode != http.StatusOK ||
					!errors.Is(err, io.ErrUnexpectedEOF) || !errors.Is(err, ErrPageInterrupted) ||
					errors.Is(err, ErrMalformedPage) || errors.Is(err, ErrPageBytes) {
					t.Fatalf("status/cause: %v", err)
				}
			}
			if !tt.ok && (len(p.Rows) != 0 || p.NextCursor != "") {
				t.Fatalf("partial page: %+v", p)
			}
			if tt.ok && (len(p.Rows) != 1 || p.Rows[0].TS.Nanosecond() != 123456789 || p.NextCursor != fixtureCursor) {
				t.Fatalf("page %+v", p)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWalkAndStatuses(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		q := r.URL.Query()
		if q.Get("start") != "2025-01-01T00:00:00Z" || q.Get("end") != "2025-01-02T00:00:00Z" || q.Get("contains") != "needle" {
			t.Errorf("query: %v", q)
		}
		switch calls {
		case 1:
			if q.Get("page") != "1" {
				t.Errorf("first: %v", q)
			}
			writeContractPage(t, w, contractRow("first"), fixtureCursor)
		case 2:
			if q.Get("cursor") != fixtureCursor || q.Has("page") {
				t.Errorf("second: %v", q)
			}
			writeContractPage(t, w, contractRow("second"), fixtureNext)
		case 3:
			if q.Get("cursor") != fixtureNext {
				t.Errorf("third: %v", q)
			}
			writeContractPage(t, w, contractRow("x"), "")
		}
	}))
	defer srv.Close()
	c, err := New(srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	start, _ := time.Parse(time.RFC3339, "2025-01-01T00:00:00Z")
	end := start.Add(24 * time.Hour)
	pages := 0
	cursor, err := c.Walk(context.Background(), Query{Start: start, End: end, Contains: "needle"}, func(Page) error { pages++; return nil })
	if err != nil || cursor != "" || pages != 3 {
		t.Fatalf("cursor %q pages %d err %v", cursor, pages, err)
	}
	for _, status := range []int{400, 422, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "failure", status) }))
			defer s.Close()
			c, e := New(s.URL, nil)
			if e != nil {
				t.Fatal(e)
			}
			_, e = c.FetchPage(context.Background(), Query{}, "token")
			var he *HTTPError
			if !errors.As(e, &he) || he.StatusCode != status || he.Retryable() != (status == 503) {
				t.Fatalf("%v", e)
			}
		})
	}
}

func TestIngestNoReplay(t *testing.T) {
	for _, code := range []int{301, 302, 303, 307, 308} {
		t.Run(fmt.Sprintf("redirect %d", code), func(t *testing.T) {
			var count, connections atomic.Int32
			s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				http.Redirect(w, r, "/other", code)
			}))
			s.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					connections.Add(1)
				}
			}
			s.Start()
			defer s.Close()
			c, e := New(s.URL, nil)
			if e != nil {
				t.Fatal(e)
			}
			out, e := c.Ingest(context.Background(), []byte("x"))
			if out != IngestUnknown || e == nil || count.Load() != 1 || connections.Load() != 1 {
				t.Fatalf("%v %v %d", out, e, count.Load())
			}
		})
	}
	t.Run("reused connection dropped acknowledgment", func(t *testing.T) {
		var count, connections atomic.Int32
		var firstRemote string
		s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := count.Add(1)
			if n == 1 {
				firstRemote = r.RemoteAddr
			} else if firstRemote != r.RemoteAddr {
				t.Error("connection was not reused")
			}
			if _, e := io.Copy(io.Discard, r.Body); e != nil {
				t.Error(e)
			}
			if n == 1 {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			h, ok := w.(http.Hijacker)
			if !ok {
				t.Error("no hijacker")
				return
			}
			conn, _, e := h.Hijack()
			if e != nil {
				t.Error(e)
				return
			}
			if e = conn.Close(); e != nil {
				t.Error(e)
			}
		}))
		s.Config.ConnState = func(_ net.Conn, state http.ConnState) {
			if state == http.StateNew {
				connections.Add(1)
			}
		}
		s.Start()
		defer s.Close()
		c, e := New(s.URL, nil)
		if e != nil {
			t.Fatal(e)
		}
		out, e := c.Ingest(context.Background(), []byte("x"))
		if out != IngestCommitted || e != nil {
			t.Fatalf("first %v %v", out, e)
		}
		out, e = c.Ingest(context.Background(), []byte("x"))
		if out != IngestUnknown || e == nil || count.Load() != 2 || connections.Load() != 1 {
			t.Fatalf("second %v %v count %d", out, e, count.Load())
		}
	})
}

func TestWalkRetryAndConsumption(t *testing.T) {
	var requests []string
	var mu sync.Mutex
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		requests = append(requests, r.URL.RawQuery)
		switch len(requests) {
		case 1:
			writeContractPage(t, w, contractRow("first"), fixtureCursor)
		case 2:
			http.Error(w, "busy", 503)
		case 3:
			writeContractPage(t, w, contractRow("resumed"), fixtureNext)
		}
	}))
	defer s.Close()
	c, err := New(s.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	q := Query{Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), End: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC), Contains: "test"}
	cursor, err := c.Walk(context.Background(), q, func(Page) error { return nil })
	if cursor != fixtureCursor || err == nil {
		t.Fatalf("cursor %q error %v", cursor, err)
	}
	consumeErr := errors.New("consumer failed")
	cursor, err = c.WalkFrom(context.Background(), q, cursor, func(Page) error { return consumeErr })
	mu.Lock()
	defer mu.Unlock()
	if cursor != fixtureCursor || !errors.Is(err, consumeErr) || len(requests) != 3 || requests[1] != requests[2] {
		t.Fatalf("cursor %q error %v requests %v", cursor, err, requests)
	}
}

func TestIngestFreshDroppedAcknowledgment(t *testing.T) {
	var requests, connections atomic.Int32
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Error(err)
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		if err = conn.Close(); err != nil {
			t.Error(err)
		}
	}))
	s.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	s.Start()
	defer s.Close()
	c, err := New(s.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.Ingest(context.Background(), []byte("batch"))
	if out != IngestUnknown || err == nil || requests.Load() != 1 || connections.Load() != 1 {
		t.Fatalf("out %v err %v requests %d connections %d", out, err, requests.Load(), connections.Load())
	}
}

func TestIngestOutcomes(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		want   IngestOutcome
	}{{204, "", IngestCommitted}, {400, "bad\n", IngestRejected}, {413, "large\n", IngestRejected}, {503, "busy\n", IngestUncommitted}, {503, "WAL unavailable\n", IngestUncommitted}, {503, "disk full\n", IngestUncommitted}, {503, "disk full", IngestUnknown}, {503, "disk full\nextra", IngestUnknown}, {503, "proxy\n", IngestUnknown}} {
		t.Run(fmt.Sprint(tc.status, tc.body), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				if _, err := io.WriteString(w, tc.body); err != nil {
					t.Error(err)
				}
			}))
			defer s.Close()
			c, err := New(s.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.Ingest(context.Background(), []byte("batch"))
			if got != tc.want || (tc.status == 204) != (err == nil) {
				t.Fatalf("outcome %v err %v", got, err)
			}
		})
	}
	c, err := New("http://127.0.0.1:1", nil)
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.Ingest(context.Background(), make([]byte, MaxBatch+1))
	if out != IngestRejected || err == nil {
		t.Fatalf("limit %v %v", out, err)
	}
}

func TestIngestCancellationAfterDispatch(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprint("deadline=", deadline), func(t *testing.T) {
			received := make(chan struct{}, 2)
			release := make(chan struct{})
			var releaseOnce sync.Once
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					t.Error(err)
				}
				received <- struct{}{}
				select {
				case <-release:
				case <-time.After(3 * time.Second):
					t.Error("handler timed out")
				}
				w.WriteHeader(204)
			}))
			defer s.Close()
			defer releaseOnce.Do(func() { close(release) })
			c, err := New(s.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			var ctx context.Context
			var cancel context.CancelFunc
			if deadline {
				ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
			} else {
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()
			done := make(chan struct {
				out IngestOutcome
				err error
			}, 1)
			go func() {
				out, err := c.Ingest(ctx, []byte("batch"))
				done <- struct {
					out IngestOutcome
					err error
				}{out, err}
			}()
			select {
			case <-received:
			case <-time.After(2 * time.Second):
				t.Fatal("request not dispatched")
			}
			if !deadline {
				cancel()
			}
			select {
			case result := <-done:
				want := context.Canceled
				if deadline {
					want = context.DeadlineExceeded
				}
				if result.out != IngestUnknown || !errors.Is(result.err, want) {
					t.Fatalf("%v %v", result.out, result.err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("request did not terminate")
			}
		})
	}
}

func TestCursorRejected(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "invalid cursor", 400) }))
	defer s.Close()
	c, err := New(s.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.FetchPage(context.Background(), Query{}, "token")
	var e *HTTPError
	if !errors.As(err, &e) || !e.CursorRejected || e.StatusCode != 400 {
		t.Fatalf("%v", err)
	}
}

func TestDuplicateTimestampOrder(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, line := range []string{"a", "a", "b"} {
			if _, err := fmt.Fprintf(w, "{\"ts\":\"2025-01-01T00:00:00.123456789Z\",\"line\":%q}\n", line); err != nil {
				t.Error(err)
			}
		}
	}))
	defer s.Close()
	c, err := New(s.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.FetchPage(context.Background(), Query{}, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Rows) != 3 || p.Rows[0].Line != "a" || p.Rows[1].Line != "a" || p.Rows[2].Line != "b" || p.Rows[2].TS.Nanosecond() != 123456789 {
		t.Fatalf("rows %+v", p.Rows)
	}
}

func TestIngestDiskFullResponseFraming(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		length     int64
		readErr    error
		want       IngestOutcome
	}{
		{"complete", "disk full\n", 10, nil, IngestUncommitted},
		{"short body", "disk full", 10, nil, IngestUnknown},
		{"truncated after exact bytes", "disk full\n", 11, io.ErrUnexpectedEOF, IngestUnknown},
		{"unframed", "disk full\n", -1, nil, IngestUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := New("http://example.test", nil)
			if err != nil {
				t.Fatal(err)
			}
			c.http.Transport = queryRoundTripper(func(r *http.Request) (*http.Response, error) {
				var body io.Reader = strings.NewReader(tc.body)
				if tc.readErr != nil {
					body = io.MultiReader(body, errorReader{tc.readErr})
				}
				return &http.Response{StatusCode: 503, ProtoMajor: 1, ContentLength: tc.length, Body: io.NopCloser(body), Header: make(http.Header)}, nil
			})
			got, err := c.Ingest(context.Background(), []byte("batch"))
			if got != tc.want || err == nil {
				t.Fatalf("outcome=%v error=%v", got, err)
			}
		})
	}
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }
