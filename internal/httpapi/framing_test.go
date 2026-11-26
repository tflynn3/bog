package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"

	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"bog/internal/client"
	"bog/internal/store"
)

func TestWriteQueryBody(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"empty", ""},
		{"small", "{\"ts\":\"2025-01-01T00:00:00Z\",\"line\":\"☃\"}\n"},
		{"larger than automatic length threshold", strings.Repeat("0123456789\n", 1024)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			w.Header().Set("Content-Length", "999")
			buf := bytes.NewBufferString(tc.body)
			if err := writeQueryBody(w, buf); err != nil {
				t.Fatal(err)
			}
			resp := w.Result()
			got, err := io.ReadAll(resp.Body)
			if err := errors.Join(err, resp.Body.Close()); err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusOK || resp.ContentLength != int64(len(tc.body)) || string(got) != tc.body {
				t.Fatalf("status %d, length %d, body length %d", resp.StatusCode, resp.ContentLength, len(got))
			}
		})
	}
}

type failedPageWriter struct {
	*httptest.ResponseRecorder
	err error
}

func (w failedPageWriter) Write(b []byte) (int, error) {
	if got := w.Header().Get("Content-Length"); got != strconv.Itoa(len(b)) {
		return 0, fmt.Errorf("length before write: %q", got)
	}
	return 0, w.err
}

func TestWriteQueryBodyFailure(t *testing.T) {
	for _, failure := range []error{io.ErrClosedPipe, nil} {
		want := failure
		if want == nil {
			want = io.ErrShortWrite
		}
		w := failedPageWriter{httptest.NewRecorder(), failure}
		if err := writeQueryBody(w, bytes.NewBufferString("row\n")); !errors.Is(err, want) {
			t.Errorf("write failure %v, want %v", err, want)
		}
	}
}

type queryContractAppender struct{ s *store.Store }

func (a queryContractAppender) Append(_ []byte, rows []store.Row) error {
	a.s.Publish(rows)
	return nil
}

func encodeContractRows(t *testing.T, rows ...store.Row) []byte {
	t.Helper()
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	return b.Bytes()
}

// interruptPage forwards the complete response header and one complete row,
// then closes the downstream connection at the row boundary.
func interruptPage(w http.ResponseWriter, page *httptest.ResponseRecorder) (err error) {
	if page.Code != http.StatusOK {
		return fmt.Errorf("interrupted page status %d", page.Code)
	}
	next := page.Header().Get("Bog-Next-Cursor")
	if next == "" {
		return errors.New("interrupted page has no continuation")
	}
	row, rest, found := bytes.Cut(page.Body.Bytes(), []byte{'\n'})
	if !found || len(rest) == 0 {
		return errors.New("interrupted page must contain multiple records")
	}
	conn, rw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(rw, "HTTP/1.0 200 OK\r\nContent-Length: %d\r\nBog-Next-Cursor: %s\r\n\r\n%s\n", page.Body.Len(), next, row); err != nil {
		return err
	}
	return rw.Flush()
}

func TestQueryPageRetryContract(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 0, 123456789, time.UTC)
	tieA := store.Row{TS: stamp, Line: "keep z"}
	tieB := store.Row{TS: stamp, Line: "keep a"}
	early := store.Row{TS: stamp.Add(-1), Line: "keep e"}
	late := store.Row{TS: stamp.Add(1), Line: "keep l"}
	seed := encodeContractRows(t, tieA, late, early, tieA, tieB, tieA,
		store.Row{TS: stamp, Line: "skip x"})
	backfill := encodeContractRows(t, store.Row{TS: stamp.Add(-1), Line: "keep n"})
	// Exactly two records fit, while the requested row limit permits all of them.
	capacity := len(encodeContractRows(t, tieA, tieB))
	st := &store.Store{}
	api := NewWithQueryMaxResponseBytes(st, queryContractAppender{st}, capacity)
	var continuations atomic.Int32
	requestURIs := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !r.URL.Query().Has("cursor") {
			api.ServeHTTP(w, r)
			return
		}
		select {
		case requestURIs <- r.RequestURI:
		default:
			t.Error("unexpected extra continuation request")
		}
		switch continuations.Add(1) {
		case 1:
			http.Error(w, "query capacity exhausted", http.StatusServiceUnavailable)
		case 2:
			page := httptest.NewRecorder()
			api.ServeHTTP(page, r)
			if err := interruptPage(w, page); err != nil {
				t.Error(err)
			}
		default:
			api.ServeHTTP(w, r)
		}
	}))
	t.Cleanup(server.Close)
	c, err := client.New(server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseIdleConnections)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if out, err := c.Ingest(ctx, seed); err != nil || out != client.IngestCommitted {
		t.Fatalf("initial ingest %v, error %v", out, err)
	}

	q := client.Query{Start: stamp.Add(-time.Second), End: stamp.Add(time.Second), Contains: "keep", Limit: 9}
	var pages []client.Page
	keep := func(page client.Page) error {
		pages = append(pages, page)
		return nil
	}
	cursor, err := c.Walk(ctx, q, func(page client.Page) error {
		pages = append(pages, page)
		out, err := c.Ingest(ctx, backfill)
		if err != nil || out != client.IngestCommitted {
			return fmt.Errorf("backfill outcome %v: %v", out, err)
		}
		return nil
	})
	var status *client.HTTPError
	if !errors.As(err, &status) || !status.Retryable() || status.StatusCode != http.StatusServiceUnavailable ||
		len(pages) != 1 || cursor == "" || cursor != pages[0].NextCursor {
		t.Fatalf("first walk cursor %q, pages %v, error %v", cursor, pages, err)
	}
	retry, err := c.WalkFrom(ctx, q, cursor, keep)
	if !errors.Is(err, io.ErrUnexpectedEOF) || !errors.As(err, &status) || status.Retryable() ||
		len(pages) != 1 || retry != cursor {
		t.Fatalf("interrupted walk cursor %q, pages %v, error %v", retry, pages, err)
	}
	terminal, err := c.WalkFrom(ctx, q, retry, keep)
	if err != nil || terminal != "" || len(pages) != 3 {
		t.Fatalf("final walk cursor %q, pages %v, error %v", terminal, pages, err)
	}
	want := [][]store.Row{{early, tieA}, {tieA, tieB}, {tieA, late}}
	for i, page := range pages {
		if !slices.Equal(page.Rows, want[i]) || (page.NextCursor == "") != (i == len(pages)-1) {
			t.Errorf("page %d = %+v, want rows %+v", i, page, want[i])
		}
	}
	if n := len(requestURIs); n != 4 {
		t.Fatalf("continuation requests %d, want 4", n)
	}
	firstURI := <-requestURIs
	for i := 0; i < 2; i++ {
		if uri := <-requestURIs; uri != firstURI {
			t.Errorf("retry target %q, want %q", uri, firstURI)
		}
	}
	if uri := <-requestURIs; uri == firstURI {
		t.Errorf("terminal request reused previous target %q", uri)
	}
}

func TestQueryBodyHTTP10(t *testing.T) {
	for _, body := range []string{"", strings.Repeat("row\n", 4096)} {
		t.Run(strconv.Itoa(len(body)), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Proto != "HTTP/1.0" {
					t.Errorf("protocol %s", r.Proto)
				}
				if err := writeQueryBody(w, bytes.NewBufferString(body)); err != nil {
					t.Error(err)
				}
			}))
			t.Cleanup(s.Close)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, resp := fetchHTTP10(t, req, 0)
			got, err := io.ReadAll(resp.Body)
			if err := errors.Join(err, resp.Body.Close()); err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusOK || resp.ContentLength != int64(len(body)) || string(got) != body {
				t.Fatalf("status %d, length %d, body length %d", resp.StatusCode, resp.ContentLength, len(got))
			}
		})
	}
}

func TestConcurrentQueryWalks(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []store.Row{{TS: stamp, Line: "z"}, {TS: stamp, Line: "a"}, {TS: stamp, Line: "z"}}
	st := &store.Store{}
	st.Publish(rows)
	s := httptest.NewServer(NewWithQueryMaxResponseBytes(st, nil, len(encodeContractRows(t, rows[0]))))
	t.Cleanup(s.Close)
	c, err := client.New(s.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseIdleConnections)
	for i := 0; i < 8; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			q := client.Query{Start: stamp, End: stamp.Add(time.Second)}
			var got []store.Row
			cursor, err := c.Walk(ctx, q, func(p client.Page) error {
				if len(p.Rows) != 1 || (p.NextCursor == "") != (len(got) == len(rows)-1) {
					return fmt.Errorf("page %d: %+v", len(got), p)
				}
				got = append(got, p.Rows...)
				return nil
			})
			if err != nil || cursor != "" || !slices.Equal(got, rows) {
				t.Fatalf("cursor %q, rows %v, error %v", cursor, got, err)
			}
			q.Contains = "no match"
			empty, err := c.FetchPage(ctx, q, "")
			if err != nil || len(empty.Rows) != 0 || empty.NextCursor != "" {
				t.Fatalf("empty page %+v, error %v", empty, err)
			}
		})
	}
}

func TestHTTP10PageDisconnectUnderBackpressure(t *testing.T) {
	const row = "{\"ts\":\"2025-01-01T00:00:00Z\",\"line\":\"continuation\"}\n"
	body := strings.Repeat(row, 64<<10)
	writeResult := make(chan error, 1)
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Bog-Next-Cursor", "next")
		writeResult <- writeQueryBody(w, bytes.NewBufferString(body))
	}))
	s.Config.WriteTimeout = 5 * time.Second
	s.Config.ConnContext = func(ctx context.Context, conn net.Conn) context.Context {
		if err := conn.(*net.TCPConn).SetWriteBuffer(4096); err != nil {
			t.Error(err)
		}
		return ctx
	}
	s.Start()
	t.Cleanup(s.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL+"/query?cursor=previous", nil)
	if err != nil {
		t.Fatal(err)
	}
	conn, resp := fetchHTTP10(t, req, 4096)
	defer func() {
		if err := resp.Body.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
	}()
	if resp.Proto != "HTTP/1.0" || resp.ContentLength != int64(len(body)) || resp.Header.Get("Bog-Next-Cursor") != "next" {
		t.Errorf("protocol %s, length %d, headers %v", resp.Proto, resp.ContentLength, resp.Header)
	}
	var first [len(row)]byte
	if _, err := io.ReadFull(resp.Body, first[:]); err != nil || string(first[:]) != row {
		t.Errorf("first row %q, error %v", first, err)
	}
	select {
	case err := <-writeResult:
		t.Errorf("write ended before disconnect: %v", err)
	default:
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
		select {
		case err := <-writeResult:
			var networkErr net.Error
			if err == nil || (errors.As(err, &networkErr) && networkErr.Timeout()) {
				t.Errorf("write after disconnect: %v", err)
			}
		case <-ctx.Done():
			t.Error("disconnect did not unblock the server write")
		}
	}
}

func fetchHTTP10(t *testing.T, req *http.Request, readBuffer int) (net.Conn, *http.Response) {
	t.Helper()
	var dialer net.Dialer
	conn, err := dialer.DialContext(req.Context(), "tcp", req.URL.Host)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
	})
	if readBuffer > 0 {
		if err := conn.(*net.TCPConn).SetReadBuffer(readBuffer); err != nil {
			t.Fatal(err)
		}
	}
	deadline, ok := req.Context().Deadline()
	if !ok {
		t.Fatal("request context needs a deadline")
	}
	if err := conn.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(conn, "GET %s HTTP/1.0\r\nHost: %s\r\n\r\n", req.URL.RequestURI(), req.URL.Host); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		t.Fatal(err)
	}
	return conn, resp
}
