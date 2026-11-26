package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const record = "{\"ts\":\"2025-01-01T00:00:00Z\",\"line\":\"hello\"}\n"

func testClient(t *testing.T, handler http.HandlerFunc, limit int64) *Client {
	t.Helper()
	s := httptest.NewServer(handler)
	t.Cleanup(s.Close)
	c, err := New(s.URL, s.Client(), limit)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func query() Query {
	return Query{Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), End: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC), Contains: "a & b", Limit: 10, Cursor: "original"}
}

func TestPageFailureDoesNotExposeData(t *testing.T) {
	tests := []struct {
		name, response string
		limit          int64
	}{
		{"mid row", "HTTP/1.1 200 OK\r\nContent-Length: 100\r\nBog-Next-Cursor: next\r\n\r\n" + record[:10], 1000},
		{"row boundary", "HTTP/1.1 200 OK\r\nContent-Length: 100\r\nBog-Next-Cursor: next\r\n\r\n" + record, 1000},
		{"incomplete chunk", fmt.Sprintf("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nBog-Next-Cursor: next\r\n\r\n%x\r\n%s\r\n", len(record), record), 1000},
		{"EOF delimited", "HTTP/1.1 200 OK\r\nConnection: close\r\nBog-Next-Cursor: next\r\n\r\n" + record, 1000},
		{"malformed record", fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\nBog-Next-Cursor: next\r\n\r\n%s", len(record)+4, record+"bad\n"), 1000},
		{"oversized", fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\nBog-Next-Cursor: next\r\n\r\n%s", len(record), record), 5},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.Close()
				_, _ = io.WriteString(conn, tc.response)
			}, tc.limit)
			page, err := c.FetchPage(context.Background(), query())
			if err == nil || len(page.Rows) != 0 || page.NextCursor != "" {
				t.Fatalf("page=%+v err=%v", page, err)
			}
		})
	}
}

func TestPagesAndRetry(t *testing.T) {
	calls := 0
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		q := r.URL.Query()
		if q.Get("start") != "2025-01-01T00:00:00Z" || q.Get("end") != "2025-01-02T00:00:00Z" || q.Get("contains") != "a & b" || q.Get("limit") != "10" {
			t.Errorf("query: %v", q)
		}
		if calls == 1 {
			if q.Get("page") != "1" || q.Has("cursor") {
				t.Errorf("first query: %v", q)
			}
			w.Header().Set("Bog-Next-Cursor", "original")
			fmt.Fprint(w, record)
			return
		}
		if q.Get("cursor") != "original" || q.Has("page") {
			t.Errorf("continuation: %v", q)
		}
		if calls == 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		// An empty, explicitly framed final page is valid.
		w.Header().Set("Content-Length", "0")
	}, 1000)
	q := query()
	q.Cursor = ""
	page, err := c.FetchPage(context.Background(), q)
	if err != nil || len(page.Rows) != 1 || page.Rows[0].Line != "hello" || page.NextCursor != "original" {
		t.Fatalf("first: %+v %v", page, err)
	}
	q.Cursor = page.NextCursor
	failed, err := c.FetchPage(context.Background(), q)
	if !errors.Is(err, ErrRetryable) || len(failed.Rows) != 0 || failed.NextCursor != "" || q.Cursor != "original" {
		t.Fatalf("retry: %+v %v", failed, err)
	}
	last, err := c.FetchPage(context.Background(), q)
	if err != nil || len(last.Rows) != 0 || last.NextCursor != "" {
		t.Fatalf("last: %+v %v", last, err)
	}
}

func TestIngestOutcomes(t *testing.T) {
	t.Run("acknowledged", func(t *testing.T) {
		c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			if string(b) != record {
				t.Errorf("body: %q", b)
			}
			w.WriteHeader(204)
		}, 1000)
		if err := c.Ingest(context.Background(), []byte(record)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("lost response", func(t *testing.T) {
		calls := 0
		c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
			calls++
			b, _ := io.ReadAll(r.Body)
			if string(b) != record {
				t.Errorf("body: %q", b)
			}
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
		}, 1000)
		err := c.Ingest(context.Background(), []byte(record))
		if !errors.Is(err, ErrAmbiguous) || calls != 1 {
			t.Fatalf("err=%v calls=%d", err, calls)
		}
	})
	for _, status := range []int{301, 302, 303, 307, 308} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			calls := 0
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Location", "/other")
				w.WriteHeader(status)
			}, 1000)
			err := c.Ingest(context.Background(), []byte(record))
			if err == nil || errors.Is(err, ErrAmbiguous) || calls != 1 || !strings.Contains(err.Error(), fmt.Sprint(status)) {
				t.Fatalf("err=%v calls=%d", err, calls)
			}
		})
	}
}
