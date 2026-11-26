package client

import (
	"context"
	"errors"
	"fmt"
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
