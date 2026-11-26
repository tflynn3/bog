package bogclient

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

func TestIngestOutcomes(t *testing.T) {
	for _, tc := range []struct {
		status   int
		rejected bool
	}{{400, true}, {413, true}, {503, false}} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status) }))
			defer s.Close()
			c, _ := New(s.URL, nil)
			err := c.Ingest(context.Background(), []byte("x"))
			var r *RejectedError
			var u *UnknownOutcomeError
			if tc.rejected && !errors.As(err, &r) || !tc.rejected && !errors.As(err, &u) {
				t.Fatalf("unexpected error %v", err)
			}
		})
	}
}
func TestLostReplyIsNotRetried(t *testing.T) {
	calls := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		conn, _, _ := w.(http.Hijacker).Hijack()
		conn.Close()
	}))
	defer s.Close()
	c, _ := New(s.URL, nil)
	err := c.Ingest(context.Background(), []byte("x"))
	var u *UnknownOutcomeError
	if !errors.As(err, &u) || calls != 1 {
		t.Fatalf("error=%v calls=%d", err, calls)
	}
}
func TestQueryStreamsAndReportsInvalidCursor(t *testing.T) {
	calls := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Bog-Next-Cursor", "cursor")
			io.WriteString(w, "{\"ts\":\"2025-01-01T00:00:00.123456789Z\",\"line\":\"same\"}\n{\"ts\":\"2025-01-01T00:00:00.123456789Z\",\"line\":\"same\"}\n")
			return
		}
		if r.URL.Query().Get("cursor") != "cursor" {
			t.Error("missing cursor")
		}
		http.Error(w, "invalid cursor", 400)
	}))
	defer s.Close()
	c, _ := New(s.URL, nil)
	count := 0
	err := c.Query(context.Background(), Query{Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), End: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)}, func(r Row) error {
		count++
		if r.TS.Nanosecond() != 123456789 {
			t.Error(r.TS)
		}
		return nil
	})
	var ce *CursorInvalidError
	if count != 2 || !errors.As(err, &ce) {
		t.Fatalf("count=%d err=%v", count, err)
	}
}
func TestIngestStreamWireLimit(t *testing.T) {
	sizes := []int{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		sizes = append(sizes, len(b))
		w.WriteHeader(204)
	}))
	defer s.Close()
	c, _ := New(s.URL, nil)
	row := strings.Repeat("x", MaxBatch/2) + "\n"
	if err := c.IngestStream(context.Background(), strings.NewReader(row+row)); err != nil {
		t.Fatal(err)
	}
	if len(sizes) != 2 || sizes[0] > MaxBatch || sizes[1] > MaxBatch {
		t.Fatal(sizes)
	}
}
