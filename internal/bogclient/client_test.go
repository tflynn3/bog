package bogclient

import (
	"bytes"
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

func TestIngestStreamBoundsAndOrder(t *testing.T) {
	var batches [][]byte
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		batches = append(batches, b)
		w.WriteHeader(204)
	}))
	defer s.Close()
	c, _ := New(s.URL, nil)
	line := strings.Repeat("x", MaxBatch/2-1) + "\n"
	if err := c.IngestStream(context.Background(), strings.NewReader(line+line+"last\n")); err != nil {
		t.Fatal(err)
	}
	if len(batches) != 2 || len(batches[0]) != MaxBatch || string(batches[1]) != "last\n" {
		t.Fatalf("unexpected batches: %d", len(batches))
	}
}
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
			var rejected *RejectedError
			var unknown *UnknownOutcomeError
			if errors.As(err, &rejected) != tc.rejected || errors.As(err, &unknown) == tc.rejected {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
func TestLostReplyIsUnknown(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		conn, _, _ := w.(http.Hijacker).Hijack()
		conn.Close()
	}))
	defer s.Close()
	c, _ := New(s.URL, nil)
	err := c.Ingest(context.Background(), []byte("x"))
	var unknown *UnknownOutcomeError
	if !errors.As(err, &unknown) {
		t.Fatalf("expected unknown outcome: %v", err)
	}
}
func TestQueryPagesAndCursorInvalidation(t *testing.T) {
	calls := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			if r.URL.Query().Get("page") != "1" {
				t.Error("missing first page")
			}
			w.Header().Set("Bog-Next-Cursor", "token")
			fmt.Fprintln(w, `{"line":"a"}`)
			return
		}
		if r.URL.Query().Get("cursor") != "token" {
			t.Error("missing cursor")
		}
		http.Error(w, "invalid cursor", 400)
	}))
	defer s.Close()
	c, _ := New(s.URL, nil)
	var out bytes.Buffer
	err := c.QueryStream(context.Background(), Query{Start: time.Unix(0, 0), End: time.Unix(1, 0), Limit: 1}, &out)
	var invalid *CursorInvalidError
	if !errors.As(err, &invalid) || out.String() != "{\"line\":\"a\"}\n" {
		t.Fatalf("out=%q err=%v", out.String(), err)
	}
}
