package bogclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIngestStreamBatchBoundaries(t *testing.T) {
	var batches [][]byte
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b bytes.Buffer
		b.ReadFrom(r.Body)
		batches = append(batches, b.Bytes())
		w.WriteHeader(204)
	}))
	defer s.Close()
	c, _ := New(s.URL, nil)
	line := strings.Repeat("x", MaxBatch-1) + "\n"
	if err := c.IngestStream(context.Background(), strings.NewReader(line+"next\n")); err != nil {
		t.Fatal(err)
	}
	if len(batches) != 2 || len(batches[0]) != MaxBatch || string(batches[1]) != "next\n" {
		t.Fatalf("unexpected batches: %d", len(batches))
	}
}
func TestQueryPagination(t *testing.T) {
	calls := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			if r.URL.Query().Get("page") != "1" {
				t.Error("missing page")
			}
			w.Header().Set("Bog-Next-Cursor", "token")
		} else if r.URL.Query().Get("cursor") != "token" {
			t.Error("missing cursor")
		}
		fmt.Fprintln(w, `{"ts":"2025-01-01T00:00:00.000000001Z","line":"x"}`)
	}))
	defer s.Close()
	c, _ := New(s.URL, nil)
	var out bytes.Buffer
	if err := c.Query(context.Background(), Query{Start: "a", End: "b"}, &out); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || strings.Count(out.String(), ".000000001Z") != 2 {
		t.Fatal(out.String())
	}
}
func TestOutcomes(t *testing.T) {
	for _, tc := range []struct {
		status   int
		rejected bool
	}{{400, true}, {503, true}, {500, false}} {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status) }))
		c, _ := New(s.URL, nil)
		err := c.Ingest(context.Background(), []byte("x"))
		var rejected *RejectedError
		var unknown *UnknownOutcomeError
		if tc.rejected && !errors.As(err, &rejected) || !tc.rejected && !errors.As(err, &unknown) {
			t.Fatalf("status %d: %v", tc.status, err)
		}
		s.Close()
	}
}
