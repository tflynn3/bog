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
	"time"
)

func TestIngestOutcomes(t *testing.T) {
	status := http.StatusServiceUnavailable
	calls := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(status) }))
	defer s.Close()
	c := New(s.URL)
	var unknown *UnknownOutcomeError
	if err := c.Ingest(context.Background(), []byte("x")); !errors.As(err, &unknown) {
		t.Fatalf("expected unknown: %v", err)
	}
	status = 400
	var rejected *RejectedError
	if err := c.Ingest(context.Background(), []byte("x")); !errors.As(err, &rejected) {
		t.Fatalf("expected rejection: %v", err)
	}
	if calls != 2 {
		t.Fatalf("unexpected retries: %d", calls)
	}
}
func TestQueryPages(t *testing.T) {
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
		fmt.Fprintln(w, `{"ts":"2025-01-01T00:00:00.000000001Z","line":"same"}`)
	}))
	defer s.Close()
	var out bytes.Buffer
	err := New(s.URL).Query(context.Background(), Query{Start: time.Now(), End: time.Now()}, &out)
	if err != nil || calls != 2 || strings.Count(out.String(), "same") != 2 {
		t.Fatalf("%v %d %q", err, calls, out.String())
	}
}
func TestBatchBoundaries(t *testing.T) {
	sizes := []int{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b bytes.Buffer
		b.ReadFrom(r.Body)
		sizes = append(sizes, b.Len())
		w.WriteHeader(204)
	}))
	defer s.Close()
	line := strings.Repeat("x", MaxBatch/2) + "\n"
	if err := New(s.URL).IngestNDJSON(context.Background(), strings.NewReader(line+line)); err != nil {
		t.Fatal(err)
	}
	if len(sizes) != 2 || sizes[0] > MaxBatch || sizes[1] > MaxBatch {
		t.Fatal(sizes)
	}
}
