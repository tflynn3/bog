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
)

func TestStreamBatchBoundaries(t *testing.T) {
	var batches [][]byte
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		batches = append(batches, b)
		w.WriteHeader(204)
	}))
	defer s.Close()
	line := strings.Repeat("x", MaxBatch/2) + "\n"
	if err := New(s.URL).IngestStream(context.Background(), strings.NewReader(line+line)); err != nil {
		t.Fatal(err)
	}
	if len(batches) != 2 || string(batches[0]) != line || string(batches[1]) != line {
		t.Fatal("batch split a line or exceeded limit")
	}
}
func TestOutcomes(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "busy", 503) }))
	c := New(s.URL)
	var rejected *RejectedError
	if e := c.Ingest(context.Background(), []byte("x")); !errors.As(e, &rejected) {
		t.Fatalf("expected rejection: %v", e)
	}
	s.Close()
	var unknown *UnknownOutcomeError
	if e := c.Ingest(context.Background(), []byte("x")); !errors.As(e, &unknown) {
		t.Fatalf("expected unknown: %v", e)
	}
}
func TestQueryPages(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("cursor") == "" {
			w.Header().Set("Bog-Next-Cursor", "next")
			fmt.Fprintln(w, "first")
		} else {
			fmt.Fprintln(w, "second")
		}
	}))
	defer s.Close()
	var out bytes.Buffer
	if e := New(s.URL).Query(context.Background(), Query{Start: "a", End: "b"}, &out); e != nil || out.String() != "first\nsecond\n" {
		t.Fatalf("%q %v", out.String(), e)
	}
}
