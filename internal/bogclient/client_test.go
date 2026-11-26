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

func TestStreamBatchesPreserveLines(t *testing.T) {
	var batches [][]byte
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		batches = append(batches, b)
		w.WriteHeader(204)
	}))
	defer s.Close()
	c, _ := New(s.URL, nil)
	line := strings.Repeat("x", MaxBatch/2) + "\n"
	if err := c.IngestStream(context.Background(), strings.NewReader(line+line)); err != nil {
		t.Fatal(err)
	}
	if len(batches) != 2 || string(batches[0])+string(batches[1]) != line+line {
		t.Fatal("invalid batches")
	}
}
func TestWriteOutcomes(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	c, _ := New(s.URL, nil)
	err := c.Ingest(context.Background(), []byte("x"))
	var rejected *Rejected
	if !errors.As(err, &rejected) {
		t.Fatalf("expected rejection: %v", err)
	}
	s.Close()
	err = c.Ingest(context.Background(), []byte("x"))
	var unknown *UnknownOutcome
	if !errors.As(err, &unknown) {
		t.Fatalf("expected unknown: %v", err)
	}
}
func TestQueryPaginationAndInvalidation(t *testing.T) {
	n := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		if n == 1 {
			if r.URL.Query().Get("page") != "1" {
				t.Error("missing page")
			}
			w.Header().Set("Bog-Next-Cursor", "token")
			fmt.Fprintln(w, `{"ts":"a","line":"x"}`)
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
	err := c.Query(context.Background(), Query{Start: "a", End: "b"}, &out)
	var invalid *CursorInvalid
	if !errors.As(err, &invalid) || out.Len() == 0 {
		t.Fatalf("%v %q", err, out.String())
	}
}
