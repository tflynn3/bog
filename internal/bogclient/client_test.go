package bogclient

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIngestOutcomes(t *testing.T) {
	status := http.StatusNoContent
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
	defer s.Close()
	c, err := New(s.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Ingest(context.Background(), []byte("x")); err != nil {
		t.Fatal(err)
	}
	status = http.StatusServiceUnavailable
	var rejected *Rejected
	if err := c.Ingest(context.Background(), []byte("x")); !errors.As(err, &rejected) {
		t.Fatalf("expected rejection, got %v", err)
	}
	status = http.StatusBadGateway
	var unknown *UnknownOutcome
	if err := c.Ingest(context.Background(), []byte("x")); !errors.As(err, &unknown) {
		t.Fatalf("expected unknown outcome, got %v", err)
	}
}
func TestQueryPaginationAndInvalidation(t *testing.T) {
	calls := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			if r.URL.Query().Get("page") != "1" {
				t.Error("missing page")
			}
			w.Header().Set("Bog-Next-Cursor", "token")
			io.WriteString(w, "first\n")
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
	if !errors.As(err, &invalid) || out.String() != "first\n" {
		t.Fatalf("output %q error %v", out.String(), err)
	}
}
func TestOversizedBatch(t *testing.T) {
	c, _ := New("http://localhost", nil)
	var rejected *Rejected
	if err := c.Ingest(context.Background(), []byte(strings.Repeat("x", MaxBatch+1))); !errors.As(err, &rejected) {
		t.Fatalf("got %v", err)
	}
}
