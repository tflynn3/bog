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

func TestIngestBatchesAndUnknownOutcome(t *testing.T) {
	var bodies [][]byte
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, b)
		w.WriteHeader(204)
	}))
	defer s.Close()
	c := New(s.URL)
	line := strings.Repeat("x", MaxBatch/2) + "\n"
	if err := c.Ingest(context.Background(), strings.NewReader(line+line+"last")); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 || len(bodies[0]) > MaxBatch || string(bytes.Join(bodies, nil)) != line+line+"last" {
		t.Fatal("batch boundaries changed input")
	}
	s.Close()
	err := c.IngestBatch(context.Background(), []byte("row"))
	var unknown *UnknownOutcomeError
	if !errors.As(err, &unknown) {
		t.Fatalf("expected unknown outcome, got %v", err)
	}
}
func TestRejectAndCursorInvalidation(t *testing.T) {
	calls := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ingest" {
			http.Error(w, "busy", 503)
			return
		}
		calls++
		if calls == 1 {
			w.Header().Set("Bog-Next-Cursor", "opaque")
			fmt.Fprintln(w, `{"ts":"2025-01-01T00:00:00Z","line":"x"}`)
			return
		}
		http.Error(w, "invalid cursor", 400)
	}))
	defer s.Close()
	c := New(s.URL)
	var rejected *RejectedError
	if err := c.IngestBatch(context.Background(), []byte("x")); !errors.As(err, &rejected) {
		t.Fatalf("expected rejection: %v", err)
	}
	var out bytes.Buffer
	err := c.Query(context.Background(), "2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z", "", 1, &out)
	var cursor *CursorError
	if !errors.As(err, &cursor) || out.Len() == 0 {
		t.Fatalf("expected invalid cursor after first page: %v", err)
	}
}
