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
	for _, tc := range []struct {
		status            int
		rejected, unknown bool
	}{{204, false, false}, {400, true, false}, {413, true, false}, {503, true, false}, {500, false, true}} {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			if string(b) != "{}\n" {
				t.Errorf("batch = %q", b)
			}
			w.WriteHeader(tc.status)
		}))
		c, _ := New(s.URL, nil)
		err := c.Ingest(context.Background(), []byte("{}\n"))
		var reject *RejectedError
		var unknown *UnknownOutcomeError
		if errors.As(err, &reject) != tc.rejected || errors.As(err, &unknown) != tc.unknown {
			t.Errorf("status %d: %v", tc.status, err)
		}
		s.Close()
	}
}
func TestQueryPagesAndCursorError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("cursor") == "bad" {
			http.Error(w, "invalid cursor", 400)
			return
		}
		w.Header().Set("Bog-Next-Cursor", "next")
		io.WriteString(w, "{\"ts\":\"2025-01-01T00:00:00.000000001Z\",\"line\":\"x\"}\n")
	}))
	defer s.Close()
	c, _ := New(s.URL, nil)
	var out bytes.Buffer
	next, err := c.Query(context.Background(), Query{Start: "a", End: "b", Page: true}, &out)
	if err != nil || next != "next" || !strings.Contains(out.String(), "000000001") {
		t.Fatalf("%q %q %v", next, out.String(), err)
	}
	_, err = c.Query(context.Background(), Query{Cursor: "bad"}, io.Discard)
	var cursor *CursorError
	if !errors.As(err, &cursor) {
		t.Fatalf("%v", err)
	}
}
func TestOversizedBatchRejectedLocally(t *testing.T) {
	c, _ := New("http://localhost:1", nil)
	err := c.Ingest(context.Background(), make([]byte, MaxBatch+1))
	var reject *RejectedError
	if !errors.As(err, &reject) {
		t.Fatal(err)
	}
}
