package bogclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestIngestOutcomes(t *testing.T) {
	status := 204
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
	defer s.Close()
	c, err := New(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Ingest(context.Background(), []byte("x")); err != nil {
		t.Fatal(err)
	}
	status = 413
	var rejected *RejectedError
	if err = c.Ingest(context.Background(), []byte("x")); !errors.As(err, &rejected) {
		t.Fatalf("expected rejection, got %v", err)
	}
	status = 503
	var unknown *UnknownOutcomeError
	if err = c.Ingest(context.Background(), []byte("x")); !errors.As(err, &unknown) {
		t.Fatalf("expected unknown, got %v", err)
	}
}
func TestQueryCursor(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("cursor") != "" {
			http.Error(w, "invalid cursor", 400)
			return
		}
		w.Header().Set("Bog-Next-Cursor", "token")
		io.WriteString(w, "row\n")
	}))
	defer s.Close()
	c, _ := New(s.URL)
	q := Query{Start: time.Now(), End: time.Now()}
	body, next, err := c.Query(context.Background(), q, "")
	if err != nil || next != "token" {
		t.Fatalf("%q %v", next, err)
	}
	body.Close()
	_, _, err = c.Query(context.Background(), q, next)
	var invalid *CursorInvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("expected cursor invalidation: %v", err)
	}
}
