package bogclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWriteOutcomes(t *testing.T) {
	for _, tc := range []struct {
		status   int
		rejected bool
	}{{400, true}, {413, true}, {503, true}, {500, false}, {307, false}} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status) }))
			defer s.Close()
			c, _ := New(s.URL, nil)
			err := c.Ingest(context.Background(), []byte("{}\n"))
			var rejected *RejectedError
			var unknown *UnknownOutcomeError
			if tc.rejected && !errors.As(err, &rejected) || !tc.rejected && !errors.As(err, &unknown) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
func TestQueryPagesPreserveDuplicates(t *testing.T) {
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
	var rows []string
	err := c.Query(context.Background(), Query{Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), End: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)}, func(r json.RawMessage) error { rows = append(rows, string(r)); return nil })
	if err != nil || len(rows) != 2 || rows[0] != rows[1] {
		t.Fatalf("rows %v: %v", rows, err)
	}
}
func TestOversizeLine(t *testing.T) {
	c, _ := New("http://127.0.0.1:1", nil)
	err := c.IngestStream(context.Background(), strings.NewReader(strings.Repeat("x", MaxBatch+1)))
	var rejected *RejectedError
	if !errors.As(err, &rejected) {
		t.Fatalf("got %v", err)
	}
}
