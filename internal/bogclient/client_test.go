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

func TestIngestBoundaries(t *testing.T) {
	var sizes []int
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		sizes = append(sizes, len(b))
		if len(b) > MaxBatch {
			t.Error("oversized")
		}
		w.WriteHeader(204)
	}))
	defer s.Close()
	c, _ := New(s.URL, nil)
	line := strings.Repeat("x", MaxBatch/2) + "\n"
	if err := c.Ingest(context.Background(), strings.NewReader(line+line+"tail")); err != nil {
		t.Fatal(err)
	}
	if len(sizes) != 2 || sizes[0] != len(line) || sizes[1] != len(line)+4 {
		t.Fatal(sizes)
	}
}
func TestWriteOutcomes(t *testing.T) {
	for _, tc := range []struct {
		status   int
		body     string
		rejected bool
	}{{400, "invalid batch", true}, {503, "busy", true}, {500, "failure", false}} {
		t.Run(fmt.Sprint(tc.status, tc.body), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, tc.body, tc.status) }))
			defer s.Close()
			c, _ := New(s.URL, nil)
			err := c.IngestBatch(context.Background(), []byte("x"))
			var r *RejectedError
			var u *UnknownOutcomeError
			if tc.rejected && !errors.As(err, &r) || !tc.rejected && !errors.As(err, &u) {
				t.Fatal(err)
			}
		})
	}
}
func TestQueryCursorAndStreaming(t *testing.T) {
	calls := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			if r.URL.Query().Get("page") != "1" {
				t.Error("missing page")
			}
			w.Header().Set("Bog-Next-Cursor", "token")
			fmt.Fprintln(w, `{"ts":"2025-01-01T00:00:00.000000001Z","line":"a"}`)
		} else {
			if r.URL.Query().Get("cursor") != "token" {
				t.Error("missing cursor")
			}
			http.Error(w, "invalid cursor", 400)
		}
	}))
	defer s.Close()
	c, _ := New(s.URL, nil)
	var out bytes.Buffer
	err := c.Query(context.Background(), Query{Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), End: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)}, &out)
	var invalid *CursorInvalidError
	if !errors.As(err, &invalid) || calls != 2 || !strings.Contains(out.String(), "000000001") {
		t.Fatalf("%v %d %s", err, calls, out.String())
	}
}
