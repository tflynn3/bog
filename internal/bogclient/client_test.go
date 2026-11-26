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

	"bog/internal/httpapi"
	"bog/internal/store"
)

type memoryWAL struct{ s *store.Store }

func (w memoryWAL) Append(_ []byte, rows []store.Row) error { w.s.Publish(rows); return nil }
func TestStandalone(t *testing.T) {
	s := new(store.Store)
	srv := httptest.NewServer(httpapi.New(s, memoryWAL{s}))
	defer srv.Close()
	c, e := New(srv.URL, nil)
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Health(context.Background()); e != nil {
		t.Fatal(e)
	}
	input := strings.Repeat("{\"ts\":\"2025-01-01T00:00:00.123456789Z\",\"line\":\"same\"}\n", 1200)
	if e = c.IngestStream(context.Background(), strings.NewReader(input)); e != nil {
		t.Fatal(e)
	}
	var out bytes.Buffer
	e = c.Query(context.Background(), Query{Start: "2025-01-01T00:00:00Z", End: "2025-01-02T00:00:00Z", Limit: 17}, &out)
	if e != nil {
		t.Fatal(e)
	}
	if n := bytes.Count(out.Bytes(), []byte("\n")); n != 1200 {
		t.Fatalf("got %d rows", n)
	}
}
func TestOutcomes(t *testing.T) {
	for _, tc := range []struct {
		status   int
		body     string
		rejected bool
	}{{503, "busy", true}, {503, "WAL unavailable", true}, {400, "invalid batch", true}, {502, "proxy", false}} {
		t.Run(fmt.Sprint(tc.status, tc.body), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, tc.body, tc.status) }))
			defer srv.Close()
			c, _ := New(srv.URL, nil)
			err := c.Ingest(context.Background(), []byte("{}"))
			var rejected *RejectedError
			var unknown *UnknownOutcomeError
			if tc.rejected && !errors.As(err, &rejected) || !tc.rejected && !errors.As(err, &unknown) {
				t.Fatalf("%T %v", err, err)
			}
		})
	}
}
func TestLargeLines(t *testing.T) {
	var sizes []int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		sizes = append(sizes, len(b))
		w.WriteHeader(204)
	}))
	defer srv.Close()
	c, _ := New(srv.URL, nil)
	line := strings.Repeat("x", MaxBatch/2+100) + "\n"
	if err := c.IngestStream(context.Background(), strings.NewReader(line+line)); err != nil {
		t.Fatal(err)
	}
	if len(sizes) != 2 || sizes[0] != len(line) || sizes[1] != len(line) {
		t.Fatal(sizes)
	}
}
