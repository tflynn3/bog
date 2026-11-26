//go:build linux

package bogclient

import (
	"bytes"
	"context"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/internal/wal"
)

func TestStandaloneServer(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	st := &store.Store{}
	w, err := wal.Open(dir, st)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	server := httptest.NewServer(httpapi.New(st, w))
	defer server.Close()
	c, err := New(server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := c.Health(ctx); err != nil {
		t.Fatal(err)
	}
	data := "{\"ts\":\"2025-01-01T00:00:00.123456789Z\",\"line\":\"same\"}\n"
	if err := c.IngestStream(ctx, strings.NewReader(data+data)); err != nil {
		t.Fatal(err)
	}
	start, _ := time.Parse(time.RFC3339, "2025-01-01T00:00:00Z")
	end := start.Add(time.Second)
	var out bytes.Buffer
	if err := c.QueryStream(ctx, Query{Start: start, End: end, Limit: 1}, &out); err != nil {
		t.Fatal(err)
	}
	if strings.Count(out.String(), "same") != 2 || !strings.Contains(out.String(), "123456789") {
		t.Fatalf("unexpected output %s", out.String())
	}
}
