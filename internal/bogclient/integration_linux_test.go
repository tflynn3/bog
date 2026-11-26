//go:build linux

package bogclient

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/internal/wal"
)

func TestStandaloneServer(t *testing.T) {
	dir := t.TempDir()
	s := &store.Store{}
	w, err := wal.Open(dir, s)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	server := httptest.NewServer(httpapi.New(s, w))
	defer server.Close()
	c := New(server.URL)
	ctx := context.Background()
	if err := c.Health(ctx); err != nil {
		t.Fatal(err)
	}
	input := "{\"ts\":\"2025-01-01T00:00:00.123456789Z\",\"line\":\"a\"}\n"
	if err := c.IngestStream(ctx, strings.NewReader(input+input)); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := c.Query(ctx, Query{Start: "2025-01-01T00:00:00Z", End: "2025-01-02T00:00:00Z", Limit: 1}, &output); err != nil {
		t.Fatal(err)
	}
	if output.String() != input+input {
		t.Fatalf("got %q", output.String())
	}
}
