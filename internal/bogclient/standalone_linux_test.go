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

func TestStandaloneRoundTrip(t *testing.T) {
	s := &store.Store{}
	w, err := wal.Open(t.TempDir(), s)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	server := httptest.NewServer(httpapi.New(s, w))
	defer server.Close()
	c, err := New(server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := "{\"ts\":\"2025-01-01T00:00:00.000000001Z\",\"line\":\"same\"}\n"
	if err := c.IngestStream(context.Background(), strings.NewReader(rows+rows)); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err = c.Query(context.Background(), Query{Start: "2025-01-01T00:00:00Z", End: "2025-01-02T00:00:00Z", Limit: 1}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != rows+rows {
		t.Fatalf("unexpected rows: %s", out.String())
	}
}
