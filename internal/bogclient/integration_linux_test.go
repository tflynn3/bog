//go:build linux

package bogclient_test

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"bog/internal/bogclient"
	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/internal/wal"
)

func TestStandaloneServer(t *testing.T) {
	st := &store.Store{}
	log, err := wal.Open(t.TempDir(), st)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	server := httptest.NewServer(httpapi.New(st, log))
	defer server.Close()
	c := bogclient.New(server.URL)
	ctx := context.Background()
	if err := c.Health(ctx); err != nil {
		t.Fatal(err)
	}
	row := `{"ts":"2025-01-01T00:00:00.123456789Z","line":"duplicate"}` + "\n"
	if err := c.Ingest(ctx, strings.NewReader(row+row)); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := c.Query(ctx, "2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z", "", 1, &output); err != nil {
		t.Fatal(err)
	}
	if output.String() != row+row {
		t.Fatalf("unexpected query output: %q", output.String())
	}
}
