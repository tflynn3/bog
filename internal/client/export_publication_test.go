package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"
)

// The sink and file store use the same sequence as the CLI. Fail the actual
// replacement after the second page is synced, once on each side of rename.
func TestExportSessionCheckpointPublication(t *testing.T) {
	const first = `{"ts":"2025-01-01T00:00:01Z","line":"a"}` + "\n"
	const second = `{"ts":"2025-01-01T00:00:01Z","line":"b"}` + "\n"
	const third = `{"ts":"2025-01-01T00:00:02Z","line":"c"}` + "\n"
	const want = first + second + first + third
	next := fixtureNext
	interrupted := errors.New("interrupted checkpoint publication")
	for _, installed := range []bool{false, true} {
		t.Run(fmt.Sprintf("replacement installed = %t", installed), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := filepath.Join(dir, "progress")
			output, err := os.OpenFile(filepath.Join(dir, "rows"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := output.Close(); err != nil {
					t.Error(err)
				}
			})
			read := func(name string) []byte {
				t.Helper()
				data, err := os.ReadFile(name)
				if err != nil {
					t.Fatal(err)
				}
				return data
			}
			var mu sync.Mutex
			var requests []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				token := r.URL.Query().Get("cursor")
				mu.Lock()
				requests = append(requests, token)
				mu.Unlock()
				var body, successor string
				switch token {
				case "":
					body, successor = first, fixtureCursor
				case fixtureCursor:
					body, successor = second+first, next
				case next:
					body = third
				default:
					t.Errorf("unexpected cursor %q", token)
					http.Error(w, "invalid cursor", http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/x-ndjson")
				if successor != "" {
					w.Header().Set(nextCursorHeader, successor)
				}
				if _, err := io.WriteString(w, body); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			c := newTestExportClient(t, server.URL)
			q := Query{Limit: 2}
			st := newTestExportStore(t, path)
			sink := func(_ context.Context, p ExportPage) error {
				encoder := json.NewEncoder(output)
				for _, row := range p.Page.Rows {
					if err := encoder.Encode(row); err != nil {
						return err
					}
				}
				return output.Sync()
			}
			if err := newTestExport(t, c, q, 1, st, sink).Run(ctx); !errors.Is(err, ErrPageBudget) {
				t.Fatalf("commit first page: %v", err)
			}
			before := read(path)
			if got := string(read(output.Name())); got != first {
				t.Fatalf("committed first page = %q", got)
			}

			runWithFault := func() (err error) {
				lease, err := st.Acquire()
				if err != nil {
					return err
				}
				defer func() { err = errors.Join(err, lease.Close()) }()
				fault := exportSaveHook{ExportProgressStore: lease, save: func(ctx context.Context, p ExportProgress) error {
					rename, syncDir := (*os.Root).Rename, syncExportDirectory
					if installed {
						syncDir = func(*os.Root) error { return interrupted }
					} else {
						rename = func(*os.Root, string, string) error { return interrupted }
					}
					return lease.save(ctx, p, rename, syncDir)
				}}
				return newTestExport(t, c, q, 3, fault, sink).Run(ctx)
			}
			if err := runWithFault(); !errors.Is(err, interrupted) {
				t.Fatalf("publication interruption: %v", err)
			}
			if got := string(read(output.Name())); got != first+second+first {
				t.Fatalf("output after publication interruption = %q", got)
			}
			if unchanged := bytes.Equal(read(path), before); unchanged == installed {
				t.Fatalf("checkpoint unchanged = %t after installed=%t", unchanged, installed)
			}
			saved := loadTestExport(t, st)
			wantCursor, wantAck := fixtureCursor, 1
			wantRequests := []string{"", fixtureCursor, fixtureCursor, next}
			if installed {
				wantCursor, wantAck = next, 2
				wantRequests = []string{"", fixtureCursor, next}
			} else {
				if err := output.Truncate(int64(len(first))); err != nil {
					t.Fatal(err)
				}
				if err := output.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			if saved.Complete || saved.Cursor != wantCursor || saved.AcknowledgedPages != wantAck {
				t.Fatalf("recovered checkpoint = %+v, want page %d and cursor %q", saved, wantAck, wantCursor)
			}
			if err := newTestExport(t, c, q, 3, st, sink).Run(ctx); err != nil {
				t.Fatal(err)
			}
			if got := string(read(output.Name())); got != want {
				t.Errorf("resumed bytes = %q, want %q", got, want)
			}
			if p := loadTestExport(t, st); !p.Complete || p.AcknowledgedPages != 3 {
				t.Errorf("terminal checkpoint = %+v", p)
			}
			mu.Lock()
			defer mu.Unlock()
			if !slices.Equal(requests, wantRequests) {
				t.Errorf("requests = %q, want %q", requests, wantRequests)
			}
		})
	}
}
