package client

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExportPageAndByteBounds(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "progress")
	store := newTestExportStore(t, path)
	c := newTestExportClient(t, "http://bog.test")
	body := contractRow(strings.Repeat("x", queryReadBlock+9))
	c.MaxPageBytes = int64(len(body))
	calls := 0
	c.http.Transport = queryRoundTripper(func(req *http.Request) (*http.Response, error) {
		calls++
		switch calls {
		case 1:
			resp := exportTestResponse(body+" ", fixtureCursor)
			resp.ContentLength = -1
			resp.TransferEncoding = []string{"chunked"}
			return resp, nil
		case 2:
			return exportTestResponse(body, fixtureCursor), nil
		case 3:
			if req.URL.Query().Get("cursor") != fixtureCursor {
				t.Fatalf("request after resuming: %s", req.URL)
			}
			return exportTestResponse("", ""), nil
		default:
			t.Fatalf("unexpected request: %s", req.URL)
			return nil, nil
		}
	})
	stopped := errors.New("sink stopped")
	deliveries := 0
	blocked := newTestExport(t, c, Query{}, 1, store, func(_ context.Context, p ExportPage) error {
		deliveries++
		if p.Number != 1 || len(p.Page.Rows) != 1 || contractRow(p.Page.Rows[0].Line) != body {
			t.Fatalf("opening delivery: %+v", p)
		}
		return stopped
	})
	if err := blocked.Run(t.Context()); !errors.Is(err, ErrPageBytes) {
		t.Fatalf("streamed overflow: %v", err)
	}
	if _, found, err := store.Load(t.Context(), c.MaxPageBytes); err != nil || found || deliveries != 0 {
		t.Fatalf("overflow persisted or delivered: %t, %v, %d", found, err, deliveries)
	}
	if err := blocked.Run(t.Context()); !errors.Is(err, stopped) {
		t.Fatalf("opening exactly at the byte ceiling: %v", err)
	}
	checkpoint := loadTestExport(t, store)
	if string(checkpoint.Opening.Body) != body {
		t.Fatal("journal changed the opening bytes")
	}

	c.MaxPageBytes--
	if err := blocked.Run(t.Context()); !errors.Is(err, ErrPageBytes) || calls != 2 || deliveries != 1 {
		t.Fatalf("over-limit replay: calls=%d deliveries=%d error=%v", calls, deliveries, err)
	}
	c.MaxPageBytes++
	sink := func(_ context.Context, p ExportPage) error {
		deliveries++
		if p.Number == 2 && (p.RequestCursor != fixtureCursor || len(p.Page.Rows) != 0 || p.Page.NextCursor != "") {
			t.Fatalf("empty terminal delivery: %+v", p)
		}
		return nil
	}
	for range 2 {
		if err := newTestExport(t, c, Query{}, 1, store, sink).Run(t.Context()); !errors.Is(err, ErrPageBudget) {
			t.Fatalf("exhausted export: %v", err)
		}
		if calls != 2 || deliveries != 2 {
			t.Fatalf("over-budget fetch or replay: calls=%d deliveries=%d", calls, deliveries)
		}
	}
	if err := newTestExport(t, c, Query{}, 2, store, sink).Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	if calls != 3 || deliveries != 3 || !loadTestExport(t, store).Complete {
		t.Fatalf("increased budget failed: calls=%d deliveries=%d", calls, deliveries)
	}
}

func TestExportTerminalOpeningReplay(t *testing.T) {
	for _, body := range []string{"", contractRow("one page")} {
		t.Run(strings.TrimSpace(body), func(t *testing.T) {
			t.Parallel()
			c := newTestExportClient(t, "http://bog.test")
			calls := 0
			c.http.Transport = queryRoundTripper(func(*http.Request) (*http.Response, error) {
				calls++
				return exportTestResponse(body, ""), nil
			})
			store := newTestExportStore(t, filepath.Join(t.TempDir(), "progress"))
			deliveries := 0
			interrupted := errors.New("sink interrupted")
			session := newTestExport(t, c, Query{}, 1, store, func(_ context.Context, p ExportPage) error {
				deliveries++
				if p.Number != 1 || p.RequestCursor != "" || p.Page.NextCursor != "" ||
					(body == "" && len(p.Page.Rows) != 0) ||
					(body != "" && (len(p.Page.Rows) != 1 || contractRow(p.Page.Rows[0].Line) != body)) {
					t.Fatalf("terminal opening: %+v", p)
				}
				if deliveries == 1 {
					return interrupted
				}
				return nil
			})
			if err := session.Run(t.Context()); !errors.Is(err, interrupted) {
				t.Fatalf("interrupted terminal opening: %v", err)
			}
			if p := loadTestExport(t, store); p.Complete || p.Opening == nil || p.Opening.NextCursor != "" {
				t.Fatalf("terminal opening prematurely completed: %+v", p)
			}
			for range 2 {
				if err := session.Run(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if p := loadTestExport(t, store); !p.Complete || p.Opening != nil || calls != 1 || deliveries != 2 {
				t.Fatalf("completion: %+v, calls=%d deliveries=%d", p, calls, deliveries)
			}
		})
	}
}

func TestExportBindingAndExpiredContinuation(t *testing.T) {
	t.Parallel()
	c := newTestExportClient(t, "http://bog.test/first/")
	store := newTestExportStore(t, filepath.Join(t.TempDir(), "progress"))
	q := Query{Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), End: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC), Contains: "a+b &=é", Limit: 1}
	requests := 0
	transport := queryRoundTripper(func(req *http.Request) (*http.Response, error) {
		requests++
		if got := req.URL.Query(); got.Get("start") != q.Start.Format(time.RFC3339Nano) ||
			got.Get("end") != q.End.Format(time.RFC3339Nano) || got.Get("contains") != q.Contains || got.Get("limit") != "1" {
			t.Fatalf("wrong query %s", req.URL)
		}
		if requests == 1 {
			return exportTestResponse(contractRow("first"), fixtureCursor), nil
		}
		if req.URL.Query().Get("cursor") != fixtureCursor || req.URL.Query().Has("page") {
			t.Fatalf("expired cursor was replaced: %s", req.URL)
		}
		resp := exportTestResponse("query capture expired\n", "")
		resp.StatusCode = http.StatusGone
		return resp, nil
	})
	c.http.Transport = transport
	if err := newTestExport(t, c, q, 1, store, func(context.Context, ExportPage) error { return nil }).Run(t.Context()); !errors.Is(err, ErrPageBudget) {
		t.Fatalf("first page: %v", err)
	}
	prior := loadTestExport(t, store)
	unexpected := func(context.Context, ExportPage) error {
		t.Fatal("unexpected sink delivery")
		return nil
	}
	for _, tc := range []struct {
		name   string
		target string
		change func(*Query)
	}{
		{"start", "http://bog.test/first/", func(q *Query) { q.Start = q.Start.Add(time.Nanosecond) }},
		{"end", "http://bog.test/first/", func(q *Query) { q.End = q.End.Add(time.Nanosecond) }},
		{"filter", "http://bog.test/first/", func(q *Query) { q.Contains += " " }},
		{"row limit", "http://bog.test/first/", func(q *Query) { q.Limit = 2 }},
		{"host", "http://other.test/first/", func(*Query) {}},
		{"base path", "http://bog.test/second/", func(*Query) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := q
			tc.change(&changed)
			target := newTestExportClient(t, tc.target)
			target.http.Transport = queryRoundTripper(func(r *http.Request) (*http.Response, error) {
				t.Fatalf("mismatch triggered HTTP: %s", r.URL)
				return nil, nil
			})
			if err := newTestExport(t, target, changed, 2, store, unexpected).Run(t.Context()); !errors.Is(err, ErrExportMismatch) {
				t.Fatalf("mismatched resume: %v", err)
			}
		})
	}
	equivalent := newTestExportClient(t, "http://bog.test/first")
	equivalent.http.Transport = transport
	zone := time.FixedZone("UTC+1", 3600)
	same := q
	same.Start, same.End = q.Start.In(zone), q.End.In(zone)
	if err := newTestExport(t, equivalent, same, 2, store, unexpected).Run(t.Context()); !errors.Is(err, ErrCaptureExpired) {
		t.Fatalf("expired capture: %v", err)
	}
	if after := loadTestExport(t, store); after != prior || requests != 2 {
		t.Fatalf("failed resume changed progress: %+v, requests=%d", after, requests)
	}
}

func TestFileExportProgressRejectsInvalidRecords(t *testing.T) {
	good := ExportProgress{Version: ExportProgressVersion, Opening: &ExportOpeningPage{Body: []byte(contractRow("sensitive"))}}
	for _, tc := range []struct {
		name  string
		alter func(ExportProgress) ExportProgress
	}{
		{"version", func(p ExportProgress) ExportProgress { p.Version++; return p }},
		{"lost opening", func(p ExportProgress) ExportProgress { p.Opening = nil; return p }},
		{"opening with cursor", func(p ExportProgress) ExportProgress { p.Cursor = fixtureCursor; return p }},
		{"no terminal acknowledgment", func(p ExportProgress) ExportProgress { p.Opening = nil; p.Complete = true; return p }},
		{"completed with cursor", func(p ExportProgress) ExportProgress { p = p.advance(""); p.Cursor = fixtureCursor; return p }},
		{"invalid continuation", func(p ExportProgress) ExportProgress { return p.advance("opaque-but-unframed") }},
		{"inconsistent cycle anchor", func(p ExportProgress) ExportProgress {
			p = p.advance(fixtureCursor)
			p.CycleAnchor = fixtureNext
			return p
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newTestExportStore(t, filepath.Join(t.TempDir(), "progress"))
			if err := store.Save(t.Context(), good); err != nil {
				t.Fatal(err)
			}
			if err := store.Save(t.Context(), tc.alter(good)); !errors.Is(err, ErrExportProgress) {
				t.Fatalf("invalid save: %v", err)
			}
			original := loadTestExport(t, store)
			if original.Opening == nil || !bytes.Equal(original.Opening.Body, good.Opening.Body) {
				t.Fatalf("invalid save changed the original checkpoint: %+v", original)
			}
		})
	}
}

func TestFileExportProgressCorruptionAndCleanup(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "progress")
	store := newTestExportStore(t, path)
	good := ExportProgress{Version: ExportProgressVersion, Opening: &ExportOpeningPage{Body: []byte(contractRow("private"))}}
	stage := filepath.Join(dir, store.stage)
	if err := os.WriteFile(stage, good.Opening.Body, 0600); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Load(t.Context(), DefaultMaxPage); err != nil || found {
		t.Fatalf("abandoned staging file became progress: found=%t error=%v", found, err)
	}
	if _, err := os.Stat(stage); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("abandoned staging file remains: %v", err)
	}
	if err := store.Save(t.Context(), good); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0077 != 0 {
		t.Fatalf("checkpoint is accessible to others: %v", info.Mode())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		corrupt func([]byte) []byte
	}{
		{"truncated", func(data []byte) []byte { return data[:len(data)-1] }},
		{"changed body", func(data []byte) []byte { data[bytes.Index(data, []byte("private"))] = 'P'; return data }},
		{"unknown file format", func(data []byte) []byte { data[0] = 'X'; return data }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, tc.corrupt(bytes.Clone(raw)), 0600); err != nil {
				t.Fatal(err)
			}
			if _, found, err := store.Load(t.Context(), DefaultMaxPage); found || !errors.Is(err, ErrExportProgress) {
				t.Fatalf("corrupt checkpoint accepted: found=%t error=%v", found, err)
			}
		})
	}

	occupied := filepath.Join(dir, "occupied")
	if err := os.Mkdir(occupied, 0700); err != nil {
		t.Fatal(err)
	}
	if err := newTestExportStore(t, occupied).Save(t.Context(), good); err == nil {
		t.Fatal("checkpoint replaced a directory")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("failed rename leaked a staging file: %v", entries)
	}
}
