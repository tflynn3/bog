package client

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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
	stage := filepath.Join(dir, exportStageName(store.name))
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
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), exportStagePrefix) {
			t.Fatalf("failed rename leaked a staging file: %s", entry.Name())
		}
	}
}

func TestFileExportCaseOwnership(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	alias := exportCaseAlias(t, dir)
	original := newTestExportStore(t, filepath.Join(dir, "Progress"))
	reopened := newTestExportStore(t, filepath.Join(dir, "progress"))
	good := ExportProgress{Version: ExportProgressVersion, Opening: &ExportOpeningPage{Body: []byte(contractRow("installed"))}}
	// Install a checkpoint from before leases existed.
	writeLegacyExportFile(t, filepath.Join(dir, original.name), good)
	stage := filepath.Join(dir, exportStageName(original.name))
	writeLegacyExportFile(t, stage, ExportProgress{Version: ExportProgressVersion,
		Opening: &ExportOpeningPage{Body: []byte(contractRow("abandoned"))}})
	other := filepath.Join(dir, exportStageName("Other"))
	writeLegacyExportFile(t, other, good)

	progress, found, err := reopened.Load(t.Context(), DefaultMaxPage)
	if err != nil || found != alias {
		t.Fatalf("alias load: found=%t, case alias=%t, err=%v", found, alias, err)
	}
	if alias {
		if progress.Opening == nil || !bytes.Equal(progress.Opening.Body, good.Opening.Body) {
			t.Fatalf("load promoted abandoned stage: %+v", progress)
		}
		assertExportStageMissing(t, stage)
	} else {
		if err := os.Link(filepath.Join(dir, original.name), filepath.Join(dir, reopened.name)); err != nil {
			t.Fatal(err)
		}
		if _, found, err := reopened.Load(t.Context(), DefaultMaxPage); !found || err != nil {
			t.Fatalf("distinct hard-linked checkpoint did not load: found=%t, err=%v", found, err)
		}
		if _, err := os.Stat(stage); err != nil {
			t.Fatalf("distinct checkpoint's stage was removed: %v", err)
		}
	}
	if _, err := os.Stat(other); err != nil {
		t.Fatalf("unrelated stage was removed: %v", err)
	}

	lease, err := original.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lease.Close(); err != nil {
			t.Error(err)
		}
	})
	err = reopened.Save(t.Context(), good)
	if alias && !errors.Is(err, ErrExportCheckpointBusy) || !alias && err != nil {
		t.Fatalf("save while differently cased name is held: %v (same checkpoint=%t)", err, alias)
	}
}

func TestFileExportRejectsUnownedStage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		create func(string, string) error
	}{
		{"directory", func(stage, _ string) error { return os.Mkdir(stage, 0700) }},
		{"symlink", func(stage, target string) error { return os.Symlink(target, stage) }},
		{"hard link", func(stage, target string) error { return os.Link(target, stage) }},
		{"public file", func(stage, _ string) error {
			if err := os.WriteFile(stage, []byte("unrelated"), 0600); err != nil {
				return err
			}
			return os.Chmod(stage, 0644)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			store := newTestExportStore(t, filepath.Join(dir, "progress"))
			stage := filepath.Join(dir, exportStageName(store.name))
			target := filepath.Join(dir, "other")
			if err := os.WriteFile(target, []byte("keep"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := tc.create(stage, target); err != nil {
				t.Fatal(err)
			}
			if _, _, err := store.Load(t.Context(), DefaultMaxPage); err == nil {
				t.Fatal("load accepted an unsafe stage")
			}
			if _, err := os.Lstat(stage); err != nil {
				t.Fatalf("unsafe entry was removed: %v", err)
			}
			if data, err := os.ReadFile(target); err != nil || string(data) != "keep" {
				t.Fatalf("unrelated file changed: %q, %v", data, err)
			}
		})
	}
}

func TestFileExportSessionsExcludeCompetingRecoveryAndSave(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "Progress")
	competingPath := path
	if exportCaseAlias(t, dir) {
		competingPath = filepath.Join(dir, "progress")
	}
	owner := newTestExportStore(t, path)
	contender := newTestExportStore(t, competingPath)
	c := newTestExportClient(t, "http://bog.test")
	var requests atomic.Int32
	c.http.Transport = queryRoundTripper(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return exportTestResponse(contractRow("opening"), ""), nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	inSink := make(chan struct{})
	releaseSink := make(chan struct{})
	first := newTestExport(t, c, Query{}, 1, owner, func(ctx context.Context, _ ExportPage) error {
		close(inSink)
		select {
		case <-releaseSink:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	finished := make(chan error, 1)
	go func() {
		defer close(finished)
		finished <- first.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		for err := range finished {
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		}
	})
	select {
	case <-inSink:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	second := newTestExport(t, c, Query{}, 1, contender, func(context.Context, ExportPage) error {
		t.Error("competing session reached its sink")
		return nil
	})
	good := ExportProgress{Version: ExportProgressVersion, AcknowledgedPages: 1, Complete: true}
	for name, err := range map[string]error{
		"session": second.Run(ctx),
		"save":    contender.Save(ctx, good),
	} {
		if !errors.Is(err, ErrExportCheckpointBusy) {
			t.Errorf("competing %s: %v", name, err)
		}
	}
	if _, _, err := contender.Load(ctx, DefaultMaxPage); !errors.Is(err, ErrExportCheckpointBusy) {
		t.Errorf("competing recovery: %v", err)
	}
	close(releaseSink)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if err := second.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 1 {
		t.Errorf("made %d requests, want one", requests.Load())
	}
}

func exportCaseAlias(t *testing.T, dir string) bool {
	t.Helper()
	probe := filepath.Join(dir, "case-probe")
	if err := os.WriteFile(probe, nil, 0600); err != nil {
		t.Fatal(err)
	}
	_, err := os.Lstat(filepath.Join(dir, "Case-Probe"))
	if removeErr := os.Remove(probe); removeErr != nil {
		t.Fatal(removeErr)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return err == nil
}

func writeLegacyExportFile(t *testing.T, path string, progress ExportProgress) {
	t.Helper()
	body, metadata, err := encodeExportCheckpoint(progress)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(writeExportCheckpoint(t.Context(), file, body, metadata), file.Close()); err != nil {
		t.Fatal(err)
	}
}

func assertExportStageMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stage %q still exists or cannot be checked: %v", path, err)
	}
}
