package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/internal/wal"
)

func runtimeTestServer(t *testing.T, rt *retainedRuntime) (http.Handler, func(context.Context) error) {
	t.Helper()
	probe, err := rt.wal.OpenProbeDirectory()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	handler, api := newReadyHandler(ctx, config{
		maxResponseBytes: 32 << 20, queryConcurrency: 8, queryWorkBudget: 128 << 20,
	}, rt, probe, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	return handler, func(ctx context.Context) error {
		cancel()
		return errors.Join(api.Close(ctx), probe.Close(), rt.close(ctx))
	}
}

func TestOrdinaryRetainedQueryUsesOneCaptureAcrossShortScans(t *testing.T) {
	rt, err := openRuntime(t.Context(), privateDataDir(t))
	if err != nil {
		t.Fatal(err)
	}
	api := httpapi.NewWithPageSourceAndQueryResources(rt, rt, 4<<20, 2, 32<<20,
		httpapi.QueryResourceLimits{ResponseBytes: 4 << 20, ScanBytes: 32 << 20, Captures: 1})
	t.Cleanup(func() {
		if err := errors.Join(api.Close(context.Background()), rt.close(context.Background())); err != nil {
			t.Error(err)
		}
	})
	const ts = "2025-01-01T00:00:00Z"
	want := []string{
		"needle-first-" + strings.Repeat("x", 700<<10),
		"needle-second-" + strings.Repeat("y", 700<<10),
		"needle-third",
	}
	for _, line := range want {
		payload := []byte(fmt.Sprintf("{\"ts\":%q,\"line\":%q}\n", ts, line))
		runtimeRequest(t, api, http.MethodPost, "/ingest", payload, 204)
	}
	if err := rt.advance(t.Context(), 2); err != nil {
		t.Fatal(err)
	}
	const query = "/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&contains=needle"
	opening := runtimeRequest(t, api, http.MethodGet, query+"&page=1&limit=1", nil, 200)
	token := opening.Header().Get("Bog-Next-Cursor")
	if token == "" {
		t.Fatal("missing continuation")
	}
	refused := runtimeRequest(t, api, http.MethodGet, query, nil, 503)
	if got := strings.TrimSpace(refused.Body.String()); got != httpapi.ErrQueryCaptureCapacityExhausted.Error() {
		t.Fatalf("capture limit response = %q", got)
	}
	if err := api.RetireQueryCapture(token); err != nil {
		t.Fatal(err)
	}
	out := runtimeRequest(t, api, http.MethodGet, query+"&limit=3", nil, 200)
	if got := runtimeLines(t, out); !reflect.DeepEqual(got, want) {
		t.Fatalf("ordinary query across short scans returned %d rows, want %d exact rows", len(got), len(want))
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.views != 0 || len(rt.captures) != 0 {
		t.Fatalf("ordinary query leaked a view, views=%d captures=%d", rt.views, len(rt.captures))
	}
}

func runtimeRequest(t *testing.T, h http.Handler, method, path string, body []byte, status int) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(method, path, bytes.NewReader(body)))
	if w.Code != status {
		t.Fatalf("%s %s = %d %s, want %d", method, path, w.Code, w.Body, status)
	}
	return w
}

func runtimeLines(t *testing.T, w *httptest.ResponseRecorder) []string {
	t.Helper()
	var lines []string
	d := json.NewDecoder(w.Body)
	for {
		var row store.Row
		if err := d.Decode(&row); err != nil {
			if errors.Is(err, io.EOF) {
				return lines
			}
			t.Fatal(err)
		}
		lines = append(lines, row.Line)
	}
}

func TestRetainedPagingAcrossReplacement(t *testing.T) {
	t.Setenv("BOG_CAPACITY_CONTROL", "1")
	path := privateDataDir(t)
	rt, err := openRuntime(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	h, closeServer := runtimeTestServer(t, rt)
	t.Cleanup(func() {
		if rt != nil {
			if err := closeServer(context.Background()); err != nil {
				t.Error(err)
			}
		}
	})
	const query = "/query?start=2025-01-01T00:00:00Z&end=2025-01-03T00:00:00Z&contains=needle"
	const seed = `{"ts":"2025-01-02T00:00:00Z","line":"needle-a"}
{"ts":"2025-01-01T00:00:00Z","line":"NEEDLE"}
{"ts":"2025-01-02T00:00:00Z","line":"needle-a"}
`
	const second = `{"ts":"2025-01-02T00:00:00Z","line":"needle-b"}
`
	const backfill = `{"ts":"2025-01-01T00:00:00Z","line":"needle-backfill"}
`
	runtimeRequest(t, h, http.MethodPost, "/ingest", []byte(seed), 204)
	runtimeRequest(t, h, http.MethodPost, "/ingest", []byte(second), 204)
	publish := func(cutoff uint64) {
		const endpoint = "/_capacity/compaction"
		runtimeRequest(t, h, http.MethodPost, fmt.Sprintf("%s?action=start&cutoff=%d", endpoint, cutoff), nil, 200)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			w := runtimeRequest(t, h, http.MethodGet, fmt.Sprintf("%s?action=status&cutoff=%d", endpoint, cutoff), nil, 200)
			var state compactionObservation
			if err := json.Unmarshal(w.Body.Bytes(), &state); err != nil {
				t.Fatal(err)
			}
			if state.State == "complete" && state.Covered == cutoff {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatalf("compaction %d timed out: %+v", cutoff, state)
			case <-ticker.C:
			}
		}
	}
	publish(3)
	oldManifest := rt.manifest
	first := runtimeRequest(t, h, http.MethodGet, query+"&page=1&limit=1", nil, 200)
	token := first.Header().Get("Bog-Next-Cursor")
	if token == "" || !reflect.DeepEqual(runtimeLines(t, first), []string{"needle-a"}) {
		t.Fatalf("opening did not retain the ordered snapshot, cursor %q", token)
	}
	runtimeRequest(t, h, http.MethodPost, "/ingest", []byte(backfill), 204)
	publish(4)
	if _, err := os.Stat(filepath.Join(path, oldManifest)); err != nil {
		t.Fatalf("pinned predecessor manifest: %v", err)
	}
	p, tail := rt.wal.PublishedAuthority()
	if p.First != 4 || p.Next != 5 || tail == 0 {
		t.Fatalf("selected cutoff lost the carried suffix: %+v", p)
	}
	w := runtimeRequest(t, h, http.MethodGet, query+"&limit=10&cursor="+url.QueryEscape(token), nil, 200)
	if got := runtimeLines(t, w); !reflect.DeepEqual(got, []string{"needle-a", "needle-b"}) {
		t.Fatalf("held capture = %q", got)
	}
	runtimeRequest(t, h, http.MethodPost, "/_capacity/compaction?action=retire&cursor="+url.QueryEscape(token), nil, 204)
	runtimeRequest(t, h, http.MethodGet, query+"&limit=10&cursor="+url.QueryEscape(token), nil, 410)
	want := []string{"needle-backfill", "needle-a", "needle-a", "needle-b"}
	checkFresh := func() {
		w := runtimeRequest(t, h, http.MethodGet, query, nil, 200)
		if got := runtimeLines(t, w); !reflect.DeepEqual(got, want) {
			t.Fatalf("fresh capture = %q, want %q", got, want)
		}
	}
	checkFresh()
	if err := closeServer(t.Context()); err != nil {
		t.Fatal(err)
	}
	rt = nil
	if _, err := os.Stat(filepath.Join(path, oldManifest)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retired predecessor manifest still present: %v", err)
	}
	rt, err = openRuntime(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	h, closeServer = runtimeTestServer(t, rt)
	checkFresh()
}

func TestRuntimeDrainKeepsLockUntilViewReleases(t *testing.T) {
	path := privateDataDir(t)
	rt, err := openRuntime(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	view, err := rt.acquireView(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	if err := rt.close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown with live view: %v", err)
	}
	if w, err := wal.Open(path, &store.Store{}); !errors.Is(err, syscall.EWOULDBLOCK) {
		if err == nil {
			err = w.Close()
		}
		t.Fatalf("another WAL acquired the directory during drain: %v", err)
	}
	if err := view.close(); err != nil {
		t.Fatal(err)
	}
	if err := rt.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	w, err := wal.Open(path, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLargeOrdinaryWALMigratesBeforeServing(t *testing.T) {
	path := privateDataDir(t)
	w, err := wal.Open(path, &store.Store{})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"ts":"2025-01-02T00:00:00Z","line":"` + strings.Repeat("x", 1<<20) + `"}` + "\n")
	rows, err := store.Decode(body)
	if err != nil {
		t.Fatal(err)
	}
	for range 13 {
		if err := w.Append(body, rows); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	rt, err := openRuntime(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rt.close(t.Context()); err != nil {
			t.Error(err)
		}
	}()
	point, active := rt.wal.PublishedAuthority()
	if point.Sequence == 0 || point.First != 13 || point.Next != 13 || active != 0 {
		t.Fatalf("large ordinary WAL was not migrated: %+v, active=%d", point, active)
	}
	view, err := rt.acquireView(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := view.close(); err != nil {
			t.Error(err)
		}
	}()
	if got := view.reader.Generation().Boundary.NextOrdinal; got != 13 {
		t.Fatalf("retained generation contains %d rows, want 13", got)
	}
}

func TestInterruptedFirstAdvanceRecoversOrdinaryWAL(t *testing.T) {
	path := privateDataDir(t)
	rt, err := openRuntime(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	h, closeServer := runtimeTestServer(t, rt)
	const body = "{\"ts\":\"2025-01-01T00:00:00Z\",\"line\":\"before selection\"}\n"
	runtimeRequest(t, h, http.MethodPost, "/ingest", []byte(body), 204)
	view, err := rt.acquireView(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	point := recoveryPoint(view.point)
	point.Generation = view.reader.Generation().ID
	point.SuffixSHA256, err = digestRange(view.wal, point.CoveredOffset, point.CommittedEnd)
	if err != nil {
		t.Fatal(err)
	}
	interrupted := errors.New("interrupted before selecting authority")
	_, err = rt.publisher.PublishRecoveryView(t.Context(), recoveryView{rt.dir, view.manifest, view.wal, point},
		point.CommittedEnd, point.CommittedOrdinal, func(context.Context, checkpoint.RecoveryPoint) error { return interrupted })
	if !errors.Is(err, checkpoint.ErrPublicationUncertain) || !errors.Is(err, interrupted) {
		t.Fatalf("interrupt publication: %v", err)
	}
	if err := errors.Join(view.close(), closeServer(t.Context())); err != nil {
		t.Fatal(err)
	}
	rt, err = openRuntime(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	h, closeServer = runtimeTestServer(t, rt)
	t.Cleanup(func() {
		if err := closeServer(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := checkpoint.CheckPublication(rt.dir); err != nil {
		t.Fatal(err)
	}
	w := runtimeRequest(t, h, http.MethodGet, "/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z", nil, 200)
	if got := runtimeLines(t, w); !reflect.DeepEqual(got, []string{"before selection"}) {
		t.Fatalf("recovered lines = %q", got)
	}
}
