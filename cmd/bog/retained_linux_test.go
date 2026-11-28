package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/internal/wal"
)

func openTestStorage(t *testing.T, dir string) *storageOwner {
	t.Helper()
	s, err := openStorage(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	// Drive individual publications at chosen cutoffs.
	s.stop()
	<-s.done
	return s
}

func appendTestLine(t *testing.T, s *storageOwner, second int, line string) {
	t.Helper()
	payload := []byte(fmt.Sprintf("{\"ts\":\"2025-01-01T00:00:%02dZ\",\"line\":%q}\n", second, line))
	rows, err := store.Decode(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append(payload, rows); err != nil {
		t.Fatal(err)
	}
}

func viewLines(t *testing.T, source *pageSource, id string) []string {
	t.Helper()
	req := httpapi.PageRequest{Capture: id, Limit: 10, Filter: httpapi.PageFilter{
		Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), End: time.Date(2025, 1, 1, 0, 0, 3, 0, time.UTC),
		Contains: "needle",
	}}
	result, err := source.Page(t.Context(), req, store.NewQueryWork(t.Context(), 4096))
	if err != nil || result.More {
		t.Fatalf("scan: %v, more=%t", err, result.More)
	}
	var lines []string
	for _, row := range result.Rows {
		lines = append(lines, row.Row.Line)
	}
	return lines
}

type blockedSuffix struct {
	recoveryView
	started chan struct{}
	proceed chan struct{}
}

func (v blockedSuffix) OpenSuffix() (io.ReadCloser, error) {
	close(v.started)
	<-v.proceed
	return v.recoveryView.OpenSuffix()
}

func TestRetainedActivationCarriesCommitsAndKeepsPins(t *testing.T) {
	dir := privateDataDir(t)
	s := openTestStorage(t, dir)
	source := &pageSource{owner: s}
	t.Cleanup(func() {
		if err := closeStartup(s); err != nil {
			t.Error(err)
		}
	})
	appendTestLine(t, s, 2, "needle-late")
	appendTestLine(t, s, 1, "needle-tie")
	appendTestLine(t, s, 1, "NEEDLE")
	appendTestLine(t, s, 3, "needle-excluded")
	id, err := source.CapturePage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := source.Release(id); err != nil {
			t.Error(err)
		}
	}()

	old, err := s.acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	point := old.point
	point.SuffixSHA256, err = digestRange(old.wal, point.CoveredOffset, point.CommittedEnd)
	if err != nil {
		t.Fatal(errors.Join(err, old.close()))
	}
	ticket, err := s.catalog.ReserveReplacement(t.Context())
	if err != nil {
		t.Fatal(errors.Join(err, old.close()))
	}
	defer ticket.Close()
	gate := blockedSuffix{recoveryView{s.log, old.wal, old.name, old.manifest, point}, make(chan struct{}), make(chan struct{})}
	finished := make(chan error, 1)
	go func() {
		_, err := s.publisher.PublishRecoveryView(t.Context(), gate, point.CommittedEnd, point.CommittedOrdinal,
			func(ctx context.Context, next checkpoint.RecoveryPoint) error {
				return s.selectSuccessor(ctx, ticket, old, next)
			})
		finished <- errors.Join(err, old.close())
	}()
	select {
	case <-gate.started:
	case err := <-finished:
		t.Fatalf("publication stopped before reading its pinned view: %v", err)
	case <-time.After(5 * time.Second):
		close(gate.proceed)
		t.Fatal("publication did not read the captured suffix")
	}
	func() {
		defer close(gate.proceed)
		appendTestLine(t, s, 0, "needle-backfill")
		appendTestLine(t, s, 1, "needle-tie")
	}()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("selection did not finish")
	}
	a, ok := s.log.Authority()
	if !ok || a.First != 4 || a.Next != 6 || a.Covered >= a.End {
		t.Fatalf("successor did not carry the later commits: %+v", a)
	}
	if got := viewLines(t, source, id); !slices.Equal(got, []string{"needle-tie", "needle-late"}) {
		t.Fatalf("held view: %q", got)
	}
	freshWant := []string{"needle-backfill", "needle-tie", "needle-tie", "needle-late"}
	if got := viewLines(t, source, ""); !slices.Equal(got, freshWant) {
		t.Fatalf("fresh view: %q", got)
	}
	if err := s.reclaim(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"bog.wal", canonicalManifest} {
		if _, err := s.root.Stat(name); err != nil {
			t.Fatalf("predecessor was reclaimed while pinned: %s: %v", name, err)
		}
	}
	if err := source.Release(id); err != nil {
		t.Fatal(err)
	}
	if err := s.reclaim(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"bog.wal", canonicalManifest} {
		if _, err := s.root.Stat(name); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("predecessor survived final release: %s: %v", name, err)
		}
	}
	if err := closeStartup(s); err != nil {
		t.Fatal(err)
	}
	s = openTestStorage(t, dir)
	source = &pageSource{owner: s}
	if got := viewLines(t, source, ""); !slices.Equal(got, freshWant) {
		t.Fatalf("restart: %q", got)
	}
}

func TestRetainedRecoveryResolvesInterruptedPublication(t *testing.T) {
	for _, selected := range []bool{false, true} {
		t.Run(fmt.Sprintf("selector_committed_%t", selected), func(t *testing.T) {
			dir := privateDataDir(t)
			s := openTestStorage(t, dir)
			appendTestLine(t, s, 1, "needle-selected")
			old, err := s.acquire(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			point := old.point
			point.SuffixSHA256, err = digestRange(old.wal, point.CoveredOffset, point.CommittedEnd)
			if err != nil {
				t.Fatal(errors.Join(err, old.close()))
			}
			ticket, err := s.catalog.ReserveReplacement(t.Context())
			if err != nil {
				t.Fatal(errors.Join(err, old.close()))
			}
			defer ticket.Close()
			interrupted := errors.New("interrupted publication")
			_, err = s.publisher.PublishRecoveryView(t.Context(), recoveryView{s.log, old.wal, old.name, old.manifest, point},
				point.CommittedEnd, point.CommittedOrdinal, func(ctx context.Context, next checkpoint.RecoveryPoint) error {
					if selected {
						if err := s.selectSuccessor(ctx, ticket, old, next); err != nil {
							return err
						}
					}
					return interrupted
				})
			if !errors.Is(err, interrupted) || !errors.Is(err, checkpoint.ErrPublicationUncertain) {
				t.Fatalf("publication: %v", err)
			}
			// Simulate process loss after the staging intent is durable.
			if err := errors.Join(old.close(), s.catalog.Close(), s.log.Close()); err != nil {
				t.Fatal(err)
			}
			s = openTestStorage(t, dir)
			defer func() {
				if err := closeStartup(s); err != nil {
					t.Error(err)
				}
			}()
			_, retained := s.log.Authority()
			if selected != retained {
				t.Fatalf("retained=%t, expected %t", retained, selected)
			}
			if got := viewLines(t, &pageSource{owner: s}, ""); !slices.Equal(got, []string{"needle-selected"}) {
				t.Fatalf("after recovery: %q", got)
			}
			if err := checkpoint.CheckPublication(s.dir); err != nil {
				t.Fatalf("publication intent did not drain: %v", err)
			}
		})
	}
}

func TestRetainedRecoveryRejectsMissingSelector(t *testing.T) {
	dir := privateDataDir(t)
	s := openTestStorage(t, dir)
	appendTestLine(t, s, 1, "needle-committed")
	if err := s.advance(t.Context()); err != nil {
		t.Fatal(err)
	}
	authority, ok := s.log.Authority()
	if !ok {
		t.Fatal("missing retained authority")
	}
	if err := closeStartup(s); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "bog.commit")); err != nil {
		t.Fatal(err)
	}
	if reopened, err := openStorage(t.Context(), dir); err == nil {
		t.Fatal(errors.Join(errors.New("recovered without the retained selector"), closeStartup(reopened)))
	}
	if _, err := os.Stat(filepath.Join(dir, authority.Tail)); err != nil {
		t.Fatalf("selected tail was lost: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "bog.wal")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy WAL was recreated: %v", err)
	}
}

type testDrain struct {
	calls *[]string
	fail  error
}

func (d testDrain) Shutdown(context.Context) error {
	*d.calls = append(*d.calls, "http")
	return nil
}
func (d testDrain) Close(context.Context) error {
	*d.calls = append(*d.calls, "capture")
	return d.fail
}
func (d testDrain) drain(context.Context) error {
	*d.calls = append(*d.calls, "storage")
	return nil
}

type testProbe struct{ calls *[]string }

func (p testProbe) Close() error {
	*p.calls = append(*p.calls, "probe")
	return nil
}

func TestDrainOrderAndFailureRetainLock(t *testing.T) {
	dir := privateDataDir(t)
	s := openTestStorage(t, dir)
	defer func() {
		if err := closeStartup(s); err != nil {
			t.Error(err)
		}
	}()
	for _, fail := range []error{context.DeadlineExceeded, nil} {
		var calls []string
		stages := testDrain{&calls, fail}
		err := drainServer(t.Context(), stages, stages, stages, testProbe{&calls})
		want := []string{"http", "capture", "storage", "probe"}
		if fail != nil {
			want = want[:2]
		}
		if !errors.Is(err, fail) || !slices.Equal(calls, want) {
			t.Fatalf("ordered drain: calls=%v, err=%v, want=%v, %v", calls, err, want, fail)
		}
		if other, err := wal.OpenStreaming(dir, tailLimit); err == nil {
			t.Fatal(errors.Join(errors.New("WAL lock released before final close"), other.Close()))
		}
	}
}

func TestRetainedResourceHandlerReplaysHeldCursor(t *testing.T) {
	s := openTestStorage(t, privateDataDir(t))
	appendTestLine(t, s, 1, "needle-a")
	appendTestLine(t, s, 2, "needle-b")
	probe, err := openProbe(s)
	if err != nil {
		t.Fatal(err)
	}
	probeCtx, stopProbe := context.WithCancel(t.Context())
	handler, api := newReadyHandler(probeCtx, config{maxResponseBytes: 1024, queryConcurrency: 2, queryWorkBudget: 1 << 20}, s, probe)
	defer func() {
		stopProbe()
		if err := errors.Join(api.Close(t.Context()), probe.Close(), closeStartup(s)); err != nil {
			t.Error(err)
		}
	}()
	const filter = "/query?start=2025-01-01T00:00:00Z&end=2025-01-01T00:00:03Z&contains=needle&limit=1"
	request := func(uri string) *httptest.ResponseRecorder {
		out := httptest.NewRecorder()
		handler.ServeHTTP(out, httptest.NewRequest(http.MethodGet, uri, nil))
		if out.Code != http.StatusOK {
			t.Fatalf("GET %s: %d %s", uri, out.Code, out.Body.String())
		}
		return out
	}
	first := request(filter + "&page=1")
	cursor := first.Header().Get("Bog-Next-Cursor")
	if cursor == "" {
		t.Fatalf("opening gave no cursor: %v", first.Header())
	}
	appendTestLine(t, s, 0, "needle-backfill")
	if err := s.advance(t.Context()); err != nil {
		t.Fatal(err)
	}
	const latter = "{\"ts\":\"2025-01-01T00:00:02Z\",\"line\":\"needle-b\"}\n"
	for range 2 {
		got := request(filter + "&cursor=" + cursor)
		if got.Body.String() != latter || got.Header().Get("Bog-Next-Cursor") != "" {
			t.Fatalf("held cursor got %q, headers %v", got.Body.String(), got.Header())
		}
	}
}
