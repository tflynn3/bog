package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type exportSaveHook struct {
	ExportProgressStore
	save func(context.Context, ExportProgress) error
}

func (s exportSaveHook) Save(ctx context.Context, p ExportProgress) error {
	return s.save(ctx, p)
}

func newTestExportStore(t *testing.T, path string) *FileExportProgressStore {
	t.Helper()
	store, err := NewFileExportProgressStore(path)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func newTestExport(t *testing.T, c *Client, q Query, budget int, store ExportProgressStore, sink ExportSink) *ExportSession {
	t.Helper()
	session, err := c.NewExportSession(q, budget, store, sink)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func newTestExportClient(t *testing.T, target string) *Client {
	t.Helper()
	c, err := New(target, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseIdleConnections)
	return c
}

func loadTestExport(t *testing.T, store ExportProgressStore) ExportProgress {
	t.Helper()
	progress, found, err := store.Load(t.Context(), DefaultMaxPage)
	if err != nil || !found {
		t.Fatalf("load checkpoint: found=%t, error=%v", found, err)
	}
	return progress
}

func TestExportAcknowledgmentTransitions(t *testing.T) {
	failure := errors.New("interrupted export")
	for _, tc := range []struct {
		name          string
		stage         string
		page          int
		committed     bool
		wantAck       int
		wantCursor    string
		wantOpening   bool
		wantComplete  bool
		wantDelivered []int
		wantRequests  []string
	}{
		{"journal failed before install", "journal", 1, false, -1, "", false, false, []int{1, 2}, []string{"", "", fixtureCursor}},
		{"journal outcome ambiguous", "journal", 1, true, 0, "", true, false, []int{1, 2}, []string{"", fixtureCursor}},
		{"opening sink interrupted", "sink", 1, false, 0, "", true, false, []int{1, 1, 2}, []string{"", fixtureCursor}},
		{"opening acknowledgment save failed", "save", 1, false, 0, "", true, false, []int{1, 1, 2}, []string{"", fixtureCursor}},
		{"opening acknowledgment saved before interruption", "save", 1, true, 1, fixtureCursor, false, false, []int{1, 2}, []string{"", fixtureCursor}},
		{"terminal sink interrupted", "sink", 2, false, 1, fixtureCursor, false, false, []int{1, 2, 2}, []string{"", fixtureCursor, fixtureCursor}},
		{"terminal acknowledgment save failed", "save", 2, false, 1, fixtureCursor, false, false, []int{1, 2, 2}, []string{"", fixtureCursor, fixtureCursor}},
		{"terminal saved before interruption", "save", 2, true, 2, "", false, true, []int{1, 2}, []string{"", fixtureCursor}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "export.progress")
			store := newTestExportStore(t, path)
			c := newTestExportClient(t, "http://bog.test/instance")
			// Wire escapes and whitespace differ from re-encoded rows.
			opening := "{ \"ts\": \"" + contractStamp + "\", \"line\": \"\\u0074ie\" }\n" + contractRow("tie")
			terminal := contractRow("last")
			var requests []string
			openings := 0
			c.http.Transport = queryRoundTripper(func(r *http.Request) (*http.Response, error) {
				cursor := r.URL.Query().Get("cursor")
				requests = append(requests, cursor)
				if r.URL.Path != "/instance/query" || r.URL.Query().Get("limit") != "2" {
					t.Fatalf("wrong request: %s", r.URL)
				}
				if cursor == "" {
					openings++
					if r.URL.Query().Get("page") != "1" {
						t.Fatal("opening request omitted page=1")
					}
					body := opening
					if openings > 1 {
						body = contractRow("new snapshot")
					}
					return exportTestResponse(body, fixtureCursor), nil
				}
				return exportTestResponse(terminal, ""), nil
			})
			injected := exportSaveHook{ExportProgressStore: store, save: func(ctx context.Context, p ExportProgress) error {
				failSave := (tc.stage == "journal" && p.Opening != nil) ||
					(tc.stage == "save" && p.AcknowledgedPages == tc.page)
				if !failSave || tc.committed {
					if err := store.Save(ctx, p); err != nil {
						return err
					}
				}
				if failSave {
					return failure
				}
				return nil
			}}
			var delivered []int
			sink := func(_ context.Context, delivery ExportPage) error {
				delivered = append(delivered, delivery.Number)
				prior := loadTestExport(t, store)
				switch delivery.Number {
				case 1:
					wantRows := []string{"tie", "tie"}
					if openings > 1 {
						wantRows = []string{"new snapshot"}
					}
					if delivery.RequestCursor != "" || delivery.Page.NextCursor != fixtureCursor ||
						requests[len(requests)-1] != "" || !slices.Equal(exportLines(delivery.Page), wantRows) ||
						prior.AcknowledgedPages != 0 || prior.Opening == nil ||
						(openings == 1 && string(prior.Opening.Body) != opening) {
						t.Fatalf("opening delivery %+v with checkpoint %+v and requests %q", delivery, prior, requests)
					}
					// The sink owns the decoded rows. Its mutation must
					// never affect the journal or a subsequent delivery.
					delivery.Page.Rows[0].Line = "mutated by sink"
				case 2:
					if delivery.RequestCursor != fixtureCursor || delivery.Page.NextCursor != "" ||
						!slices.Equal(exportLines(delivery.Page), []string{"last"}) ||
						prior.AcknowledgedPages != 1 || prior.Cursor != fixtureCursor || prior.Opening != nil {
						t.Fatalf("terminal delivery %+v with checkpoint %+v", delivery, prior)
					}
				default:
					t.Fatalf("unexpected delivery %+v", delivery)
				}
				return nil
			}
			q := Query{Limit: 2}
			session := newTestExport(t, c, q, 2, injected, func(ctx context.Context, p ExportPage) error {
				if err := sink(ctx, p); err != nil {
					return err
				}
				if tc.stage == "sink" && p.Number == tc.page {
					return failure
				}
				return nil
			})
			if err := session.Run(t.Context()); !errors.Is(err, failure) {
				t.Fatalf("first run: %v", err)
			}
			progress, found, err := store.Load(t.Context(), DefaultMaxPage)
			if err != nil || found != (tc.wantAck >= 0) || found &&
				(progress.AcknowledgedPages != tc.wantAck || progress.Cursor != tc.wantCursor ||
					(progress.Opening != nil) != tc.wantOpening || progress.Complete != tc.wantComplete) {
				t.Fatalf("checkpoint after failure: %+v, found=%t, error=%v", progress, found, err)
			}

			resumed := newTestExport(t, c, q, 2, newTestExportStore(t, path), sink)
			for range 2 { // The second successful run must be a no-op.
				if err := resumed.Run(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if !slices.Equal(delivered, tc.wantDelivered) || !slices.Equal(requests, tc.wantRequests) {
				t.Fatalf("delivered %v, requested %q", delivered, requests)
			}
			finished := loadTestExport(t, store)
			if !finished.Complete || finished.AcknowledgedPages != 2 || finished.Opening != nil || finished.Cursor != "" {
				t.Fatalf("final checkpoint: %+v", finished)
			}
		})
	}
}

func exportTestResponse(body, next string) *http.Response {
	resp := &http.Response{
		StatusCode:    http.StatusOK,
		ProtoMajor:    1,
		Header:        make(http.Header),
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
	if next != "" {
		resp.Header.Set(nextCursorHeader, next)
	}
	return resp
}

func exportLines(p Page) []string {
	var lines []string
	for _, row := range p.Rows {
		lines = append(lines, row.Line)
	}
	return lines
}

func TestExportTransportInterruption(t *testing.T) {
	for _, openingInterrupted := range []bool{true, false} {
		t.Run(strconv.FormatBool(openingInterrupted), func(t *testing.T) {
			t.Parallel()
			var openings, continuations atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				const partialLine = "response interrupted at a record boundary"
				body, next := contractRow("first"), fixtureCursor
				var interrupted bool
				switch cursor := r.URL.Query().Get("cursor"); cursor {
				case "":
					interrupted = openings.Add(1) == 1 && openingInterrupted
				case fixtureCursor:
					interrupted = continuations.Add(1) == 1 && !openingInterrupted
					body, next = contractRow("last"), ""
				default:
					t.Errorf("unexpected cursor %q", cursor)
					http.Error(w, "unexpected cursor", http.StatusBadRequest)
					return
				}
				if interrupted {
					body = contractRow(partialLine)
					w.Header().Set("Content-Length", strconv.Itoa(len(body)+7))
					if _, err := io.WriteString(w, body); err != nil {
						t.Errorf("write truncated response: %v", err)
					}
					return
				}
				writeContractPage(t, w, body, next)
			}))
			t.Cleanup(server.Close)
			client := newTestExportClient(t, server.URL)
			path := filepath.Join(t.TempDir(), "progress")
			store := newTestExportStore(t, path)
			var lines []string
			sink := func(_ context.Context, p ExportPage) error {
				lines = append(lines, exportLines(p.Page)...)
				return nil
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			session := newTestExport(t, client, Query{}, 2, store, sink)
			if err := session.Run(ctx); !errors.Is(err, ErrPageInterrupted) || !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("incomplete frame: %v", err)
			}
			if openingInterrupted {
				if _, found, err := store.Load(ctx, DefaultMaxPage); err != nil || found || len(lines) != 0 {
					t.Fatalf("partial opening persisted or delivered: %t, %v, %v", found, err, lines)
				}
			} else if progress := loadTestExport(t, store); progress.Cursor != fixtureCursor ||
				progress.AcknowledgedPages != 1 || !slices.Equal(lines, []string{"first"}) {
				t.Fatalf("interrupted continuation: %+v, %v", progress, lines)
			}
			if err := newTestExport(t, client, Query{}, 2, newTestExportStore(t, path), sink).Run(ctx); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(lines, []string{"first", "last"}) ||
				(openingInterrupted && (openings.Load() != 2 || continuations.Load() != 1)) ||
				(!openingInterrupted && (openings.Load() != 1 || continuations.Load() != 2)) {
				t.Fatalf("delivered %v, opening calls %d, continuation calls %d", lines, openings.Load(), continuations.Load())
			}
		})
	}
}

func TestExportCancelAndExcludeConcurrentRun(t *testing.T) {
	c := newTestExportClient(t, "http://bog.test")
	store := newTestExportStore(t, filepath.Join(t.TempDir(), "progress"))
	var requests atomic.Int32
	c.http.Transport = queryRoundTripper(func(r *http.Request) (*http.Response, error) {
		requests.Add(1)
		if r.URL.Query().Get("cursor") == "" {
			return exportTestResponse(contractRow("first"), fixtureCursor), nil
		}
		return exportTestResponse(contractRow("last"), ""), nil
	})
	entered := make(chan struct{})
	var deliveries atomic.Int32
	sink := func(ctx context.Context, page ExportPage) error {
		if deliveries.Add(1) == 1 {
			close(entered)
			<-ctx.Done()
			// Even nil cannot advance with an already-canceled context.
			return nil
		}
		return nil
	}
	session := newTestExport(t, c, Query{}, 2, store, sink)
	deadline, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	ctx, cancel := context.WithCancelCause(deadline)
	defer cancel(nil)
	done := make(chan error, 1)
	go func() { done <- session.Run(ctx) }()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("run stopped before sink: %v", err)
	case <-deadline.Done():
		t.Fatal("run never entered the sink")
	}
	if err := session.Run(deadline); !errors.Is(err, ErrExportRunning) {
		t.Fatalf("overlapping run: %v", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("requests while sink was blocked: %d", requests.Load())
	}
	cause := errors.New("stop export")
	cancel(cause)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
			t.Fatalf("canceled run: %v", err)
		}
	case <-deadline.Done():
		t.Fatal("canceled run did not exit")
	}
	if progress := loadTestExport(t, store); progress.AcknowledgedPages != 0 || progress.Opening == nil {
		t.Fatalf("progress advanced after canceled acknowledgment: %+v", progress)
	}
	if err := session.Run(deadline); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 2 || deliveries.Load() != 3 {
		t.Fatalf("requests=%d deliveries=%d", requests.Load(), deliveries.Load())
	}
}

func TestExportCancellationAfterDurableAdvance(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(strconv.FormatBool(terminal), func(t *testing.T) {
			t.Parallel()
			c := newTestExportClient(t, "http://bog.test")
			store := newTestExportStore(t, filepath.Join(t.TempDir(), "progress"))
			var requests []string
			c.http.Transport = queryRoundTripper(func(req *http.Request) (*http.Response, error) {
				cursor := req.URL.Query().Get("cursor")
				requests = append(requests, cursor)
				if cursor == "" && !terminal {
					return exportTestResponse(contractRow("first"), fixtureCursor), nil
				}
				return exportTestResponse(contractRow("terminal"), ""), nil
			})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			hook := exportSaveHook{ExportProgressStore: store, save: func(ctx context.Context, p ExportProgress) error {
				if err := store.Save(ctx, p); err != nil {
					return err
				}
				if p.AcknowledgedPages == 1 {
					cancel()
				}
				return nil
			}}
			var deliveries []int
			sink := func(_ context.Context, p ExportPage) error {
				deliveries = append(deliveries, p.Number)
				return nil
			}
			if err := newTestExport(t, c, Query{}, 2, hook, sink).Run(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("run canceled after save: %v", err)
			}
			if p := loadTestExport(t, store); p.AcknowledgedPages != 1 || p.Complete != terminal ||
				len(requests) != 1 || len(deliveries) != 1 {
				t.Fatalf("progress at cancellation: %+v, requests %q, deliveries %v", p, requests, deliveries)
			}
			if err := newTestExport(t, c, Query{}, 2, store, sink).Run(t.Context()); err != nil {
				t.Fatal(err)
			}
			wantRequests, wantDeliveries := []string{""}, []int{1}
			if !terminal {
				wantRequests, wantDeliveries = append(wantRequests, fixtureCursor), append(wantDeliveries, 2)
			}
			if !slices.Equal(requests, wantRequests) || !slices.Equal(deliveries, wantDeliveries) {
				t.Fatalf("resume made requests %q and deliveries %v", requests, deliveries)
			}
		})
	}
}

func TestExportDetectsCursorCycleAcrossRestart(t *testing.T) {
	c := newTestExportClient(t, "http://bog.test")
	path := filepath.Join(t.TempDir(), "progress")
	store := newTestExportStore(t, path)
	var requests []string
	c.http.Transport = queryRoundTripper(func(r *http.Request) (*http.Response, error) {
		cursor := r.URL.Query().Get("cursor")
		requests = append(requests, cursor)
		if cursor == fixtureCursor {
			return exportTestResponse("", fixtureNext), nil
		}
		return exportTestResponse("", fixtureCursor), nil
	})
	var deliveries []int
	sink := func(_ context.Context, p ExportPage) error {
		deliveries = append(deliveries, p.Number)
		return nil
	}
	if err := newTestExport(t, c, Query{}, 2, store, sink).Run(t.Context()); !errors.Is(err, ErrPageBudget) {
		t.Fatal(err)
	}
	previous := loadTestExport(t, store)
	if err := newTestExport(t, c, Query{}, 3, newTestExportStore(t, path), sink).Run(t.Context()); !errors.Is(err, errCursorCycle) {
		t.Fatalf("cycling continuation: %v", err)
	}
	if !slices.Equal(requests, []string{"", fixtureCursor, fixtureNext}) || !slices.Equal(deliveries, []int{1, 2}) ||
		loadTestExport(t, store) != previous {
		t.Fatalf("cycle advanced checkpoint or delivered a page: %q, %v", requests, deliveries)
	}
}
