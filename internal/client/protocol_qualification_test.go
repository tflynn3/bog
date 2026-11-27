package client

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	qualificationFilter = "keep + / café"
	qualificationEarly  = "{\"ts\":\"2025-03-01T00:00:00Z\",\"line\":\"keep + / café early\"}\n"
	qualificationTie    = "{\"ts\":\"2025-03-01T00:00:01Z\",\"line\":\"keep + / café tie\"}\n"
	qualificationOther  = "{\"ts\":\"2025-03-01T00:00:01Z\",\"line\":\"keep + / café other\"}\n"
	qualificationLast   = "{\"ts\":\"2025-03-01T00:00:02Z\",\"line\":\"keep + / café last\"}\n"
	qualificationSeed   = qualificationTie + qualificationOther + qualificationLast + qualificationEarly + qualificationTie +
		"{\"ts\":\"2025-03-01T00:00:01Z\",\"line\":\"excluded\"}\n" +
		"{\"ts\":\"2025-03-01T00:00:03Z\",\"line\":\"keep + / café at end\"}\n"
	qualificationWant = qualificationEarly + qualificationTie + qualificationOther + qualificationTie + qualificationLast
)

var qualificationStart = time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)

func qualificationQuery() Query {
	return Query{Start: qualificationStart, End: qualificationStart.Add(3 * time.Second), Contains: qualificationFilter, Limit: 2}
}

func serveQualificationPage(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	switch r.URL.Query().Get("cursor") {
	case "":
		writeContractPage(t, w, qualificationEarly+qualificationTie, fixtureCursor)
	case fixtureCursor:
		writeContractPage(t, w, qualificationOther+qualificationTie, fixtureNext)
	case fixtureNext:
		writeContractPage(t, w, qualificationLast, "")
	default:
		t.Errorf("unexpected cursor in %s", r.RequestURI)
		http.Error(w, "unexpected cursor", http.StatusBadRequest)
	}
}

type qualificationExchange struct {
	target string
	next   string
}

// The proxy forwards real HTTP responses and cuts the first continuation
// immediately after one complete row, preserving the advertised frame size
// and successor. A client that accepts a partial page would skip a row.
func qualificationProxy(t *testing.T, base string) (string, func() []qualificationExchange) {
	t.Helper()
	target, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{}
	t.Cleanup(transport.CloseIdleConnections)
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = transport
	var mu sync.Mutex
	var exchanges []qualificationExchange
	proxy.ModifyResponse = func(resp *http.Response) error {
		mu.Lock()
		exchanges = append(exchanges, qualificationExchange{resp.Request.URL.RequestURI(), resp.Header.Get(nextCursorHeader)})
		cut := len(exchanges) == 2
		mu.Unlock()
		if !cut {
			return nil
		}
		prefix, err := bufio.NewReader(resp.Body).ReadSlice('\n')
		closeErr := resp.Body.Close()
		resp.Body = io.NopCloser(bytes.NewReader(prefix))
		if err != nil {
			return errors.Join(fmt.Errorf("read complete row to truncate continuation: %w", err), closeErr)
		}
		if resp.StatusCode != http.StatusOK || resp.ContentLength <= int64(len(prefix)) ||
			resp.Request.URL.Query().Get("cursor") == "" || resp.Header.Get(nextCursorHeader) == "" {
			return errors.Join(fmt.Errorf("cannot cut response after first row: status=%d content length=%d prefix=%d",
				resp.StatusCode, resp.ContentLength, len(prefix)), closeErr)
		}
		return closeErr
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		t.Errorf("qualification proxy %s: %v", r.RequestURI, err)
		http.Error(w, "proxy error", http.StatusBadGateway)
	}
	server := httptest.NewServer(proxy)
	t.Cleanup(server.Close)
	return server.URL, func() []qualificationExchange {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(exchanges)
	}
}

func assertQualificationReplay(t *testing.T, got []qualificationExchange) {
	t.Helper()
	if len(got) != 4 {
		t.Fatalf("query exchanges = %+v, want opening, interrupted continuation, retry and terminal", got)
	}
	if got[0].next == "" || got[1].next == "" || got[1].next != got[2].next || got[3].next != "" || got[1].target != got[2].target {
		t.Fatalf("cursor chain or exact request replay mismatch: %+v", got)
	}
	expectedCursors := []string{"", got[0].next, got[0].next, got[2].next}
	q := qualificationQuery()
	for i, exchange := range got {
		u, err := url.ParseRequestURI(exchange.target)
		if err != nil {
			t.Fatal(err)
		}
		want := url.Values{
			"start": {q.Start.Format(time.RFC3339Nano)}, "end": {q.End.Format(time.RFC3339Nano)},
			"contains": {qualificationFilter}, "limit": {"2"},
		}
		if i == 0 {
			want.Set("page", "1")
		} else {
			want.Set("cursor", expectedCursors[i])
		}
		if u.Path != "/query" || u.Query().Encode() != want.Encode() {
			t.Errorf("exchange %d target %s, want /query?%s", i+1, exchange.target, want.Encode())
		}
	}
}

func TestBoundedWalkRetriesTruncatedContinuation(t *testing.T) {
	t.Parallel()
	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveQualificationPage(t, w, r)
	}))
	t.Cleanup(issuer.Close)
	base, trace := qualificationProxy(t, issuer.URL)
	c := newTestExportClient(t, base)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	walk, err := c.NewBoundedWalkWithContext(ctx, qualificationQuery(), 3)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	for _, size := range []int{2, 2, 1} {
		page, err := walk.NextPageWithRetry(PageRetry{MaxRetries: 1})
		if err != nil || len(page.Rows) != size {
			t.Fatalf("walk page = %+v, %v, want %d rows", page, err, size)
		}
		for _, row := range page.Rows {
			if _, err := fmt.Fprintf(&output, "{\"ts\":%q,\"line\":%q}\n", row.TS.Format(time.RFC3339Nano), row.Line); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := walk.NextPage(); err != io.EOF {
		t.Fatalf("after terminal page got %v, want EOF", err)
	}
	if got := output.String(); got != qualificationWant {
		t.Errorf("walk ordered rows = %q, want %q", got, qualificationWant)
	}
	assertQualificationReplay(t, trace())
}

func TestExportFilePrePublicationFailures(t *testing.T) {
	rejected := errors.New("injected destination rename failure")
	for _, tc := range []struct {
		name     string
		maxPages int
		truncate bool
		rename   bool
		want     error
	}{
		{name: "truncated continuation", maxPages: 3, truncate: true, want: ErrPageInterrupted},
		{name: "page budget", maxPages: 2, want: ErrPageBudget},
		{name: "destination rename", maxPages: 3, rename: true, want: rejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := setupQualificationDestination(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				serveQualificationPage(t, w, r)
			}))
			t.Cleanup(server.Close)
			base := server.URL
			if tc.truncate {
				var trace func() []qualificationExchange
				base, trace = qualificationProxy(t, base)
				defer func() {
					if got := trace(); len(got) != 2 {
						t.Errorf("requests after truncation = %+v, want two", got)
					}
				}()
			}
			c := newTestExportClient(t, base)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			renameCalls := 0
			err := c.exportFile(ctx, qualificationQuery(), tc.maxPages, PageRetry{}, path, func(root *os.Root, stage, name string) error {
				renameCalls++
				assertQualificationFile(t, path, priorQualificationOutput)
				got, err := root.ReadFile(stage)
				if err != nil || string(got) != qualificationWant || name != filepath.Base(path) {
					t.Errorf("at rename stage=%q destination=%s error=%v", got, name, err)
				}
				return rejected
			})
			if !errors.Is(err, tc.want) || (renameCalls == 1) != tc.rename {
				t.Fatalf("export returned %v after %d renames, want %v", err, renameCalls, tc.want)
			}
			if tc.truncate && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Errorf("truncation error = %v, want unexpected EOF", err)
			}
			assertQualificationFile(t, path, priorQualificationOutput)
			assertQualificationDirectory(t, path)
		})
	}
}

func TestExportFileCancellationDuringContinuation(t *testing.T) {
	t.Parallel()
	path := setupQualificationDestination(t)
	ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	exportCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	cause := errors.New("cancel after continuation prefix")
	closed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch cursor := r.URL.Query().Get("cursor"); cursor {
		case "":
			serveQualificationPage(t, w, r)
		case fixtureCursor:
			w.Header().Set("Content-Length", strconv.Itoa(len(qualificationOther+qualificationTie)))
			w.Header().Set(nextCursorHeader, fixtureNext)
			if _, err := io.WriteString(w, qualificationOther); err != nil {
				t.Error(err)
				return
			}
			if err := http.NewResponseController(w).Flush(); err != nil {
				t.Error(err)
				return
			}
			select {
			case <-r.Context().Done():
				close(closed)
			case <-ctx.Done():
				t.Error("continuation connection remained open after cancellation")
			}
		default:
			t.Errorf("unexpected request after cancellation: %s", r.RequestURI)
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	c := newTestExportClient(t, server.URL)
	transport := c.http.Transport.(*http.Transport)
	t.Cleanup(transport.CloseIdleConnections)
	c.http.Transport = queryRoundTripper(func(req *http.Request) (*http.Response, error) {
		resp, err := transport.RoundTrip(req)
		if err != nil || req.URL.Query().Get("cursor") == "" {
			return resp, err
		}
		body := resp.Body
		resp.Body = queryResponseBody{queryReadFunc(func(p []byte) (int, error) {
			n, err := body.Read(p)
			if n > 0 {
				assertQualificationFile(t, path, priorQualificationOutput)
				matches, globErr := filepath.Glob(filepath.Join(filepath.Dir(path), exportDestinationStagePrefix+"*"))
				if globErr != nil || len(matches) != 1 {
					t.Errorf("staged files during cancellation = %v, %v", matches, globErr)
				} else {
					assertQualificationFile(t, matches[0], qualificationEarly+qualificationTie)
				}
				cancel(cause)
			}
			return n, err
		}), body.Close}
		return resp, nil
	})
	err := c.ExportFile(exportCtx, qualificationQuery(), 3, PageRetry{MaxRetries: 2}, path)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, cause) || errors.Is(err, ErrPageRetryExhausted) {
		t.Errorf("canceled export returned %v", err)
	}
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("issuer did not observe cancellation")
	}
	assertQualificationFile(t, path, priorQualificationOutput)
	assertQualificationDirectory(t, path)
}

func TestExportFileExistingDirectoryRejectsReplacement(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "destination")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	prior := filepath.Join(path, "previous")
	if err := os.WriteFile(prior, []byte(priorQualificationOutput), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveQualificationPage(t, w, r)
	}))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	c := newTestExportClient(t, server.URL)
	err := c.ExportFile(ctx, qualificationQuery(), 3, PageRetry{}, path)
	var renameErr *os.LinkError
	if !errors.As(err, &renameErr) || renameErr.Op != "renameat" && renameErr.Op != "rename" {
		t.Fatalf("replacing a directory: %v, want a filesystem rename error", err)
	}
	assertQualificationFile(t, prior, priorQualificationOutput)
	assertQualificationDirectory(t, path)
}

func TestExportFileAtomicPublication(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveQualificationPage(t, w, r)
	}))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	qualifyAtomicExport(t, ctx, server.URL)
}

const priorQualificationOutput = "previous export\n"

func setupQualificationDestination(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "rows.ndjson")
	if err := os.WriteFile(path, []byte(priorQualificationOutput), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertQualificationFile(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || string(got) != want {
		t.Errorf("read %s = %q, %v, want %q", path, got, err, want)
	}
}

func assertQualificationDirectory(t *testing.T, path string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(path) {
		t.Errorf("destination directory entries = %v, want only %s", entries, filepath.Base(path))
	}
}

func qualifyAtomicExport(t *testing.T, ctx context.Context, issuer string) {
	t.Helper()
	path := setupQualificationDestination(t)
	old, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := old.Close(); err != nil {
			t.Error(err)
		}
	}()
	base, trace := qualificationProxy(t, issuer)
	c := newTestExportClient(t, base)

	renames := 0
	err = c.exportFile(ctx, qualificationQuery(), 3, PageRetry{MaxRetries: 1}, path,
		func(root *os.Root, stage, name string) error {
			renames++
			assertQualificationReplay(t, trace())
			assertQualificationFile(t, path, priorQualificationOutput)
			info, err := root.Stat(stage)
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
				t.Errorf("stage mode = %v, want private regular file", info.Mode())
			}
			staged, err := root.ReadFile(stage)
			if err != nil {
				return err
			}
			if string(staged) != qualificationWant {
				t.Errorf("complete stage = %q, want %q", staged, qualificationWant)
			}
			return root.Rename(stage, name)
		})
	if err != nil || renames != 1 {
		t.Fatalf("atomic export = %v after %d renames, want success after one rename", err, renames)
	}
	assertQualificationFile(t, path, qualificationWant)
	if data, err := io.ReadAll(old); err != nil || string(data) != priorQualificationOutput {
		t.Errorf("old open descriptor = %q, %v, want %q", data, err, priorQualificationOutput)
	}
	assertQualificationDirectory(t, path)
}
