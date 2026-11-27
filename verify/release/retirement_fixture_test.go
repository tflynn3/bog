package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"bog/internal/httpapi"
	"bog/internal/store"
)

const (
	retirementFixtureBudget = 15 * time.Second
	retirementReaderBudget  = 8 * time.Second
	retirementTail          = "old-tail"
	retirementTailContents  = "held terminal row"
	retirementReaderHeader  = "X-Retirement-Reader"
	retirementPageCap       = 256
)

var retirementPredecessor = []string{"old-manifest", retirementTail, "old-segment"}
var retirementSuccessor = []string{"new-manifest", "new-tail", "new-segment"}

// Only the test binary exposes these barriers. The query admission, cursor
// replay and retirement paths inside the fixture use the real HTTP API.
func TestRetirementFixtureProcess(t *testing.T) {
	args := os.Args[1:]
	for len(args) > 0 && args[0] != "--" {
		args = args[1:]
	}
	if len(args) == 0 {
		return
	}
	flags := flag.NewFlagSet("retirement fixture", flag.ContinueOnError)
	dir := flags.String("data-dir", "", "")
	addr := flags.String("addr", "", "")
	if err := flags.Parse(args[1:]); err != nil {
		t.Fatal(err)
	}
	if *dir == "" || *addr == "" || flags.NArg() != 0 {
		t.Fatal("retirement fixture requires a data directory and listener address")
	}
	if err := serveRetirementFixture(*dir, *addr); err != nil {
		t.Fatal(err)
	}
}

func serveRetirementFixture(dir, addr string) (err error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	source := &retirementFixtureSource{root: root}
	api := httpapi.NewWithPageSource(source, nil, retirementPageCap, 4, httpapi.DefaultQueryWorkBudget)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), candidateStopTimeout)
		defer cancel()
		err = errors.Join(err, api.Close(ctx))
	}()

	listener, err := net.Listen("tcp4", addr)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: newRetirementFixtureHandler(api, source), ReadHeaderTimeout: time.Second}
	err = server.Serve(listener)
	ctx, cancel := context.WithTimeout(context.Background(), candidateStopTimeout)
	defer cancel()
	if shutdownErr := server.Shutdown(ctx); shutdownErr != nil {
		err = errors.Join(err, shutdownErr, server.Close())
	}
	return err
}

func newRetirementFixtureHandler(api *httpapi.API, source *retirementFixtureSource) http.Handler {
	fixture := retirementFixtureHTTP{api: api, source: source}
	for i := range fixture.readers {
		fixture.readers[i] = retirementReader{entered: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/_capacity/compaction", fixture.compaction)
	mux.HandleFunc("/fixture/reader", fixture.reader)
	mux.HandleFunc("/query", fixture.query)
	mux.Handle("/", api)
	return mux
}

type retirementFixtureSource struct {
	root *os.Root
	mu   sync.Mutex // protects publication, descriptor ownership and release errors
	tail *os.File

	published  bool
	reclaimErr error
}

const retirementCapture = "predecessor"

func (s *retirementFixtureSource) Page(ctx context.Context, req httpapi.PageRequest, work *store.QueryWork) (httpapi.PageResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return httpapi.PageResult{}, err
	}

	result := httpapi.PageResult{}
	line := retirementTailContents
	if req.Capture == retirementCapture || req.Snapshot && req.Capture == "" {
		if req.Capture == "" {
			if s.tail != nil || s.published {
				return result, errors.New("fixture allows one predecessor capture before publication")
			}
			tail, err := s.root.Open(retirementTail)
			if err != nil {
				return result, err
			}
			s.tail = tail
		} else if s.tail == nil {
			return result, httpapi.ErrCaptureExpired
		}
		raw := make([]byte, len(retirementTailContents))
		if _, err := s.tail.ReadAt(raw, 0); err != nil {
			if req.Capture == "" {
				err = errors.Join(err, s.releaseLocked())
			}
			return result, err
		}
		line = string(raw)
		result.Capture = retirementCapture
	} else if req.Capture != "" {
		return result, httpapi.ErrCaptureExpired
	}

	rows := []httpapi.PageRow{
		{Position: "1", Row: store.Row{TS: fixtureTime(1), Line: "held first row"}},
		{Position: "2", Row: store.Row{TS: fixtureTime(2), Line: line}},
	}
	if s.published && !req.Snapshot {
		rows = append([]httpapi.PageRow{{Position: "0", Row: store.Row{TS: fixtureTime(0), Line: "successor backfill"}}}, rows...)
	}
	if err := work.Charge(uint64(len(rows))); err != nil {
		if req.Snapshot && req.Capture == "" {
			err = errors.Join(err, s.releaseLocked())
		}
		return httpapi.PageResult{}, err
	}
	for _, item := range rows {
		if item.Position <= req.After || item.Row.TS.Before(req.Filter.Start) || !item.Row.TS.Before(req.Filter.End) ||
			!strings.Contains(item.Row.Line, req.Filter.Contains) {
			continue
		}
		if len(result.Rows) == req.Limit {
			result.More = true
			break
		}
		result.Rows = append(result.Rows, item)
	}
	return result, nil
}

func (s *retirementFixtureSource) Release(capture string) error {
	if capture != retirementCapture {
		return fmt.Errorf("unknown fixture capture %q", capture)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.releaseLocked()
}

func (s *retirementFixtureSource) releaseLocked() error {
	if s.tail == nil {
		return s.reclaimErr
	}
	if s.published {
		for _, name := range retirementPredecessor {
			s.reclaimErr = errors.Join(s.reclaimErr, s.root.Remove(name))
		}
	}
	s.reclaimErr = errors.Join(s.reclaimErr, s.tail.Close())
	s.tail = nil
	return s.reclaimErr
}

type retirementFixtureHTTP struct {
	api     *httpapi.API
	source  *retirementFixtureSource
	readers [2]retirementReader
}

func (f *retirementFixtureHTTP) compaction(w http.ResponseWriter, r *http.Request) {
	switch r.Method + " " + r.URL.Query().Get("action") {
	case "POST retire":
		if err := f.api.RetireQueryCapture(r.URL.Query().Get("cursor")); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	case "POST start", "GET status":
	default:
		http.Error(w, "invalid fixture compaction request", http.StatusBadRequest)
		return
	}

	s := f.source
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reclaimErr != nil {
		http.Error(w, s.reclaimErr.Error(), http.StatusInternalServerError)
		return
	}
	if r.Method == http.MethodPost {
		if r.URL.Query().Get("cutoff") != "2" || s.published || s.tail == nil {
			http.Error(w, "unexpected fixture publication", http.StatusConflict)
			return
		}
		s.published = true
	}
	status := map[string]any{
		"state": "complete", "generation": 1, "covered_rows": 1, "committed_rows": 3,
		"metrics": map[string]int{"retired_generation_pins": 0},
	}
	if s.published {
		status["generation"], status["covered_rows"] = 2, 2
		if s.tail != nil {
			status["metrics"] = map[string]int{"retired_generation_pins": 1}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(status); err != nil {
		panic(http.ErrAbortHandler) // Surface a failed body write to the verifier.
	}
}

type retirementReader struct {
	admitted sync.Once
	released sync.Once
	entered  chan struct{}
	release  chan struct{}
	done     chan struct{}
}

func (f *retirementFixtureHTTP) query(w http.ResponseWriter, r *http.Request) {
	id := r.Header.Get(retirementReaderHeader)
	if id == "" {
		f.api.ServeHTTP(w, r)
		return
	}
	reader := f.findReader(id)
	if reader == nil || r.URL.Query().Get("cursor") == "" {
		http.Error(w, "invalid fixture reader", http.StatusBadRequest)
		return
	}
	admitted := false
	reader.admitted.Do(func() { admitted = true })
	if !admitted {
		http.Error(w, "duplicate fixture reader", http.StatusConflict)
		return
	}
	defer close(reader.done) // The API's query and capture defers have run.
	ctx, cancel := context.WithTimeout(r.Context(), retirementReaderBudget)
	defer cancel()
	f.api.ServeHTTP(&retirementBarrierWriter{ResponseWriter: w, ctx: ctx, reader: reader}, r.WithContext(ctx))
}

type retirementBarrierWriter struct {
	http.ResponseWriter
	ctx    context.Context
	reader *retirementReader
}

func (w *retirementBarrierWriter) WriteHeader(status int) {
	if status == http.StatusOK {
		// Header preparation follows admission and scan. Block before sending
		// it so the parent can prove the API still holds the capture.
		close(w.reader.entered)
		select {
		case <-w.reader.release:
		case <-w.ctx.Done():
			panic(http.ErrAbortHandler) // Unwind API defers and abort the connection.
		}
	}
	w.ResponseWriter.WriteHeader(status)
}

func (f *retirementFixtureHTTP) findReader(id string) *retirementReader {
	index, err := strconv.Atoi(id)
	if err != nil || index < 0 || index >= len(f.readers) {
		return nil
	}
	return &f.readers[index]
}

func (f *retirementFixtureHTTP) reader(w http.ResponseWriter, r *http.Request) {
	reader := f.findReader(r.URL.Query().Get("id"))
	if reader == nil {
		http.Error(w, "unknown fixture reader", http.StatusBadRequest)
		return
	}
	var barrier <-chan struct{}
	switch r.Method + " " + r.URL.Query().Get("action") {
	case "GET entered":
		barrier = reader.entered
	case "POST release":
		select {
		case <-reader.entered:
			reader.released.Do(func() { close(reader.release) })
			barrier = reader.done
		default:
			http.Error(w, "release before reader admission", http.StatusConflict)
			return
		}
	default:
		http.Error(w, "unknown fixture barrier", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), retirementReaderBudget)
	defer cancel()
	select {
	case <-barrier:
		w.WriteHeader(http.StatusNoContent)
	case <-ctx.Done():
		http.Error(w, ctx.Err().Error(), http.StatusRequestTimeout)
	}
}

func retirementFixtureBinary(t *testing.T) string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	quoted := "'" + strings.ReplaceAll(executable, "'", "'\\''") + "'"
	binary := filepath.Join(t.TempDir(), "retirement-fixture")
	script := "#!/bin/sh\nexec " + quoted + " -test.run='^TestRetirementFixtureProcess$' -- \"$@\"\n"
	if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	return binary
}

type retirementFixtureTimeout struct {
	phase string
	cause error
}

func (e *retirementFixtureTimeout) Error() string {
	return fmt.Sprintf("retirement fixture timed out during %s: %v", e.phase, e.cause)
}

func (e *retirementFixtureTimeout) Unwrap() error { return e.cause }

func retirementPhase(phase string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &retirementFixtureTimeout{phase: phase, cause: err}
	}
	return fmt.Errorf("%s: %w", phase, err)
}

func retirementBarrier(ctx context.Context, client *http.Client, base, method, action string, id int) error {
	target := fmt.Sprintf("%s/fixture/reader?action=%s&id=%d", base, action, id)
	phase := fmt.Sprintf("reader %d %s barrier", id, action)
	res, err := request(ctx, client, method, target, nil)
	if err != nil {
		return retirementPhase(phase, err)
	}
	if res.status == http.StatusRequestTimeout {
		return &retirementFixtureTimeout{phase: phase, cause: context.DeadlineExceeded}
	}
	if res.status != http.StatusNoContent || len(res.body) != 0 || res.cursorHeader {
		return fmt.Errorf("%s: expected empty HTTP 204, got HTTP %d with body %q", phase, res.status, res.body)
	}
	return nil
}

type retirementReaderKey struct{}

type retirementReaderTransport struct {
	http.RoundTripper
}

func (t retirementReaderTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if id, ok := req.Context().Value(retirementReaderKey{}).(int); ok {
		req = req.Clone(req.Context())
		req.Header.Set(retirementReaderHeader, strconv.Itoa(id))
	}
	return t.RoundTripper.RoundTrip(req)
}

// Run the HTTP and storage fixture contract on portable hosts as well. The
// Linux subprocess test additionally attributes the open file to its PID and
// exercises the two admitted readers across publication and retirement.
func TestRetirementFixtureContract(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTailFixture(t, root) })
	for _, name := range retirementPredecessor {
		writeFixture(t, filepath.Join(dir, name), []byte(retirementTailContents))
	}
	source := &retirementFixtureSource{root: root}
	api := httpapi.NewWithPageSource(source, nil, retirementPageCap, 4, httpapi.DefaultQueryWorkBudget)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), candidateStopTimeout)
		defer cancel()
		if err := api.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	server := httptest.NewServer(newRetirementFixtureHandler(api, source))
	t.Cleanup(server.Close)
	raw := &http.Client{Transport: retirementReaderTransport{server.Client().Transport}}
	ctx, cancel := context.WithTimeout(t.Context(), retirementReaderBudget)
	defer cancel()
	pager, err := newReleasePager(server.URL, raw, retirementPageCap)
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := pager.fetch(ctx, "", 1, "")
	if err != nil || len(first.Rows) != 1 || first.Rows[0].Line != "held first row" || first.NextCursor == "" {
		t.Fatalf("opening page = %+v, error = %v", first, err)
	}
	published, err := readCompactionObservation(ctx, raw, server.URL, http.MethodPost, "start", 2, true)
	if err != nil || published.generation != 2 || published.covered != 2 || published.retiredGenerationPins != 1 {
		t.Fatalf("published = %+v, error = %v", published, err)
	}
	replay, _, err := pager.fetch(ctx, "", 1, first.NextCursor)
	if err != nil || len(replay.Rows) != 1 || replay.Rows[0].Line != retirementTailContents || replay.NextCursor != "" {
		t.Fatalf("terminal page = %+v, error = %v", replay, err)
	}
	heldCtx, cancelHeld := context.WithCancel(context.WithValue(ctx, retirementReaderKey{}, 0))
	defer cancelHeld()
	held := make(chan error, 1)
	go func() {
		page, _, err := pager.fetch(heldCtx, "", 1, first.NextCursor)
		if err == nil && (len(page.Rows) != 1 || page.Rows[0].Line != retirementTailContents || page.NextCursor != "") {
			err = fmt.Errorf("held terminal page = %+v", page)
		}
		held <- err
	}()
	if err := retirementBarrier(ctx, raw, server.URL, http.MethodGet, "entered", 0); err != nil {
		t.Fatal(err)
	}
	params := url.Values{"action": {"retire"}, "cursor": {first.NextCursor}}
	res, err := request(ctx, raw, http.MethodPost, server.URL+"/_capacity/compaction?"+params.Encode(), nil)
	if err != nil || res.status != http.StatusNoContent || len(res.body) != 0 || res.cursorHeader {
		t.Fatalf("retire = %+v, error = %v", res, err)
	}
	stillPinned, err := readCompactionObservation(ctx, raw, server.URL, http.MethodGet, "status", 2, true)
	if err != nil || stillPinned != published {
		t.Fatalf("retired with an admitted reader = %+v, want %+v, error = %v", stillPinned, published, err)
	}
	if err := retirementBarrier(ctx, raw, server.URL, http.MethodPost, "release", 0); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-held:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	drained, err := readCompactionObservation(ctx, raw, server.URL, http.MethodGet, "status", 2, true)
	if err != nil || drained.backfillCompaction != published.backfillCompaction || drained.retiredGenerationPins != 0 {
		t.Fatalf("drained = %+v, error = %v", drained, err)
	}
	for _, name := range retirementPredecessor {
		if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("retired artifact %q still accessible: %v", name, err)
		}
	}
}
