package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"bog/internal/store"
	model "bog/verify/query-model"
)

// The test adapter swaps the active store but routes held captures back to
// the store that opened them. The append log supplies expectations separately.
type replacementPageSource struct {
	prior, replacement *trackedPageSource
	activated          atomic.Bool
}

func (s *replacementPageSource) lookup(capture string) (*trackedPageSource, string, string, error) {
	if capture == "" {
		if s.activated.Load() {
			return s.replacement, "replacement", "", nil
		}
		return s.prior, "prior", "", nil
	}
	generation, localID, _ := strings.Cut(capture, "/")
	switch generation {
	case "prior":
		return s.prior, generation, localID, nil
	case "replacement":
		return s.replacement, generation, localID, nil
	default:
		return nil, "", "", ErrCaptureExpired
	}
}

func (s *replacementPageSource) Page(ctx context.Context, req PageRequest, work *store.QueryWork) (PageResult, error) {
	if err := work.Charge(1); err != nil {
		return PageResult{}, err
	}
	source, generation, localID, err := s.lookup(req.Capture)
	if err != nil {
		return PageResult{}, err
	}
	req.Capture = localID
	result, err := source.Page(ctx, req, work)
	if err == nil && result.Capture != "" {
		result.Capture = generation + "/" + result.Capture
	}
	return result, err
}

func (s *replacementPageSource) Release(capture string) error {
	source, _, localID, err := s.lookup(capture)
	if err != nil {
		return err
	}
	return source.Release(localID)
}

func TestRetainedQueryContractAcrossGenerationReplacement(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	filter := model.Filter{Start: start, End: start.Add(24 * time.Hour), Contains: "hit"}
	tie := start.Add(time.Nanosecond)
	large := "hit" + strings.Repeat("<", 9)
	prior := []model.Row{
		{Time: start.Add(70 * time.Nanosecond), Line: "hit last"},
		{Time: tie, Line: "hit z"},
		{Time: tie, Line: "hit z"},
		{Time: tie, Line: large},
		{Time: tie, Line: "hit a"},
		{Time: filter.Start, Line: "HIT"},
		{Time: filter.End, Line: "hit excluded end"},
		{Time: filter.Start.Add(-time.Nanosecond), Line: "hit excluded start"},
	}
	for i := 2; i < 128; i++ {
		prior = append(prior, model.Row{Time: start.Add(time.Duration(i)), Line: "ordinary miss"})
	}
	appended := []model.Row{
		{Time: start, Line: "hit backfill"},
		{Time: tie, Line: "hit new tie"},
		{Time: start.Add(90 * time.Nanosecond), Line: "hit new last"},
	}

	var log model.Log
	oldCutoff := log.Append(prior)
	priorSource := newContractGeneration(prior)
	replacementSource := newContractGeneration(prior)
	publishContractRows(replacementSource.store, appended)
	source := &replacementPageSource{prior: priorSource, replacement: replacementSource}

	const budget = 4096
	shortRecord := jsonRow(t, store.Row{TS: tie, Line: "hit z"})
	byteCap := 2 * len(shortRecord)
	a := NewWithPageSource(source, nil, byteCap, 1, budget)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), admissionWaitTimeout)
		defer cancel()
		if err := a.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	request := func(params string) *httptest.ResponseRecorder {
		return requestCapturePage(a, "&contains=hit"+params)
	}
	first := request("&page=1&limit=4")
	if first.Body.String() != shortRecord+shortRecord {
		t.Fatalf("initial page should stop at the byte cap after both duplicates: %q", first.Body.String())
	}
	freshCutoff := log.Append(appended)
	source.activated.Store(true)

	walk := func(first *httptest.ResponseRecorder, cutoff, initialLimit int, resumeLimits []int) {
		t.Helper()
		matches, err := log.Match(cutoff, filter)
		if err != nil {
			t.Fatal(err)
		}
		out, offset, limit := first, 0, initialLimit
		for page := 0; ; page++ {
			var expected strings.Builder
			n := 0
			for _, entry := range matches[offset:min(offset+limit, len(matches))] {
				record := jsonRow(t, store.Row{TS: entry.Time, Line: entry.Line})
				if expected.Len()+len(record) > byteCap {
					break
				}
				expected.WriteString(record)
				n++
			}
			token := assertRetainedPage(t, out, expected.String())
			if n == 0 {
				t.Fatalf("fixture did not fit a row at offset %d", offset)
			}
			offset += n
			if offset == len(matches) {
				if token != "" {
					t.Fatalf("terminal page at cutoff %d returned a cursor", cutoff)
				}
				return
			}
			if token == "" {
				t.Fatalf("page %d ended at offset %d of %d", page, offset, len(matches))
			}
			limit = resumeLimits[page%len(resumeLimits)]
			params := "&cursor=" + token + "&limit=" + strconv.Itoa(limit)
			out = request(params)
			if retryToken := assertRetainedPage(t, request(params), out.Body.String()); retryToken != out.Header().Get("Bog-Next-Cursor") {
				t.Fatalf("page %d retry changed the next cursor", page+1)
			}
		}
	}
	// Start both walks on the same API with the same fixed work and byte caps.
	fresh := request("&page=1&limit=3")
	walk(first, oldCutoff, 4, []int{1, 3, 2})
	walk(fresh, freshCutoff, 3, []int{1, 4})
	if old, fresh := priorSource.releases.Load(), replacementSource.releases.Load(); old != 0 || fresh != 0 {
		t.Fatalf("terminal pages released live captures: prior=%d replacement=%d", old, fresh)
	}
	if token := assertRetainedPage(t, requestCapturePage(a, "&page=1&contains=absent"), ""); token != "" {
		t.Fatal("empty capture returned a cursor")
	}

	if err := a.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if old, fresh := priorSource.releases.Load(), replacementSource.releases.Load(); old != 1 || fresh != 2 {
		t.Fatalf("release calls after drain: prior=%d replacement=%d", old, fresh)
	}
}

func newContractGeneration(rows []model.Row) *trackedPageSource {
	s := new(store.Store)
	publishContractRows(s, rows)
	return &trackedPageSource{legacyPageSource: legacyPageSource{store: s}}
}

func publishContractRows(s *store.Store, rows []model.Row) {
	batch := make([]store.Row, 0, len(rows))
	for _, row := range rows {
		batch = append(batch, store.Row{TS: row.Time, Line: row.Line})
	}
	s.Publish(batch)
}

func assertRetainedPage(t *testing.T, out *httptest.ResponseRecorder, body string) string {
	t.Helper()
	if out.Code != http.StatusOK || out.Body.String() != body ||
		out.Header().Get("Content-Length") != strconv.Itoa(len(body)) ||
		out.Header().Get("Content-Type") != "application/x-ndjson" {
		t.Fatalf("page: status=%d headers=%v body=%q, want %q", out.Code, out.Header(), out.Body.String(), body)
	}
	return out.Header().Get("Bog-Next-Cursor")
}

func TestRetainedQueryContractCancelledReplayAndExpiryDuringDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &blockingReplaySource{
			trackedPageSource: captureFixtureSource(),
			entered:           make(chan struct{}),
			proceed:           make(chan struct{}),
		}
		source.expiresAt = time.Now().Add(time.Minute)
		a := NewWithPageSource(source, nil, 4096, 2, 1024)
		defer func() {
			if err := a.Close(t.Context()); err != nil {
				t.Error(err)
			}
		}()
		path := "&cursor=" + issueCapture(t, a)
		req := httptest.NewRequest(http.MethodGet, queryBounds+path, nil)
		source.block.Store(true)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		out := httptest.NewRecorder()
		cancelled := make(chan struct{})
		go func() {
			defer close(cancelled)
			a.ServeHTTP(out, req.WithContext(ctx))
		}()
		waitForAdmission(t, source.entered, "retained replay")
		cancel()
		waitForAdmission(t, cancelled, "cancelled replay")
		assertQueryRejection(t, out, http.StatusRequestTimeout, "request cancelled")

		row := store.Row{TS: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), Line: "second"}
		if token := assertRetainedPage(t, requestCapturePage(a, path), jsonRow(t, row)); token != "" {
			t.Fatal("terminal retry returned a cursor")
		}
		if n := source.releases.Load(); n != 0 {
			t.Fatalf("cancelled replay or terminal retry released the capture: %d", n)
		}

		writer := &blockingQueryWriter{httptest.NewRecorder(), make(chan struct{}), make(chan struct{})}
		done := make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(writer.release) }) }
		defer func() {
			unblock()
			waitForAdmission(t, done, "delivery cleanup")
		}()
		go func() {
			defer close(done)
			a.ServeHTTP(writer, req.Clone(t.Context()))
		}()
		waitForAdmission(t, writer.entered, "retained body delivery")
		time.Sleep(time.Minute)
		synctest.Wait()
		assertQueryRejection(t, requestCapturePage(a, path), http.StatusGone, "query capture expired")
		assertQueryRejection(t, requestCapturePage(a, path+"&contains=other"), http.StatusBadRequest, "invalid cursor")
		drain, stop := context.WithTimeout(t.Context(), time.Second)
		defer stop()
		if err := a.Close(drain); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("close while response is blocked: %v", err)
		}
		if n := source.releases.Load(); n != 0 {
			t.Fatalf("capture released before body drain: %d", n)
		}

		unblock()
		waitForAdmission(t, done, "retained body drain")
		if err := a.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		if n := source.releases.Load(); n != 1 {
			t.Fatalf("release calls after drain: %d", n)
		}
		if writer.Code != http.StatusOK || writer.Body.Len() != 0 ||
			writer.Header().Get("Content-Length") != strconv.Itoa(len(jsonRow(t, row))) {
			t.Fatalf("interrupted page framing: status=%d headers=%v bytes=%d", writer.Code, writer.Header(), writer.Body.Len())
		}
	})
}
