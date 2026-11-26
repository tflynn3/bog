package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"bog/internal/store"
	querymodel "bog/verify/query-model"
)

func requestPage(h http.Handler, params string) *httptest.ResponseRecorder {
	out := httptest.NewRecorder()
	h.ServeHTTP(out, httptest.NewRequest(http.MethodGet, queryBounds+params, nil))
	return out
}

func TestFixedWorkBudgetWalk(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name  string
		dense int
		gap   int
	}{
		{"ties and byte cuts", 8, 0},
		{"late sparse and terminal scans across storage blocks", queryBlockRows + 4, 640},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := new(store.Store)
			var model querymodel.Log
			var batch []store.Row
			add := func(offset int, line string) {
				stamp := start.Add(time.Duration(offset) * time.Millisecond)
				batch = append(batch, store.Row{TS: stamp, Line: line})
			}
			for i := range tc.dense {
				// Two identical rows and one distinct tied row. The append
				// ordinal breaks ties and the backfill sorts before them all.
				line := "@a"
				if i%3 == 2 {
					line = "@<&"
				}
				add(i/3+2, line)
			}
			s.Publish(batch)
			endDense := tc.dense/3 + 3
			for i := range tc.gap {
				add(endDense+i, strings.Repeat("miss", 12))
			}
			add(endDense+tc.gap, "@z")
			add(1, "@backfill")
			for i := range tc.gap {
				add(endDense+tc.gap+1+i, "terminal gap")
			}
			s.Publish(batch[tc.dense:])
			for _, row := range batch {
				model.Append([]querymodel.Row{{Time: row.TS, Line: row.Line}})
			}
			filter := querymodel.Filter{Start: start, End: start.Add(24 * time.Hour), Contains: "@"}
			want, err := model.Match(model.HighWater(), filter)
			if err != nil {
				t.Fatal(err)
			}
			wantRecords := make([]string, len(want))
			for i, row := range want {
				wantRecords[i] = jsonRow(t, store.Row{TS: row.Time, Line: row.Line})
			}

			capBytes := len(wantRecords[0]) + len(wantRecords[1])
			const firstParams = "&contains=%40&page=1&limit=5"
			find := func(budget uint64, keep bool) (*API, *httptest.ResponseRecorder) {
				a := newAPI(s, nil, capBytes, budget)
				out := requestPage(a.handler(2), firstParams)
				if out.Code == http.StatusServiceUnavailable {
					assertQueryRejection(t, out, http.StatusServiceUnavailable, "query work budget exhausted")
					return a, out
				}
				if out.Code != http.StatusOK || out.Header().Get("Bog-Next-Cursor") == "" {
					t.Fatalf("budget %d: status %d, headers %v, body %q", budget, out.Code, out.Header(), out.Body.String())
				}
				token, err := a.decodeCursor(out.Header().Get("Bog-Next-Cursor"),
					a.filterDigest(PageFilter{filter.Start, filter.End, filter.Contains}))
				if err != nil {
					t.Fatal(err)
				}
				if keep {
					t.Cleanup(func() { a.captures.retire(token.capture) })
				} else {
					a.captures.retire(token.capture)
				}
				return a, out
			}
			lo, hi := uint64(1), uint64(1<<20)
			for lo < hi {
				mid := lo + (hi-lo)/2
				if _, out := find(mid, false); out.Code == http.StatusOK {
					hi = mid
				} else {
					lo = mid + 1
				}
			}
			if _, out := find(lo-1, false); out.Code != http.StatusServiceUnavailable {
				t.Fatalf("predecessor budget %d also issued a cursor", lo-1)
			}
			a, out := find(lo, true)
			if out.Code != http.StatusOK {
				t.Fatalf("no successful first page at budget %d", lo)
			}

			h := a.handler(2)
			s.Publish([]store.Row{{TS: start, Line: "@late append"}, {TS: batch[0].TS, Line: "@late tie"}})
			seen := make(map[string]bool)
			for page, offset, limit := 1, 0, 5; ; page++ {
				token := out.Header().Get("Bog-Next-Cursor")
				var expected strings.Builder
				for i := range limit {
					if offset+i == len(wantRecords) || expected.Len()+len(wantRecords[offset+i]) > capBytes {
						break
					}
					expected.WriteString(wantRecords[offset+i])
				}
				if out.Code != http.StatusOK || out.Body.String() != expected.String() ||
					out.Header().Get("Content-Length") != strconv.Itoa(expected.Len()) {
					t.Fatalf("budget %d page %d (offset %d limit %d): status %d length %s body %q, want %q",
						lo, page, offset, limit, out.Code, out.Header().Get("Content-Length"), out.Body.String(), expected.String())
				}
				for consumed := expected.Len(); consumed > 0; offset++ {
					consumed -= len(wantRecords[offset])
				}
				if offset == len(wantRecords) {
					if token != "" {
						t.Fatalf("budget %d page %d: terminal page has a cursor", lo, page)
					}
					break
				}
				if token == "" || seen[token] || expected.Len() == 0 {
					t.Fatalf("budget %d page %d: cursor did not advance", lo, page)
				}
				seen[token] = true
				limit = []int{1, 3, 2}[page%3]
				out = requestPage(h, "&contains=%40&limit="+strconv.Itoa(limit)+"&cursor="+token)
			}
		})
	}
}

type trackedPageSource struct {
	PageSource
	resumes      atomic.Int32
	releases     atomic.Int32
	deadline     time.Time
	beforeResume func(context.Context, int32) error
}

func (s *trackedPageSource) Page(ctx context.Context, req PageRequest, work *store.QueryWork) (PageResult, error) {
	if req.Capture != "" && s.beforeResume != nil {
		if err := s.beforeResume(ctx, s.resumes.Add(1)); err != nil {
			return PageResult{}, err
		}
	}
	page, err := s.PageSource.Page(ctx, req, work)
	if req.Snapshot && req.Capture == "" {
		page.ExpiresAt = s.deadline
	}
	return page, err
}

func (s *trackedPageSource) Release(id string) error {
	s.releases.Add(1)
	return s.PageSource.Release(id)
}

func TestCaptureExpiryWhileAnAdmittedResumeDrains(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := new(store.Store)
		stamp := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		last := store.Row{TS: stamp, Line: "last"}
		s.Publish([]store.Row{{TS: stamp, Line: "first"}, last})
		entered, drain := make(chan struct{}), make(chan struct{})
		source := &trackedPageSource{
			PageSource: newMemoryPageSource(s),
			deadline:   time.Now().Add(time.Second),
			beforeResume: func(ctx context.Context, attempt int32) error {
				if attempt == 2 { // The first pinned scan certifies the suffix.
					close(entered)
					select {
					case <-drain:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			},
		}
		h := NewWithPageSource(source, nil, DefaultQueryMaxResponseBytes, 3, DefaultQueryWorkBudget)
		first := requestPage(h, "&page=1&limit=1")
		token := first.Header().Get("Bog-Next-Cursor")
		if first.Code != http.StatusOK || token == "" {
			t.Fatalf("first: %d %s", first.Code, first.Body.String())
		}
		path := "&cursor=" + token
		done := make(chan *httptest.ResponseRecorder, 1)
		released := false
		defer func() {
			if !released {
				close(drain)
			}
			synctest.Wait()
		}()
		go func() { done <- requestPage(h, path) }()
		waitForAdmission(t, entered, "admitted continuation")
		time.Sleep(time.Second)
		synctest.Wait()

		assertQueryRejection(t, requestPage(h, path), http.StatusGone, "query capture expired")
		if source.releases.Load() != 0 || source.resumes.Load() != 2 {
			t.Fatalf("expiry during resume: releases %d, source scans %d", source.releases.Load(), source.resumes.Load())
		}

		close(drain)
		released = true
		out := waitForAdmission(t, done, "draining continuation")
		if out.Code != http.StatusOK || out.Body.String() != jsonRow(t, last) ||
			out.Header().Get("Bog-Next-Cursor") != "" || source.releases.Load() != 1 {
			t.Fatalf("after draining: status %d body %q releases %d", out.Code, out.Body.String(), source.releases.Load())
		}
		assertQueryRejection(t, requestPage(h, path), http.StatusGone, "query capture expired")
	})
}

func TestCaptureReleasedOnFailedCertification(t *testing.T) {
	for _, tc := range []struct {
		err     error
		status  int
		message string
	}{
		{store.ErrQueryWorkExhausted, http.StatusServiceUnavailable, "query work budget exhausted"},
		{ErrQueryCapacityExhausted, http.StatusServiceUnavailable, "query capacity exhausted"},
		{context.Canceled, http.StatusRequestTimeout, "request cancelled"},
		{ErrCaptureExpired, http.StatusGone, "query capture expired"},
		{errors.New("storage failure"), http.StatusInternalServerError, "query failed"},
	} {
		t.Run(tc.message, func(t *testing.T) {
			s := new(store.Store)
			stamp := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
			row := store.Row{TS: stamp, Line: "row"}
			s.Publish([]store.Row{row, row})
			source := &trackedPageSource{
				PageSource:   newMemoryPageSource(s),
				beforeResume: func(context.Context, int32) error { return tc.err },
			}
			h := NewWithPageSource(source, nil, DefaultQueryMaxResponseBytes, 1, DefaultQueryWorkBudget)
			for range 2 {
				assertQueryRejection(t, requestPage(h, "&page=1&limit=1"), tc.status, tc.message)
			}
			if n := source.releases.Load(); n != 2 {
				t.Fatalf("released %d failed captures, want 2", n)
			}
		})
	}
}

func TestSourceExpiryRetiresPublishedCapture(t *testing.T) {
	s := new(store.Store)
	stamp := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	row := store.Row{TS: stamp, Line: "row"}
	s.Publish([]store.Row{row, row})
	source := &trackedPageSource{
		PageSource: newMemoryPageSource(s),
		beforeResume: func(_ context.Context, attempt int32) error {
			if attempt == 2 {
				return fmt.Errorf("retained generation unavailable: %w", ErrCaptureExpired)
			}
			return nil
		},
	}
	h := NewWithPageSource(source, nil, DefaultQueryMaxResponseBytes, 1, DefaultQueryWorkBudget)
	first := requestPage(h, "&page=1&limit=1")
	token := first.Header().Get("Bog-Next-Cursor")
	if first.Code != http.StatusOK || token == "" {
		t.Fatalf("first: %d %s", first.Code, first.Body.String())
	}
	for range 2 {
		assertQueryRejection(t, requestPage(h, "&cursor="+token), http.StatusGone, "query capture expired")
	}
	if source.releases.Load() != 1 || source.resumes.Load() != 2 {
		t.Fatalf("expired source: releases %d, scans %d", source.releases.Load(), source.resumes.Load())
	}
}

func TestTerminalFirstPageReleasesCapture(t *testing.T) {
	s := new(store.Store)
	stamp := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	row := store.Row{TS: stamp, Line: "row"}
	s.Publish([]store.Row{row})
	source := &trackedPageSource{PageSource: newMemoryPageSource(s)}
	h := NewWithPageSource(source, nil, DefaultQueryMaxResponseBytes, 1, DefaultQueryWorkBudget)
	for i := 1; i <= maxQueryCaptures+1; i++ {
		out := requestPage(h, "&page=1&limit=1")
		if out.Code != http.StatusOK || out.Body.String() != jsonRow(t, row) ||
			out.Header().Get("Bog-Next-Cursor") != "" || source.releases.Load() != int32(i) {
			t.Fatalf("request %d: status %d body %q releases %d", i, out.Code, out.Body.String(), source.releases.Load())
		}
	}
}

func TestReleaseBudgetFixtureCanIssueCursorBelowVerifierCeiling(t *testing.T) {
	// The release verifier tries at most 2,048 budgets for a three-row walk.
	s := new(store.Store)
	stamp := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []store.Row{{TS: stamp, Line: "c"}, {TS: stamp, Line: "a"}, {TS: stamp, Line: "b"}}
	s.Publish(rows)
	a := newAPI(s, nil, DefaultQueryMaxResponseBytes, 2048)
	h := a.handler(1)
	out := requestPage(h, "&page=1&limit=1")
	for i, row := range rows {
		next := out.Header().Get("Bog-Next-Cursor")
		if out.Code != http.StatusOK || out.Body.String() != jsonRow(t, row) || (next != "") != (i < len(rows)-1) {
			t.Fatalf("row %d: status %d body %q cursor %t", i, out.Code, out.Body.String(), next != "")
		}
		if i == 0 {
			cursor, err := decodeTestCursor(a, next)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { a.captures.retire(cursor.capture) })
		}
		if next != "" {
			out = requestPage(h, "&limit=1&cursor="+next)
		}
	}
}
