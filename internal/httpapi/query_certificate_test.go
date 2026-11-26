package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"bog/internal/store"
	model "bog/verify/query-model"
)

func queryForTest(h http.Handler, params string) *httptest.ResponseRecorder {
	out := httptest.NewRecorder()
	h.ServeHTTP(out, httptest.NewRequest(http.MethodGet, queryBounds+params, nil))
	return out
}

func filterForPath(t *testing.T, path string) PageFilter {
	t.Helper()
	u, err := url.Parse(path)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	start, startErr := store.ParseTime(q.Get("start"))
	end, endErr := store.ParseTime(q.Get("end"))
	if startErr != nil || endErr != nil {
		t.Fatalf("query bounds: %v %v", startErr, endErr)
	}
	return PageFilter{Start: start, End: end, Contains: q.Get("contains")}
}

func boundsForDay(t *testing.T) PageFilter {
	t.Helper()
	return filterForPath(t, queryBounds)
}

func decodeTestCursor(a *API, token string, filter PageFilter) (store.TailPosition, error) {
	cursor, err := a.decodeCursor(token, a.filterDigest(filter))
	if err != nil {
		return store.TailPosition{}, err
	}
	return decodeLegacyPosition(cursor.after)
}

func TestQueryWalkAtLowestIssuingBudget(t *testing.T) {
	stamp := boundsForDay(t).Start.Add(time.Second)
	tied := []store.Row{{TS: stamp, Line: "@c"}, {TS: stamp, Line: "@a"},
		{TS: stamp, Line: "@a"}, {TS: stamp, Line: "@b"}}
	sparse := append([]store.Row(nil), tied...)
	for i := 1; i <= 60; i++ {
		line := "miss"
		if i == 26 || i == 45 {
			line = "@<&"
		}
		sparse = append(sparse, store.Row{TS: stamp.Add(time.Duration(i) * time.Second), Line: line})
	}
	// Include backfill so timestamp order differs from append order.
	sparse = append(sparse, store.Row{TS: stamp.Add(500 * time.Millisecond), Line: "@early"})
	for _, tc := range []struct {
		name                            string
		rows                            []store.Row
		capBytes, firstLimit, firstRows int
		limits                          []int
	}{
		{"tied", tied, DefaultQueryMaxResponseBytes, 1, 1, []int{1}},
		{"sparse with byte trimming and smaller limits", sparse, 96, 6, 2, []int{1, 2, 1, 6}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filter := boundsForDay(t)
			filter.Contains = "@"
			s := new(store.Store)
			var oracle model.Log
			for _, row := range tc.rows {
				s.Publish([]store.Row{row})
				oracle.Append([]model.Row{{Time: row.TS, Line: row.Line}})
			}
			want, err := oracle.Match(oracle.HighWater(), model.Filter{Start: filter.Start, End: filter.End, Contains: filter.Contains})
			if err != nil {
				t.Fatal(err)
			}
			firstParams := fmt.Sprintf("&contains=@&page=1&limit=%d", tc.firstLimit)
			closeAPI := func(a *API) {
				t.Helper()
				if err := a.Shutdown(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			var api *API
			var first *httptest.ResponseRecorder
			for budget := uint64(1); budget <= 2048; budget++ {
				a := NewWithPageSource(newLegacyPageSource(s), nil, tc.capBytes, 2, budget)
				out := queryForTest(a, firstParams)
				if out.Code == http.StatusOK {
					api, first = a, out
					t.Logf("lowest issuing budget %d", budget)
					break
				}
				assertQueryRejection(t, out, http.StatusServiceUnavailable, "query work budget exhausted")
				legacy := a.source.(*legacyPageSource)
				if len(legacy.captures) != 0 || a.captures.count != 0 {
					t.Fatalf("budget %d leaked a capture", budget)
				}
				closeAPI(a)
			}
			if first == nil {
				t.Fatal("no issuing budget found")
			}
			defer closeAPI(api)

			var appends sync.WaitGroup
			appends.Go(func() {
				for range 125 {
					s.Publish([]store.Row{{TS: stamp, Line: "@new tie"}, {TS: filter.Start, Line: "@new backfill"}})
				}
			})
			defer appends.Wait()
			seen := make(map[string]bool)
			for page, offset, out := 0, 0, first; ; page++ {
				token := out.Header().Get("Bog-Next-Cursor")
				lineCount := strings.Count(out.Body.String(), "\n")
				limit := tc.firstLimit
				if page > 0 {
					limit = tc.limits[(page-1)%len(tc.limits)]
				}
				if out.Code != http.StatusOK || out.Header().Get("Content-Type") != "application/x-ndjson" ||
					out.Header().Get("Content-Length") != strconv.Itoa(out.Body.Len()) || out.Body.Len() > tc.capBytes ||
					lineCount == 0 || lineCount > limit || (page == 0 && lineCount != tc.firstRows) || offset+lineCount > len(want) {
					t.Fatalf("page %d at %d: status=%d headers=%v body=%q", page+1, offset, out.Code, out.Header(), out.Body.String())
				}
				var expected strings.Builder
				for _, row := range want[offset : offset+lineCount] {
					expected.WriteString(jsonRow(t, store.Row{TS: row.Time, Line: row.Line}))
				}
				if out.Body.String() != expected.String() {
					t.Fatalf("page %d: got %q, want %q", page+1, out.Body.String(), expected.String())
				}
				offset += lineCount
				if offset == len(want) {
					if token != "" {
						t.Fatal("terminal page has a cursor")
					}
					break
				}
				position, err := decodeTestCursor(api, token, filter)
				last := want[offset-1]
				if err != nil || seen[token] || position.Ordinal != uint64(last.Ordinal) || !position.Time.Equal(last.Time) {
					t.Fatalf("page %d: invalid continuation %v (%v), want %+v", page+1, position, err, last)
				}
				seen[token] = true
				out = queryForTest(api, fmt.Sprintf("&contains=@&cursor=%s&limit=%d", token, tc.limits[page%len(tc.limits)]))
			}
		})
	}
}

func TestQueryContinuationCrossesCertifiedWindows(t *testing.T) {
	filter := boundsForDay(t)
	stamp := filter.Start.Add(time.Second)
	s := new(store.Store)
	rows := make([]store.Row, 12)
	for i := range rows {
		rows[i] = store.Row{TS: stamp, Line: fmt.Sprintf("row %d", i)}
	}
	s.Publish(rows)
	source := &observedPageSource{
		PageSource: newLegacyPageSource(s),
		pageError: func(req PageRequest) error {
			// This source's larger request plans cannot serve a captured
			// continuation within the configured work budget.
			if req.Capture != "" && req.Limit > 3 {
				return store.ErrQueryWorkExhausted
			}
			return nil
		},
	}
	a := NewWithPageSource(source, nil, DefaultQueryMaxResponseBytes, 1, 512)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), admissionWaitTimeout)
		defer cancel()
		if err := a.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	params := "&page=1"
	for offset, i := 0, 0; offset < len(rows); i++ {
		limit := [...]int{1, 1, 5, 5}[i]
		out := queryForTest(a, fmt.Sprintf("%s&limit=%d", params, limit))
		count := min(len(rows)-offset, limit)
		var want strings.Builder
		for _, row := range rows[offset : offset+count] {
			want.WriteString(jsonRow(t, row))
		}
		offset += count
		token := out.Header().Get("Bog-Next-Cursor")
		if out.Code != http.StatusOK || out.Body.String() != want.String() || (token != "") != (offset < len(rows)) {
			t.Fatalf("page %d: status=%d headers=%v body=%q", i+1, out.Code, out.Header(), out.Body.String())
		}
		params = "&cursor=" + token
	}
}
