package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"bog/internal/store"
	model "bog/verify/query-model"
)

func TestCertifiedQueryWalkAtMinimumBudget(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	sparse := []store.Row{{TS: start, Line: "@tie"}, {TS: start, Line: "@tie"}, {TS: start.Add(3), Line: "@end"}}
	for i := 0; i < 44; i++ {
		sparse = append(sparse, store.Row{TS: start.Add(time.Duration(i + 4)), Line: "miss"})
	}
	sparse = append(sparse, store.Row{TS: start.Add(40), Line: "@late"}, store.Row{TS: start.Add(1), Line: "@middle"})
	crossing := make([]store.Row, maxQueryLimit+12)
	for i := range crossing {
		crossing[i] = store.Row{TS: start.Add(time.Duration(i % 3)), Line: fmt.Sprintf("@%04d", i)}
	}

	for _, tc := range []struct {
		name    string
		rows    []store.Row
		first   int
		limits  []int
		byteCap int
		maxWork uint64
	}{
		{"ties", []store.Row{{TS: start, Line: "@c"}, {TS: start, Line: "@a"}, {TS: start, Line: "@b"}}, 1, []int{1}, 4096, 2048},
		{"sparse and byte trimmed", sparse, 4, []int{1, 2, 5}, 2 * len(jsonRow(t, sparse[0])), 2048},
		{"crosses a certified scan", crossing, 1, []int{997, 5, 1000}, DefaultQueryMaxResponseBytes, DefaultQueryWorkBudget},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := new(store.Store)
			var log model.Log
			for _, row := range tc.rows {
				log.Append([]model.Row{{Time: row.TS, Line: row.Line}})
			}
			s.Publish(tc.rows)
			want, err := log.Match(log.HighWater(), model.Filter{Start: start, End: start.AddDate(0, 0, 1), Contains: "@"})
			if err != nil {
				t.Fatal(err)
			}

			request := func(a *API, params string) *httptest.ResponseRecorder {
				out := httptest.NewRecorder()
				a.ServeHTTP(out, httptest.NewRequest(http.MethodGet, queryBounds+"&contains=@"+params, nil))
				return out
			}
			startPage := "&page=1&limit=" + strconv.Itoa(tc.first)
			low, high := uint64(1), tc.maxWork
			for low < high {
				budget := low + (high-low)/2
				a := NewWithPageSource(&legacyPageSource{store: s}, nil, tc.byteCap, 1, budget)
				first := request(a, startPage)
				if err := a.Close(t.Context()); err != nil {
					t.Fatal(err)
				}
				if first.Code == 200 && first.Header().Get("Bog-Next-Cursor") != "" {
					high = budget
				} else {
					assertQueryRejection(t, first, 503, "query work budget exhausted")
					low = budget + 1
				}
			}
			a := NewWithPageSource(&legacyPageSource{store: s}, nil, tc.byteCap, 1, low-1)
			assertQueryRejection(t, request(a, startPage), 503, "query work budget exhausted")
			if err := a.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			a = NewWithPageSource(&legacyPageSource{store: s}, nil, tc.byteCap, 1, low)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), admissionWaitTimeout)
				defer cancel()
				if err := a.Close(ctx); err != nil {
					t.Error(err)
				}
			})
			out := request(a, startPage)
			s.Publish([]store.Row{{TS: start, Line: "@new tie"}, {TS: start.Add(-1), Line: "@backfill"}})
			seen := make(map[string]bool)
			for page, offset, limit := 0, 0, tc.first; ; page++ {
				var body strings.Builder
				count := 0
				for _, entry := range want[offset:min(offset+limit, len(want))] {
					record := jsonRow(t, store.Row{TS: entry.Time, Line: entry.Line})
					if len(record)+body.Len() > tc.byteCap {
						break
					}
					body.WriteString(record)
					count++
				}
				if count == 0 || out.Code != 200 || out.Body.String() != body.String() ||
					out.Header().Get("Content-Length") != strconv.Itoa(body.Len()) {
					t.Fatalf("budget %d, page %d, offset %d: status=%d headers=%v bytes=%d want %d", low, page, offset, out.Code, out.Header(), out.Body.Len(), body.Len())
				}
				offset += count
				token := out.Header().Get("Bog-Next-Cursor")
				if offset == len(want) {
					if token != "" {
						t.Fatal("terminal page issued a cursor")
					}
					break
				}
				last := want[offset-1]
				cursor, err := a.decodeCursor(token)
				if err != nil || seen[token] || cursor.After != positionDigest(legacyPosition(store.TailPosition{Time: last.Time, Ordinal: uint64(last.Ordinal)})) {
					t.Fatalf("page %d cursor does not identify ordinal %d: %+v, %v", page, last.Ordinal, cursor, err)
				}
				seen[token] = true
				limit = tc.limits[page%len(tc.limits)]
				out = request(a, "&cursor="+token+"&limit="+strconv.Itoa(limit))
			}
		})
	}
}

type shortScanSource struct{ legacyPageSource }

func (s *shortScanSource) Page(ctx context.Context, req PageRequest, work *store.QueryWork) (PageResult, error) {
	if req.Snapshot {
		req.Limit = min(req.Limit, 1)
	}
	return s.legacyPageSource.Page(ctx, req, work)
}

func TestCertifiedQueryFillsPagesAcrossShortSourceScans(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []store.Row{{TS: stamp, Line: "a"}, {TS: stamp, Line: "b"}, {TS: stamp, Line: "c"}, {TS: stamp, Line: "d"}}
	s := new(store.Store)
	s.Publish(rows)
	a := NewWithPageSource(&shortScanSource{legacyPageSource{store: s}}, nil, 4096, 1, DefaultQueryWorkBudget)
	defer func() {
		if err := a.Close(t.Context()); err != nil {
			t.Error(err)
		}
	}()
	first := requestCapturePage(a, "&page=1&limit=2")
	token := first.Header().Get("Bog-Next-Cursor")
	if first.Code != 200 || token == "" || first.Body.String() != jsonRow(t, rows[0])+jsonRow(t, rows[1]) {
		t.Fatalf("first page: status=%d headers=%v body=%s", first.Code, first.Header(), first.Body.String())
	}
	last := requestCapturePage(a, "&limit=3&cursor="+token)
	if last.Code != 200 || last.Header().Get("Bog-Next-Cursor") != "" || last.Body.String() != jsonRow(t, rows[2])+jsonRow(t, rows[3]) {
		t.Fatalf("terminal page: status=%d headers=%v body=%s", last.Code, last.Header(), last.Body.String())
	}
}
