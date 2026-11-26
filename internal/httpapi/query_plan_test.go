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
		{"ties under a small fixed budget", []store.Row{{TS: start, Line: "@c"}, {TS: start, Line: "@a"}, {TS: start, Line: "@b"}}, 1, []int{1}, 4096, 64},
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
				path := "&cursor=" + token + "&limit=" + strconv.Itoa(limit)
				out = request(a, path)
				retry := request(a, path)
				if retry.Code != out.Code || retry.Body.String() != out.Body.String() ||
					retry.Header().Get("Bog-Next-Cursor") != out.Header().Get("Bog-Next-Cursor") {
					t.Fatalf("retry changed the page after %d emitted rows", offset)
				}
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

func TestQueryCertificationBoundsPlanMetadata(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := make([]store.Row, maxCertifiedScans+2)
	for i := range rows {
		rows[i] = store.Row{TS: stamp, Line: "match"}
	}
	s := new(store.Store)
	s.Publish(rows)
	source := &shortScanSource{legacyPageSource{store: s}}
	a := NewWithPageSource(source, nil, 4096, 1, DefaultQueryWorkBudget)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), admissionWaitTimeout)
		defer cancel()
		if err := a.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	assertQueryRejection(t, requestCapturePage(a, "&page=1&limit=1"), 503, "query work budget exhausted")
	if len(source.views) != 0 {
		t.Fatal("certification failure retained the unpublished source view")
	}
	out := requestCapturePage(a, "&page=1&limit=2")
	if out.Code != 200 || out.Header().Get("Bog-Next-Cursor") == "" {
		t.Fatalf("exact plan bound failed: status=%d body=%q", out.Code, out.Body.String())
	}
}

// Use the release verifier's fixtures and the reported failing budgets. The
// one-row acquisition and shared certification scan keep the sparse walk
// within its historical budget even when every continuation drops to limit 1.
func TestFixedBudgetReleaseFixtures(t *testing.T) {
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(second int, line string) store.Row {
		return store.Row{TS: start.Add(time.Duration(second) * time.Second), Line: line}
	}
	tied := []store.Row{at(0, "c"), at(0, "a"), at(0, "b")}
	sparse := []store.Row{at(0, "@last"), at(0, "@tied"), at(3, "@next")}
	for second := 4; second <= 30; second++ {
		sparse = append(sparse, at(second, "miss"))
	}
	late := at(40, "@late")
	sparse = append(sparse, late)
	for second := 41; second <= 55; second++ {
		sparse = append(sparse, at(second, "miss"))
	}
	const bounds = "/query?start=2020-01-01T00:00:00Z&end=2020-01-01T00:10:00Z"

	for _, fixture := range []struct {
		name, literal   string
		seed, want      []store.Row
		firstLimit, cut int
		byteCap         int
		minimum         uint64
		historical      []uint64
	}{
		{"tied", "", tied, tied, 1, 1, DefaultQueryMaxResponseBytes, 11, nil},
		{"sparse", "@", sparse, []store.Row{sparse[0], sparse[1], sparse[2], late}, 4, 2, 96, 312, []uint64{314, 512}},
	} {
		for _, mode := range []string{"legacy", "retained seam"} {
			t.Run(fixture.name+"/"+mode, func(t *testing.T) {
				newHandler := func(s *store.Store, budget uint64) http.Handler {
					t.Helper()
					if mode == "legacy" {
						return NewWithQueryLimits(s, nil, fixture.byteCap, 1, budget)
					}
					a := NewWithPageSource(&legacyPageSource{store: s}, nil, fixture.byteCap, 1, budget)
					t.Cleanup(func() {
						ctx, cancel := context.WithTimeout(context.Background(), admissionWaitTimeout)
						defer cancel()
						if err := a.Close(ctx); err != nil {
							t.Error(err)
						}
					})
					return a
				}
				for _, budget := range append([]uint64{fixture.minimum - 1, fixture.minimum}, fixture.historical...) {
					t.Run(strconv.FormatUint(budget, 10), func(t *testing.T) {
						s := new(store.Store)
						s.Publish(fixture.seed)
						h := newHandler(s, budget)
						request := func(params string) *httptest.ResponseRecorder {
							out := httptest.NewRecorder()
							h.ServeHTTP(out, httptest.NewRequest(http.MethodGet, bounds+"&contains="+fixture.literal+params, nil))
							return out
						}
						out := request("&page=1&limit=" + strconv.Itoa(fixture.firstLimit))
						if budget < fixture.minimum {
							assertQueryRejection(t, out, 503, "query work budget exhausted")
							return
						}
						// Grow beyond the compact reservation's eligibility while
						// retaining the original capture and work accounting.
						appended := []store.Row{at(-1, "@backfill"), fixture.want[0], at(70, "@append")}
						for range compactSnapshotRows {
							appended = append(appended, at(0, "@new tie"))
						}
						s.Publish(appended)
						var first strings.Builder
						for _, row := range fixture.want[:fixture.cut] {
							first.WriteString(jsonRow(t, row))
						}
						token := assertRetainedPage(t, out, first.String())
						if fixture.name == "tied" {
							if token == "" {
								t.Fatal("tied first page omitted its cursor")
							}
							var suffix strings.Builder
							for _, row := range fixture.want[fixture.cut:] {
								suffix.WriteString(jsonRow(t, row))
							}
							if next := assertRetainedPage(t, request("&cursor="+token+"&limit=1000"), suffix.String()); next != "" {
								t.Fatal("larger continuation limit did not exhaust the tied capture")
							}
						}
						for i, row := range fixture.want[fixture.cut:] {
							if token == "" {
								t.Fatalf("walk ended before row %d", fixture.cut+i)
							}
							path := "&cursor=" + token + "&limit=1"
							want := jsonRow(t, row)
							next := assertRetainedPage(t, request(path), want)
							if retry := assertRetainedPage(t, request(path), want); retry != next || next == token {
								t.Fatalf("row %d cursor did not advance deterministically", fixture.cut+i)
							}
							token = next
						}
						if token != "" {
							t.Fatal("terminal page carried a cursor")
						}
					})
				}
			})
		}
	}
}
