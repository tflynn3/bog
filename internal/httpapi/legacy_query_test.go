package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
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
	querymodel "bog/verify/query-model"
)

func TestLegacyCursorIssuingBudgetOracle(t *testing.T) {
	for _, tc := range querymodel.FixedBudgetCases() {
		t.Run(tc.Fixture, func(t *testing.T) {
			t.Parallel()
			s := new(store.Store)
			handler := NewWithQueryLimits(s, publishingAppender{s}, int(tc.ResponseByteCap), 2, uint64(tc.ReferenceWorkBudget))
			acknowledge := func(ctx context.Context, rows []querymodel.Row) error {
				var body bytes.Buffer
				enc := json.NewEncoder(&body)
				for _, row := range rows {
					if err := enc.Encode(store.Row{TS: row.Time, Line: row.Line}); err != nil {
						return err
					}
				}
				out := httptest.NewRecorder()
				handler.ServeHTTP(out, httptest.NewRequestWithContext(ctx, http.MethodPost, "/ingest", &body))
				if out.Code != http.StatusNoContent {
					return fmt.Errorf("ingest returned HTTP %d: %s", out.Code, out.Body)
				}
				return nil
			}
			var openingCursor string
			bodies := make(map[querymodel.WireRequest]string)
			fetch := func(ctx context.Context, req querymodel.WireRequest) (querymodel.WirePage, error) {
				values := url.Values{
					"start":    {req.Filter.Start.Format(time.RFC3339Nano)},
					"end":      {req.Filter.End.Format(time.RFC3339Nano)},
					"contains": {req.Filter.Contains},
					"limit":    {strconv.Itoa(req.Limit)},
				}
				if req.Cursor == "" {
					values.Set("page", "1")
				} else {
					values.Set("cursor", req.Cursor)
				}
				out := httptest.NewRecorder()
				handler.ServeHTTP(out, httptest.NewRequestWithContext(ctx, http.MethodGet, "/query?"+values.Encode(), nil))
				if out.Code != http.StatusOK {
					return querymodel.WirePage{}, fmt.Errorf("query returned HTTP %d: %s", out.Code, out.Body)
				}
				token := out.Header().Get("Bog-Next-Cursor")
				if req.Cursor == "" {
					openingCursor = token
				}
				if out.Header().Get("Content-Length") != strconv.Itoa(out.Body.Len()) ||
					out.Header().Get("Content-Type") != "application/x-ndjson" {
					return querymodel.WirePage{}, fmt.Errorf("invalid page framing: %v", out.Header())
				}
				body := out.Body.String()
				if previous, ok := bodies[req]; ok && body != previous {
					return querymodel.WirePage{}, fmt.Errorf("replay body changed from %q to %q", previous, body)
				}
				bodies[req] = body
				page := querymodel.WirePage{BodyBytes: int64(len(body)), NextCursor: token}
				if body == "" || !strings.HasSuffix(body, "\n") {
					return querymodel.WirePage{}, fmt.Errorf("expected complete NDJSON rows, got %q", body)
				}
				for record := range strings.SplitSeq(strings.TrimSuffix(body, "\n"), "\n") {
					var row store.Row
					if err := json.Unmarshal([]byte(record), &row); err != nil {
						return querymodel.WirePage{}, err
					}
					page.Rows = append(page.Rows, querymodel.Row{Time: row.TS, Line: row.Line})
				}
				return page, nil
			}

			result, err := querymodel.RunFixedBudgetCase(t.Context(), tc, acknowledge, fetch)
			if err != nil || result.Outcome != querymodel.FixedBudgetExhausted || result.Pages != 3 ||
				len(bodies) != 3 || openingCursor == "" {
				t.Fatalf("budget %d: result=%+v replayed requests=%d error=%v", tc.ReferenceWorkBudget, result, len(bodies), err)
			}

			// A larger resume limit also has to fit at budget 11, including
			// both ties after the opening row. The sparse byte cap fits two.
			filter := querymodel.Filter{
				Start: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
			}
			filter.End = filter.Start.Add(10 * time.Minute)
			if tc.Fixture == "sparse" {
				filter.Contains = "@"
			}
			page, err := fetch(t.Context(), querymodel.WireRequest{Filter: filter, Limit: maxQueryLimit, Cursor: openingCursor})
			if err != nil || len(page.Rows) != 2 || page.NextCursor != "" {
				t.Fatalf("larger resume at budget %d: page=%+v error=%v", tc.ReferenceWorkBudget, page, err)
			}
		})
	}
}

func TestLegacyQueryReplayWindow(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 0, 1234, time.UTC)
	rows := []store.Row{
		{TS: stamp, Line: "opening"},
		{TS: stamp, Line: "b"},
		{TS: stamp, Line: "<\u2028\n"},
		{TS: stamp, Line: strings.Repeat("d", 50)},
		{TS: stamp, Line: "e"},
		{TS: stamp, Line: "e"},
	}
	s := new(store.Store)
	s.Publish(rows)
	matches, err := s.CapturePositionedMatches(stamp, stamp.Add(time.Second), "", len(rows), nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, capBytes := range []int{100, 123, 190, 1000} {
		t.Run(strconv.Itoa(capBytes), func(t *testing.T) {
			encoded := make([]string, len(rows))
			for i, row := range rows {
				encoded[i] = jsonRow(t, row)
			}
			const literalWork = 2
			var highest uint64
			type replayCase struct {
				start, limit, next int
				body               string
				work               uint64
			}
			var cases []replayCase
			for start := 1; start < len(rows); start++ {
				for limit := 1; limit <= len(rows)+1; limit++ {
					next := start
					var body string
					work := uint64(literalWork + legacyCursorWork)
					for next < len(rows) && next-start < limit && len(encoded[next]) <= capBytes-len(body) {
						body += encoded[next]
						work += 1 + uint64(len(rows[next].Line))
						next++
					}
					work += (uint64(len(body)) + 15) / 16
					if next < len(rows) {
						work += legacyCursorWork
						if next-start < limit {
							work++ // inspected a record that exceeded the remaining bytes
						}
					}
					cases = append(cases, replayCase{start, limit, next, body, work})
					highest = max(highest, work)
				}
			}
			plan := &legacyQueryPlan{matches: matches, sizes: make([]int, len(rows))}
			pay := func() *store.QueryWork { return store.NewQueryWork(t.Context(), DefaultQueryWorkBudget) }
			if _, err := plan.prepare(1, capBytes, literalWork, highest-1, pay()); err != store.ErrQueryWorkExhausted {
				t.Fatalf("certificate accepted insufficient budget or failed unexpectedly: %v", err)
			}
			response, err := plan.prepare(1, capBytes, literalWork, highest, pay())
			if err != nil || string(bytes.Join(response.rows, nil)) != encoded[0] {
				t.Fatalf("certificate at exact threshold %d: response=%q error=%v", highest, response.rows, err)
			}
			for _, tc := range cases {
				replayWork := tc.work - literalWork - legacyCursorWork
				under := store.NewQueryWork(t.Context(), replayWork-1)
				if _, _, err := plan.replay(tc.start, tc.limit, capBytes, under); err != store.ErrQueryWorkExhausted {
					t.Fatalf("start=%d limit=%d accepted insufficient replay work: %v", tc.start, tc.limit, err)
				}
				work := store.NewQueryWork(t.Context(), replayWork)
				response, next, err := plan.replay(tc.start, tc.limit, capBytes, work)
				if err != nil || next != tc.next || string(bytes.Join(response.rows, nil)) != tc.body {
					t.Fatalf("start=%d limit=%d budget=%d: next=%d body=%q error=%v", tc.start, tc.limit, tc.work, next, response.rows, err)
				}
			}
		})
	}
}

func TestLegacyCursorConcurrentReuseAndCancellation(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	s := new(store.Store)
	rows := []store.Row{{TS: stamp, Line: "c"}, {TS: stamp, Line: "b"}, {TS: stamp, Line: "b"}}
	s.Publish(rows)
	const parallel = 8
	h := NewWithQueryLimits(s, publishingAppender{s}, 96, parallel, 11)
	request := func(ctx context.Context, path string) *httptest.ResponseRecorder {
		out := httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequestWithContext(ctx, http.MethodGet, path, nil))
		return out
	}
	open := request(t.Context(), queryBounds+"&page=1&limit=1")
	token := open.Header().Get("Bog-Next-Cursor")
	if open.Code != http.StatusOK || token == "" || open.Body.String() != jsonRow(t, rows[0]) {
		t.Fatalf("opening: status=%d body=%s headers=%v", open.Code, open.Body, open.Header())
	}
	path := queryBounds + "&cursor=" + token
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cancelled := request(&queryScanContext{Context: ctx, cancel: cancel, cancelAt: 6}, path)
	assertQueryRejection(t, cancelled, http.StatusRequestTimeout, "request cancelled")

	wrongRange := strings.Replace(path, "2025-01-02", "2025-01-03", 1)
	assertQueryRejection(t, request(t.Context(), wrongRange), http.StatusBadRequest, "invalid cursor")
	assertQueryRejection(t, request(t.Context(), path+"&contains=b"), http.StatusBadRequest, "invalid cursor")
	assertQueryRejection(t, request(t.Context(), path+"x"), http.StatusBadRequest, "invalid cursor")
	foreign := NewWithQueryLimits(s, nil, 96, 1, 11)
	out := httptest.NewRecorder()
	foreign.ServeHTTP(out, httptest.NewRequest(http.MethodGet, path, nil))
	assertQueryRejection(t, out, http.StatusBadRequest, "invalid cursor")

	var group sync.WaitGroup
	defer group.Wait()
	begin := make(chan struct{})
	results := make(chan *httptest.ResponseRecorder, parallel)
	equivalent := "/query?start=2024-12-31T19:00:00-05:00&end=2025-01-01T19:00:00-05:00&cursor=" + token
	for i := range parallel {
		group.Go(func() {
			<-begin
			target := path
			if i%2 == 0 {
				target = equivalent
			}
			results <- request(t.Context(), target)
		})
	}
	close(begin)
	for range 20 {
		backfill := httptest.NewRecorder()
		h.ServeHTTP(backfill, httptest.NewRequest(http.MethodPost, "/ingest",
			strings.NewReader("{\"ts\":\"2025-01-01T00:00:00Z\",\"line\":\"backfill\"}\n"+jsonRow(t, rows[1]))))
		if backfill.Code != http.StatusNoContent {
			t.Fatalf("ingest during resume: status=%d body=%q", backfill.Code, backfill.Body)
		}
	}
	want := jsonRow(t, rows[1]) + jsonRow(t, rows[2])
	for range parallel {
		out := waitForAdmission(t, results, "legacy cursor replay")
		if out.Code != http.StatusOK || out.Body.String() != want || out.Header().Get("Bog-Next-Cursor") != "" ||
			out.Header().Get("Content-Length") != strconv.Itoa(len(want)) {
			t.Fatalf("terminal replay: status=%d headers=%v body=%q", out.Code, out.Header(), out.Body)
		}
	}
}

func TestLegacyQueryReleasesFailedCaptures(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	s := new(store.Store)
	s.Publish([]store.Row{{TS: stamp, Line: strings.Repeat("<", 30)}, {TS: stamp, Line: "a"}})
	source := &legacyPageSource{store: s}
	a := newAPIForSource(source, nil, 96, DefaultQueryWorkBudget)
	a.legacy = source
	h := a.handler(1)
	path := queryBounds + "&page=1&limit=1"
	required := len(jsonRow(t, store.Row{TS: stamp, Line: strings.Repeat("<", 30)}))
	want := fmt.Sprintf("query record exceeds response byte limit (cap 96 bytes, requires %d bytes)", required)
	for range maxQueryCaptures + 1 {
		out := httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequest(http.MethodGet, path, nil))
		assertQueryRejection(t, out, http.StatusUnprocessableEntity, want)
	}
	if len(source.views) != 0 || len(a.captures.live) != 0 {
		t.Fatal("failed opening retained a capture")
	}
}

func TestLegacyQueryBoundsLiveCaptures(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	s := new(store.Store)
	s.Publish([]store.Row{{TS: stamp, Line: "a"}, {TS: stamp, Line: "b"}})
	source := &legacyPageSource{store: s}
	a := newAPIForSource(source, nil, 96, 11)
	a.legacy = source
	h := a.handler(1)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), admissionWaitTimeout)
		defer cancel()
		if err := a.Close(ctx); err != nil {
			t.Errorf("close query captures: %v", err)
		}
	})
	request := func(suffix string) *httptest.ResponseRecorder {
		out := httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequest(http.MethodGet, queryBounds+suffix, nil))
		return out
	}
	var firstToken string
	const opening = "&page=1&limit=1"
	for i := range maxQueryCaptures {
		out := request(opening)
		token := out.Header().Get("Bog-Next-Cursor")
		if out.Code != http.StatusOK || token == "" {
			t.Fatalf("capture %d: status=%d body=%q headers=%v", i, out.Code, out.Body, out.Header())
		}
		if i == 0 {
			firstToken = token
		}
	}
	assertQueryRejection(t, request(opening), http.StatusServiceUnavailable, "query capacity exhausted")
	continuation := "&cursor=" + firstToken
	for range 2 {
		if out := request(continuation); out.Code != http.StatusOK || out.Body.String() != jsonRow(t, store.Row{TS: stamp, Line: "b"}) {
			t.Fatalf("terminal replay at capacity: status=%d body=%q", out.Code, out.Body)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), admissionWaitTimeout)
	defer cancel()
	if err := a.Close(ctx); err != nil {
		t.Fatal(err)
	}
	assertQueryRejection(t, request(continuation), http.StatusGone, "query capture expired")
	if len(source.views) != 0 || len(a.captures.live) != 0 {
		t.Fatal("closing left live captures")
	}
}
