package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"bog/internal/client"
	"bog/internal/store"
)

func TestShippedQueryFixedBudgetFixtures(t *testing.T) {
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	row := func(second int, line string) store.Row {
		return store.Row{TS: start.Add(time.Duration(second) * time.Second), Line: line}
	}
	ties := []store.Row{row(0, "c"), row(0, "a"), row(0, "b")}
	sparseMatches := []store.Row{row(0, "@last"), row(0, "@tied"), row(3, "@next"), row(40, "@late")}
	sparse := append([]store.Row(nil), sparseMatches[:3]...)
	for second := 4; second <= 30; second++ {
		sparse = append(sparse, row(second, "miss"))
	}
	sparse = append(sparse, sparseMatches[3])
	for second := 41; second <= 55; second++ {
		sparse = append(sparse, row(second, "miss"))
	}

	for _, tc := range []struct {
		name                 string
		seed, matches        []store.Row
		budget               uint64
		firstLimit, firstLen int
		filter               string
		recovered            bool
	}{
		{"live ties", ties, ties, 11, 1, 1, "", false},
		{"recovered ties", ties, ties, 11, 1, 1, "", true},
		{"sparse byte trim", sparse, sparseMatches, 314, 4, 2, "@", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const byteCap = 96
			ctx, cancel := context.WithTimeout(t.Context(), admissionWaitTimeout)
			defer cancel()
			s := new(store.Store)
			handler := NewWithQueryLimits(s, publishingAppender{s}, byteCap, 2, tc.budget)
			a := handler.(*API)
			defer func() {
				drain, stop := context.WithTimeout(context.Background(), admissionWaitTimeout)
				defer stop()
				if err := a.Close(drain); err != nil {
					t.Error(err)
				}
			}()
			server := httptest.NewServer(handler)
			defer server.Close()
			transport := &http.Transport{}
			defer transport.CloseIdleConnections()
			c, err := client.New(server.URL, transport)
			if err != nil {
				t.Fatal(err)
			}
			ingest := func(rows []store.Row) {
				t.Helper()
				var body strings.Builder
				for _, r := range rows {
					body.WriteString(jsonRow(t, r))
				}
				if outcome, err := c.Ingest(ctx, []byte(body.String())); err != nil || outcome != client.IngestCommitted {
					t.Fatalf("ingest outcome %v, error %v", outcome, err)
				}
			}
			if tc.recovered {
				recovery := store.NewRecoveryBuilder()
				recovery.Add(tc.seed)
				recovery.Install(s)
			} else {
				ingest(tc.seed)
			}

			q := client.Query{Start: start, End: start.Add(10 * time.Minute), Contains: tc.filter, Limit: tc.firstLimit}
			first, err := c.FetchPage(ctx, q, "")
			if err != nil || first.NextCursor == "" || !slices.Equal(first.Rows, tc.matches[:tc.firstLen]) {
				t.Fatalf("no exact first page at budget %d: %+v, %v", tc.budget, first, err)
			}

			// Change both the store's index layout and its live ordering while
			// the cursor holds the original source view.
			ingest([]store.Row{row(0, "@backfill tie")})
			q.Limit = 1
			cursor := first.NextCursor
			for i := tc.firstLen; i < len(tc.matches); i++ {
				if i == tc.firstLen+1 {
					ingest([]store.Row{row(1, "@backfill between matches")})
				}
				page, err := c.FetchPage(ctx, q, cursor)
				terminal := i == len(tc.matches)-1
				if err != nil || !slices.Equal(page.Rows, tc.matches[i:i+1]) ||
					(page.NextCursor == "") != terminal || page.NextCursor == cursor {
					t.Fatalf("budget %d, row %d: %+v, %v", tc.budget, i, page, err)
				}
				retry, err := c.FetchPage(ctx, q, cursor)
				if err != nil || !slices.Equal(retry.Rows, page.Rows) || retry.NextCursor != page.NextCursor {
					t.Fatalf("budget %d, retry row %d: %+v, %v", tc.budget, i, retry, err)
				}
				cursor = page.NextCursor
			}
			if err := a.RetireCursor(first.NextCursor); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestShippedQueryBudgetRejectionIsFramedBeforeCursor(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	s := new(store.Store)
	s.Publish([]store.Row{{TS: stamp, Line: "c"}, {TS: stamp, Line: "a"}, {TS: stamp, Line: "b"}})
	h := NewWithQueryLimits(s, nil, 96, 1, 10)
	out := requestCapturePage(h, "&page=1&limit=1")
	const message = "query work budget exhausted"
	assertQueryRejection(t, out, http.StatusServiceUnavailable, message)
	if length := out.Header().Get("Content-Length"); length != strconv.Itoa(len(message)+1) {
		t.Fatalf("rejection Content-Length = %q", length)
	}
}

func TestRetireCursorPreservesAdmittedReplay(t *testing.T) {
	source := &blockingReplaySource{
		trackedPageSource: captureFixtureSource(),
		entered:           make(chan struct{}),
		proceed:           make(chan struct{}),
	}
	a := NewWithPageSource(source, nil, 96, 2, DefaultQueryWorkBudget)
	token := issueCapture(t, a)
	foreignAPI := NewWithPageSource(captureFixtureSource(), nil, 96, 1, DefaultQueryWorkBudget)
	foreign := issueCapture(t, foreignAPI)
	if err := foreignAPI.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{"", token[:len(token)-1], foreign} {
		if err := a.RetireCursor(invalid); !errors.Is(err, store.ErrInvalidQueryCursor) {
			t.Fatalf("invalid retirement returned %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), admissionWaitTimeout)
	defer cancel()
	path := "&cursor=" + token + "&limit=1"
	source.block.Store(true)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		out := httptest.NewRecorder()
		a.ServeHTTP(out, httptest.NewRequest(http.MethodGet, queryBounds+path, nil).WithContext(ctx))
		done <- out
	}()
	waitForAdmission(t, source.entered, "replay before retirement")

	for range 2 {
		if err := a.RetireCursor(token); err != nil {
			t.Fatal(err)
		}
	}
	assertQueryRejection(t, requestCapturePage(a, path), http.StatusGone, "query capture expired")
	if n := source.releases.Load(); n != 0 {
		t.Fatalf("released %d captures before the admitted replay drained", n)
	}
	close(source.proceed)
	reply := waitForAdmission(t, done, "retired replay completion")
	second := store.Row{TS: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), Line: "second"}
	if next := assertRetainedPage(t, reply, jsonRow(t, second)); next != "" {
		t.Fatal("terminal page returned a cursor")
	}
	if err := a.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.RetireCursor(token); err != nil {
		t.Fatal(err)
	}
	if n := source.releases.Load(); n != 1 {
		t.Fatalf("retirement released %d captures", n)
	}
}
