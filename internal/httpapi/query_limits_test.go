package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"bog/internal/store"
)

func TestLiveQueryScanWork(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	row := func(offset int, line string) store.Row {
		return store.Row{TS: stamp.Add(time.Duration(offset) * time.Nanosecond), Line: line}
	}

	direct := new(store.Store)
	direct.Publish([]store.Row{row(0, "skip"), row(1, "miss"), row(2, "test")})
	merged := new(store.Store)
	recovery := store.NewRecoveryBuilder()
	recovery.Add([]store.Row{row(0, "skip"), row(2, "test")})
	recovery.Install(merged)
	merged.Publish([]store.Row{row(1, "miss")})

	bloom := new(store.Store)
	rows := make([]store.Row, 1024)
	for i := range rows {
		rows[i] = row(i, "xxxx")
	}
	bloom.Publish(rows)

	for _, tc := range []struct {
		name    string
		s       *store.Store
		params  string
		minimum uint64
	}{
		{"direct nonmatches", direct, "&contains=x", 1 + 2 + 3*5},
		{"merged nonmatches", merged, "&contains=x", 2 + 3 + 3*5},
		// The first 512 rows are scanned. Four blocks are pruned after
		// the upper bound seek, so the final unit is a Bloom check.
		{"Bloom pruning", bloom, "&limit=1&contains=needle", 1 + 11 + 11 + 512*5 + 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := func(budget uint64) *httptest.ResponseRecorder {
				handler := NewWithQueryLimits(tc.s, nil, DefaultQueryMaxResponseBytes, 1, budget)
				out := httptest.NewRecorder()
				handler.ServeHTTP(out, httptest.NewRequest(http.MethodGet, queryBounds+tc.params, nil))
				return out
			}
			start := time.Now()
			assertQueryRejection(t, query(tc.minimum-1), http.StatusServiceUnavailable, "query work budget exhausted")
			if elapsed := time.Since(start); elapsed >= 2*time.Second {
				t.Errorf("exhausted query took %s", elapsed)
			}
			out := query(tc.minimum)
			if out.Code != http.StatusOK || out.Body.Len() != 0 || out.Header().Get("Bog-Next-Cursor") != "" ||
				out.Header().Get("Content-Type") != "application/x-ndjson" {
				t.Fatalf("successful empty query: status=%d headers=%v body=%q", out.Code, out.Header(), out.Body.String())
			}
		})
	}
}

func TestLiveQuerySmallWorkBudget(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	const budget = 32
	rows := make([]store.Row, 4*budget)
	for i := range rows {
		rows[i] = store.Row{TS: stamp.Add(time.Duration(i+1) * time.Nanosecond), Line: "scan gap"}
	}
	rows[0].Line, rows[1].Line, rows[len(rows)-1].Line = "@a", "@b", "@c"
	s := new(store.Store)
	s.Publish(rows)
	h := NewWithQueryLimits(s, nil, DefaultQueryMaxResponseBytes, 1, budget)
	query := func(bounds, params string) *httptest.ResponseRecorder {
		out := httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequest(http.MethodGet, bounds+"&contains=@"+params, nil))
		return out
	}
	ordinary := query(queryBounds, "&limit=2")
	if ordinary.Code != http.StatusOK || ordinary.Body.String() != jsonRow(t, rows[0])+jsonRow(t, rows[1]) {
		t.Fatalf("ordinary query: status=%d body=%q", ordinary.Code, ordinary.Body.String())
	}
	assertQueryRejection(t, query(queryBounds, "&limit=3"), http.StatusServiceUnavailable, "query work budget exhausted")
	assertQueryRejection(t, query(queryBounds, "&page=1&limit=2"), http.StatusServiceUnavailable, "query work budget exhausted")
	path := "&page=1&limit=1"
	for i, row := range []store.Row{rows[0], rows[1], rows[len(rows)-1]} {
		page := query(queryBounds, path)
		token := assertRetainedPage(t, page, jsonRow(t, row))
		if (token == "") != (i == 2) {
			t.Fatalf("page %d cursor = %q", i+1, token)
		}
		path = "&cursor=" + token + "&limit=1"
	}

	underBudget := NewWithQueryLimits(s, nil, DefaultQueryMaxResponseBytes, 1, 16)
	out := requestCapturePage(underBudget, "&page=1&limit=1&contains=@")
	assertQueryRejection(t, out, http.StatusServiceUnavailable, "query work budget exhausted")

	last := rows[len(rows)-1]
	bounds := "/query?start=" + last.TS.Format(time.RFC3339Nano) + "&end=" + last.TS.Add(time.Nanosecond).Format(time.RFC3339Nano)
	terminal := query(bounds, "&page=1&limit=1")
	if terminal.Code != http.StatusOK || terminal.Body.String() != jsonRow(t, last) || terminal.Header().Get("Bog-Next-Cursor") != "" {
		t.Fatalf("terminal query after budget rejection: status=%d headers=%v body=%q", terminal.Code, terminal.Header(), terminal.Body.String())
	}
}

type publishingAppender struct{ s *store.Store }

func (a publishingAppender) Append(_ []byte, rows []store.Row) error {
	a.s.Publish(rows)
	return nil
}

func TestLiveQueryConcurrentCursorResume(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	early := store.Row{TS: stamp, Line: "first"}
	tie := store.Row{TS: stamp.Add(time.Second), Line: "tie"}
	late := store.Row{TS: stamp.Add(2 * time.Second), Line: "last"}
	firstLine, tieLine, lastLine := jsonRow(t, early), jsonRow(t, tie), jsonRow(t, late)
	s := new(store.Store)
	s.Publish([]store.Row{tie, late, early, tie})
	a := newAPI(s, publishingAppender{s}, 2*len(tieLine), DefaultQueryWorkBudget)
	h := a.handler(2)

	query := func(path string) *httptest.ResponseRecorder {
		out := httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequest(http.MethodGet, queryBounds+path, nil))
		return out
	}
	first := query("&page=1&limit=1")
	token := first.Header().Get("Bog-Next-Cursor")
	if first.Code != http.StatusOK || token == "" || first.Body.String() != firstLine {
		t.Fatalf("first page: status=%d headers=%v body=%q", first.Code, first.Header(), first.Body.String())
	}
	path := "&cursor=" + token

	// Two independent requests with the same cursor can occupy both slots.
	// Keep both writes blocked while contenders and ingest run.
	startHeldQuery := func() func() {
		t.Helper()
		writer := &blockingQueryWriter{httptest.NewRecorder(), make(chan struct{}), make(chan struct{})}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		var once sync.Once
		release := func() {
			once.Do(func() {
				cancel()
				close(writer.release)
			})
			waitForAdmission(t, done, "held continuation cleanup")
		}
		t.Cleanup(release)
		go func() {
			defer close(done)
			h.ServeHTTP(writer, httptest.NewRequest(http.MethodGet, queryBounds+path, nil).WithContext(ctx))
		}()
		waitForAdmission(t, writer.entered, "continuation to enter response write")
		if writer.Header().Get("Content-Type") != "application/x-ndjson" || writer.Header().Get("Bog-Next-Cursor") == "" {
			t.Fatalf("held continuation headers: %v", writer.Header())
		}
		return release
	}
	releaseFirst := startHeldQuery()
	releaseSecond := startHeldQuery()

	const contenders = 16
	results := make(chan *httptest.ResponseRecorder, contenders)
	for range contenders {
		go func() { results <- query(path) }()
	}
	ingest := httptest.NewRecorder()
	h.ServeHTTP(ingest, httptest.NewRequest(http.MethodPost, "/ingest",
		strings.NewReader("{\"ts\":\"2025-01-01T00:00:00Z\",\"line\":\"backfill\"}\n"+tieLine)))
	if ingest.Code != http.StatusNoContent {
		t.Fatalf("ingest while saturated: status=%d body=%q", ingest.Code, ingest.Body.String())
	}
	for range contenders {
		assertQueryRejection(t, waitForAdmission(t, results, "competing query"), http.StatusServiceUnavailable, "query capacity exhausted")
	}
	releaseFirst()

	// The rejected cursor is still valid in the single free slot. Neither
	// the older backfill nor the appended tie enters the captured snapshot.
	second := query(path)
	next := second.Header().Get("Bog-Next-Cursor")
	cursor, err := a.decodeCursor(next)
	if second.Code != http.StatusOK || second.Body.String() != tieLine+tieLine || err != nil || cursor.After != positionDigest(legacyPosition(store.TailPosition{Time: tie.TS, Ordinal: 3})) {
		t.Fatalf("second page: status=%d body=%q cursor=%+v error=%v", second.Code, second.Body.String(), cursor, err)
	}
	last := query("&cursor=" + next)
	if last.Code != http.StatusOK || last.Body.String() != lastLine || last.Header().Get("Bog-Next-Cursor") != "" {
		t.Fatalf("last page: status=%d headers=%v body=%q", last.Code, last.Header(), last.Body.String())
	}
	fresh := query("&limit=1")
	if fresh.Code != http.StatusOK || fresh.Body.String() != "{\"ts\":\"2025-01-01T00:00:00Z\",\"line\":\"backfill\"}\n" {
		t.Fatalf("fresh query: status=%d body=%q", fresh.Code, fresh.Body.String())
	}
	releaseSecond()
}
