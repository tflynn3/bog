package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"bog/internal/store"
)

func TestShippedQueryRejectsBeforeIssuingCursorAcrossUnaffordableGap(t *testing.T) {
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

	// The first two matches fit an ordinary query. A cursor commits to the
	// entire captured walk, including the gap before the third match.
	ordinary := requestCapturePage(h, "&contains=@&limit=2")
	if token := assertRetainedPage(t, ordinary, jsonRow(t, rows[0])+jsonRow(t, rows[1])); token != "" {
		t.Fatal("ordinary query issued a cursor")
	}
	assertQueryRejection(t, requestCapturePage(h, "&contains=@&page=1&limit=1"),
		http.StatusServiceUnavailable, "query work budget exhausted")
}

func TestLegacyReplayAcrossCertifiedScans(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	rows := make([]store.Row, legacyFirstScanLimit+4)
	encoded := make([]string, len(rows))
	for i := range rows {
		rows[i] = store.Row{TS: stamp, Line: fmt.Sprintf("row %04d", i)}
		encoded[i] = jsonRow(t, rows[i])
	}
	s := new(store.Store)
	s.Publish(rows)
	capBytes := (legacyFirstScanLimit - 1) * len(encoded[0])
	open := fmt.Sprintf("&page=1&limit=%d", legacyFirstScanLimit)
	newHandler := func(budget uint64) *API {
		a := newAPIForSource(&legacyPageSource{store: s}, nil, capBytes, budget)
		a.legacyQueryStore = s
		a.routes = a.handler(4)
		return a
	}
	closeHandler := func(a *API) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), admissionWaitTimeout)
		defer cancel()
		if err := a.Close(ctx); err != nil {
			t.Error(err)
		}
	}

	// Find the exact issuing budget once. Every branch below must work
	// under that same allowance, including the largest supported limit.
	low, high := uint64(1), uint64(16384)
	for low < high {
		budget := low + (high-low)/2
		h := newHandler(budget)
		out := requestCapturePage(h, open)
		closeHandler(h)
		if out.Code == http.StatusOK {
			high = budget
		} else {
			assertQueryRejection(t, out, http.StatusServiceUnavailable, "query work budget exhausted")
			low = budget + 1
		}
	}

	below := newHandler(low - 1)
	defer closeHandler(below)
	for range maxQueryCaptures + 1 {
		// Failed certifications release their capture reservations.
		assertQueryRejection(t, requestCapturePage(below, open), http.StatusServiceUnavailable, "query work budget exhausted")
	}

	h := newHandler(low)
	t.Cleanup(func() { closeHandler(h) })
	offset := legacyFirstScanLimit - 1
	token := assertRetainedPage(t, requestCapturePage(h, open), strings.Join(encoded[:offset], ""))
	if token == "" {
		t.Fatal("trimmed opening page did not issue a cursor")
	}
	s.Publish([]store.Row{{TS: stamp.Add(-time.Nanosecond), Line: "backfill"}, {TS: stamp, Line: "new tie"}})

	for _, limit := range []int{1, 1, 2, maxQueryLimit} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			t.Parallel()
			request := func(token string, limit int) *httptest.ResponseRecorder {
				return requestCapturePage(h, fmt.Sprintf("&cursor=%s&limit=%d", token, limit))
			}
			end := min(offset+limit, len(rows))
			next := assertRetainedPage(t, request(token, limit), strings.Join(encoded[offset:end], ""))
			if end == len(rows) {
				if next != "" {
					t.Fatal("terminal replay issued a cursor")
				}
				return
			}
			if next == "" || next == token {
				t.Fatal("replay did not advance")
			}
			if retry := assertRetainedPage(t, request(token, limit), strings.Join(encoded[offset:end], "")); retry != next {
				t.Fatal("retry changed the continuation")
			}
			if terminal := assertRetainedPage(t, request(next, maxQueryLimit), strings.Join(encoded[end:], "")); terminal != "" {
				t.Fatal("terminal replay issued a cursor")
			}
		})
	}
}

func TestShippedQueryCursorExpiry(t *testing.T) {
	for _, tc := range []struct {
		name, line string
		budget     uint64
	}{
		{"inline", "c", 11},
		{"scan replay", "longer tied record", 4096},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
				row := store.Row{TS: stamp, Line: tc.line}
				s := new(store.Store)
				s.Publish([]store.Row{row, row, row})
				h := NewWithQueryLimits(s, nil, 96, 1, tc.budget)
				token := assertRetainedPage(t, requestCapturePage(h, "&page=1&limit=1"), jsonRow(t, row))
				if token == "" {
					t.Fatal("opening page did not issue a cursor")
				}
				path := "&cursor=" + token + "&limit=1"
				time.Sleep(queryCaptureLifetime - time.Nanosecond)
				next := assertRetainedPage(t, requestCapturePage(h, path), jsonRow(t, row))
				if next == "" || next == token {
					t.Fatal("live replay did not advance")
				}
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				for _, cursor := range []string{token, next} {
					assertQueryRejection(t, requestCapturePage(h, "&cursor="+cursor), http.StatusGone, "query capture expired")
					assertQueryRejection(t, requestCapturePage(h, "&cursor="+cursor+"&contains=other"), http.StatusBadRequest, "invalid cursor")
				}
			})
		})
	}
}

func TestShippedQueryRejectsOversizedRecordAfterOpeningScan(t *testing.T) {
	stamp := time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC)
	rows := make([]store.Row, legacyFirstScanLimit+2)
	for i := range rows {
		rows[i] = store.Row{TS: stamp, Line: "match"}
	}
	rows[len(rows)-1].Line += strings.Repeat("<", 30)
	capBytes := 96
	required := len(jsonRow(t, rows[len(rows)-1]))
	s := new(store.Store)
	s.Publish(rows)
	h := NewWithQueryLimits(s, nil, capBytes, 1, DefaultQueryWorkBudget)
	message := fmt.Sprintf("query record exceeds response byte limit (cap %d bytes, requires %d bytes)", capBytes, required)
	assertQueryRejection(t, requestCapturePage(h, "&page=1&limit=1"), http.StatusUnprocessableEntity, message)
}
