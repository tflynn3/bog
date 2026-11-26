package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"bog/internal/store"
)

type queryTestSource struct {
	page    func(PageRequest, *store.QueryWork) (PageResult, error)
	calls   []PageRequest
	release []string
}

func (s *queryTestSource) Page(_ context.Context, req PageRequest, work *store.QueryWork) (PageResult, error) {
	s.calls = append(s.calls, req)
	return s.page(req, work)
}

func (s *queryTestSource) Release(capture string) error {
	s.release = append(s.release, capture)
	return nil
}

func TestInjectedPageSourceContinuationBudget(t *testing.T) {
	const literal = "hit"
	const capture = "test-capture"
	const firstPosition, secondPosition = "first", "second"
	const firstScan, resumeScan = 7, 19
	row := store.Row{TS: time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC), Line: literal}
	record := jsonRow(t, row)
	source := &queryTestSource{page: func(req PageRequest, work *store.QueryWork) (PageResult, error) {
		if req.Filter.Contains != literal || req.Limit != 3 || !req.Snapshot {
			t.Fatalf("source request lost its filter or limits: %+v", req)
		}
		switch req.After {
		case "":
			if err := work.Charge(firstScan); err != nil {
				return PageResult{}, err
			}
			return PageResult{Capture: capture, Rows: []PageRow{{row, firstPosition}, {row, secondPosition}}}, nil
		case firstPosition:
			if req.Capture != capture {
				t.Fatalf("wrong capture: %q", req.Capture)
			}
			if err := work.Charge(resumeScan); err != nil {
				return PageResult{}, err
			}
			return PageResult{Rows: []PageRow{{row, secondPosition}}}, nil
		default:
			t.Fatalf("scan resumed at an unemitted position: %q", req.After)
			return PageResult{}, nil
		}
	}}
	a := newAPIWithPageSource(source, nil, len(record), DefaultQueryWorkBudget)
	h := a.handler(1)
	query := func(params string) *httptest.ResponseRecorder {
		out := httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequest(http.MethodGet, queryBounds+"&contains="+literal+"&limit=3&"+params, nil))
		return out
	}

	initial := query("page=1")
	token := initial.Header().Get("Bog-Next-Cursor")
	if initial.Code != http.StatusOK || initial.Body.String() != record || token == "" {
		t.Fatalf("first page: status=%d body=%q headers=%v", initial.Code, initial.Body.String(), initial.Header())
	}
	if len(source.calls) != 2 || source.calls[1].After != firstPosition || len(source.release) != 0 {
		t.Fatalf("calls=%+v releases=%v", source.calls, source.release)
	}

	encoding := uint64(len(literal))
	delivery := uint64((len(record) + queryDeliveryBytesPerUnit - 1) / queryDeliveryBytesPerUnit)
	auth := 1 + uint64(len(token)) + uint64(len(literal))
	firstCost := firstScan + encoding + delivery + uint64(len(literal))
	resumeCost := auth + resumeScan + encoding + delivery
	for _, tt := range []struct {
		name   string
		budget uint64
	}{
		{"token validation", firstCost + auth - 1},
		{"seek and scan", firstCost + auth + resumeScan - 1},
		{"line encoding", firstCost + auth + resumeScan + encoding - 1},
		{"body delivery", firstCost + resumeCost - 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source.release = nil
			a.queryWorkBudget = tt.budget
			assertQueryRejection(t, query("page=1"), http.StatusServiceUnavailable, "query work budget exhausted")
			if !slices.Equal(source.release, []string{capture}) {
				t.Fatalf("abandoned capture releases=%v", source.release)
			}
		})
	}

	source.release = nil
	a.queryWorkBudget = firstCost + resumeCost
	initial = query("page=1")
	token = initial.Header().Get("Bog-Next-Cursor")
	if initial.Code != http.StatusOK || initial.Body.String() != record || token == "" {
		t.Fatalf("exact budget: status=%d body=%q headers=%v", initial.Code, initial.Body.String(), initial.Header())
	}
	for range 2 {
		out := query("cursor=" + token)
		if out.Code != http.StatusOK || out.Body.String() != record || out.Header().Get("Bog-Next-Cursor") != "" {
			t.Fatalf("terminal continuation: status=%d body=%q headers=%v", out.Code, out.Body.String(), out.Header())
		}
	}
	if len(source.release) != 0 {
		t.Fatalf("published cursor was released: %v", source.release)
	}
}

func TestQueryCaptureExpiryAndRelease(t *testing.T) {
	source := &queryTestSource{page: func(req PageRequest, _ *store.QueryWork) (PageResult, error) {
		if req.Capture != "" {
			return PageResult{}, ErrCaptureExpired
		}
		return PageResult{Capture: "empty-capture"}, nil
	}}
	a := newAPIWithPageSource(source, nil, DefaultQueryMaxResponseBytes, DefaultQueryWorkBudget)
	h := a.handler(1)
	query := func(params string) *httptest.ResponseRecorder {
		out := httptest.NewRecorder()
		h.ServeHTTP(out, httptest.NewRequest(http.MethodGet, queryBounds+params, nil))
		return out
	}
	out := query("&page=1")
	if out.Code != http.StatusOK || out.Body.Len() != 0 || out.Header().Get("Bog-Next-Cursor") != "" ||
		!slices.Equal(source.release, []string{"empty-capture"}) {
		t.Fatalf("terminal first page: status=%d body=%q releases=%v", out.Code, out.Body.String(), source.release)
	}
	cursor := cursorData{
		Version:  1,
		Start:    time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		End:      time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC),
		Contains: a.containsDigest(""),
		Capture:  "retired-capture",
		After:    "some-position",
	}
	for _, tt := range []struct {
		name           string
		expires        time.Time
		queriesStorage bool
	}{
		{"signed deadline elapsed", time.Now().Add(-time.Minute), false},
		{"source capture unavailable", time.Now().Add(time.Minute), true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source.calls = nil
			source.release = nil
			cursor.ExpiresAt = tt.expires.UnixNano()
			token, err := a.encodeCursor(cursor)
			if err != nil {
				t.Fatal(err)
			}
			assertQueryRejection(t, query("&cursor="+token), http.StatusGone, "query capture expired")
			if (len(source.calls) > 0) != tt.queriesStorage || len(source.release) != 0 {
				t.Fatalf("calls=%+v releases=%v", source.calls, source.release)
			}
			assertQueryRejection(t, query("&cursor="+token+"&contains=different"), http.StatusBadRequest, "invalid cursor")
		})
	}
}

func TestPageSourceFailureBeforeDelivery(t *testing.T) {
	const capture = "capture"
	row := store.Row{TS: time.Date(2025, 1, 1, 0, 0, 1, 0, time.UTC), Line: "hit"}
	for _, tc := range []struct {
		name     string
		firstErr error
		probe    PageResult
		probeErr error
		status   int
		message  string
	}{
		{name: "capture capacity", firstErr: ErrQueryCapacityExhausted, status: 503, message: "query capacity exhausted"},
		{name: "work", probeErr: store.ErrQueryWorkExhausted, status: 503, message: "query work budget exhausted"},
		{name: "cancelled", probeErr: context.Canceled, status: 408, message: "request cancelled"},
		{name: "expired", probeErr: ErrCaptureExpired, status: 410, message: "query capture expired"},
		{name: "storage I/O", probeErr: errors.New("private storage details"), status: 500, message: "query failed"},
		{name: "missing lookahead", status: 500, message: "query failed"},
		{name: "replaced capture", probe: PageResult{Capture: "other", Rows: []PageRow{{row, "second"}}}, status: 500, message: "query failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &queryTestSource{page: func(req PageRequest, _ *store.QueryWork) (PageResult, error) {
				if req.Capture != "" {
					return tc.probe, tc.probeErr
				}
				if tc.firstErr != nil {
					return PageResult{}, fmt.Errorf("acquire capture: %w", tc.firstErr)
				}
				return PageResult{Capture: capture, Rows: []PageRow{{row, "first"}}, More: true}, nil
			}}
			h := NewWithPageSource(source, nil, DefaultQueryMaxResponseBytes, 1, DefaultQueryWorkBudget)
			out := httptest.NewRecorder()
			h.ServeHTTP(out, httptest.NewRequest(http.MethodGet, queryBounds+"&page=1&limit=1", nil))
			assertQueryRejection(t, out, tc.status, tc.message)
			if tc.firstErr != nil {
				if len(source.calls) != 1 || len(source.release) != 0 {
					t.Fatalf("failed acquisition: calls=%v releases=%v", source.calls, source.release)
				}
			} else if len(source.calls) != 2 || !slices.Equal(source.release, []string{capture}) {
				t.Fatalf("failed reservation: calls=%v releases=%v", source.calls, source.release)
			}
		})
	}
}
