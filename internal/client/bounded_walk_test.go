package client

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBoundedWalk(t *testing.T) {
	third := base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("c", 33)))
	for _, tc := range []struct {
		name   string
		tokens []string
		budget int
		calls  int
		pages  int
		err    error
		detail string
		empty  bool
	}{
		{name: "empty terminal", tokens: []string{""}, budget: 1, calls: 1, pages: 1, err: io.EOF, empty: true},
		{name: "empty continuation", tokens: []string{fixtureCursor, ""}, budget: 2, calls: 2, pages: 2, err: io.EOF, empty: true},
		{name: "terminal at budget", tokens: []string{fixtureCursor, ""}, budget: 2, calls: 2, pages: 2, err: io.EOF},
		{name: "budget with continuation", tokens: []string{fixtureCursor, fixtureNext, ""}, budget: 2, calls: 2, pages: 2, err: ErrPageBudget},
		{name: "same cursor", tokens: []string{fixtureCursor, fixtureCursor}, budget: 3, calls: 2, pages: 1, detail: "does not advance"},
		{name: "delayed cursor cycle", tokens: []string{third, fixtureCursor, fixtureNext, fixtureCursor, fixtureNext}, budget: 6, calls: 5, pages: 4, detail: "cursor cycle"},
		{name: "cursor cycle", tokens: []string{fixtureCursor, fixtureNext, fixtureCursor}, budget: 4, calls: 3, pages: 2, detail: "cursor cycle"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				i := int(requests.Add(1)) - 1
				if i >= len(tc.tokens) {
					t.Errorf("unexpected request %d", i+1)
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				if i == 0 {
					if got := r.URL.Query().Get("page"); got != "1" {
						t.Errorf("initial page = %q", got)
					}
				} else if got := r.URL.Query().Get("cursor"); got != tc.tokens[i-1] {
					t.Errorf("request cursor = %q, want %q", got, tc.tokens[i-1])
				}
				body := contractRow("row")
				if tc.empty {
					body = ""
				}
				writeContractPage(t, w, body, tc.tokens[i])
			}))
			defer s.Close()
			c, err := New(s.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.CloseIdleConnections()
			walk, err := c.NewBoundedWalk(Query{Limit: 1}, tc.budget)
			if err != nil {
				t.Fatal(err)
			}
			for i := range tc.pages {
				page, err := walk.NextPage(t.Context())
				if err != nil || page.NextCursor != tc.tokens[i] {
					t.Fatalf("page %d: %+v, %v", i+1, page, err)
				}
			}
			page, err := walk.NextPage(t.Context())
			if len(page.Rows) != 0 || page.NextCursor != "" {
				t.Fatalf("failed/exhausted call delivered %+v", page)
			}
			if tc.detail != "" {
				var protocol *HTTPError
				if !errors.As(err, &protocol) || !strings.Contains(err.Error(), tc.detail) {
					t.Fatalf("got %v, want HTTP protocol error containing %q", err, tc.detail)
				}
			} else if !errors.Is(err, tc.err) {
				t.Fatalf("got %v, want %v", err, tc.err)
			}
			if got := requests.Load(); int(got) != tc.calls {
				t.Fatalf("made %d requests, want %d", got, tc.calls)
			}
		})
	}
}

func TestConcurrentBoundedWalks(t *testing.T) {
	var firstRequests atomic.Int32
	bothStarted := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch q.Get("cursor") {
		case "":
			if firstRequests.Add(1) == 2 {
				close(bothStarted)
			}
			select {
			case <-bothStarted:
			case <-r.Context().Done():
				return
			}
			writeContractPage(t, w, "", fixtureCursor)
		case fixtureCursor:
			writeContractPage(t, w, contractRow(q.Get("contains")), fixtureNext)
		case fixtureNext:
			next := ""
			if q.Get("contains") == "cycle" {
				next = fixtureCursor
			}
			writeContractPage(t, w, contractRow(q.Get("contains")), next)
		default:
			t.Errorf("unexpected request %s", r.URL)
			http.Error(w, "unexpected cursor", http.StatusBadRequest)
		}
	}))
	t.Cleanup(s.Close)
	c, err := New(s.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseIdleConnections)

	var walks sync.WaitGroup
	for _, filter := range []string{"cycle", "terminal"} {
		walks.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			walk, err := c.NewBoundedWalk(Query{Contains: filter, Limit: 1}, 3)
			if err != nil {
				t.Errorf("%s walk: %v", filter, err)
				return
			}
			if p, err := walk.NextPage(ctx); err != nil || len(p.Rows) != 0 || p.NextCursor != fixtureCursor {
				t.Errorf("%s walk first page %+v, error %v", filter, p, err)
				return
			}
			if p, err := walk.NextPage(ctx); err != nil || len(p.Rows) != 1 || p.Rows[0].Line != filter || p.NextCursor != fixtureNext {
				t.Errorf("%s walk second page %+v, error %v", filter, p, err)
				return
			}
			p, err := walk.NextPage(ctx)
			if filter == "cycle" {
				if !errors.Is(err, errCursorCycle) || len(p.Rows) != 0 || p.NextCursor != "" {
					t.Errorf("cycled page %+v, error %v", p, err)
				}
				return
			}
			if err != nil || len(p.Rows) != 1 || p.Rows[0].Line != filter || p.NextCursor != "" {
				t.Errorf("terminal page %+v, error %v", p, err)
				return
			}
			if _, err := walk.NextPage(ctx); !errors.Is(err, io.EOF) {
				t.Errorf("after terminal page got %v, want EOF", err)
			}
		})
	}
	walks.Wait()
}

func TestBoundedWalkRetryAfterFailure(t *testing.T) {
	for _, tc := range []struct{ name, body, detail string }{
		{"malformed NDJSON", contractRow("valid prefix") + "{oops}\n", "decode query record"},
		{"over row limit", contractRow("one") + contractRow("two"), "exceeds requested limit 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := requests.Add(1)
				if n == 1 {
					writeContractPage(t, w, contractRow("first"), fixtureCursor)
					return
				}
				if got := r.URL.Query().Get("cursor"); got != fixtureCursor {
					t.Errorf("resume cursor = %q, want %q", got, fixtureCursor)
				}
				if n == 2 {
					writeContractPage(t, w, tc.body, "")
					return
				}
				writeContractPage(t, w, contractRow("last"), "")
			}))
			defer s.Close()
			c, err := New(s.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.CloseIdleConnections()
			if _, err := c.NewBoundedWalk(Query{}, 0); err == nil {
				t.Fatal("accepted zero budget")
			}
			walk, err := c.NewBoundedWalk(Query{Limit: 1}, 2)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := walk.NextPage(t.Context()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := walk.NextPage(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled walk returned %v", err)
			}
			if p, err := walk.NextPage(t.Context()); err == nil || len(p.Rows) != 0 || !strings.Contains(err.Error(), tc.detail) {
				t.Fatalf("invalid continuation returned %+v, %v", p, err)
			}
			if p, err := walk.NextPage(t.Context()); err != nil || len(p.Rows) != 1 || p.Rows[0].Line != "last" || p.NextCursor != "" {
				t.Fatalf("retry returned %+v, %v", p, err)
			}
			if got := requests.Load(); got != 3 {
				t.Fatalf("made %d requests, want 3", got)
			}
		})
	}
}
