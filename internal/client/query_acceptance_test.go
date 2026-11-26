package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
)

type queryRoundTripper func(*http.Request) (*http.Response, error)

func (f queryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type queryResponseBody struct {
	io.Reader
	closeBody func() error
}

func (b queryResponseBody) Close() error {
	return b.closeBody()
}

func TestQueryFailureBeforeAcceptance(t *testing.T) {
	closeFailure := errors.New("response close failed")
	row := contractRow("complete")
	for _, tc := range []struct {
		name       string
		body       string
		length     int64
		maxBytes   int64
		next       string
		cancel     bool
		closeErr   error
		wantErr    error
		wantDetail string
	}{
		{name: "too many rows", body: row + row, length: int64(2 * len(row)), next: fixtureNext, wantDetail: "exceeds requested limit 1"},
		{name: "body above byte ceiling", body: row, length: int64(len(row)), maxBytes: int64(len(row) - 1), next: fixtureNext, wantDetail: "size limit"},
		{name: "truncated body at record boundary", body: row, length: int64(len(row) + 1), next: fixtureNext, wantErr: io.ErrUnexpectedEOF},
		{name: "malformed record", body: "{oops}\n", length: int64(len("{oops}\n")), next: fixtureNext, wantDetail: "decode query record"},
		{name: "repeated cursor on empty page", next: fixtureCursor, wantDetail: "does not advance"},
		{name: "close failure after complete body", body: row, length: int64(len(row)), next: fixtureNext, closeErr: closeFailure, wantErr: closeFailure},
		{name: "cancellation after complete body", body: row, length: int64(len(row)), next: fixtureNext, cancel: true, wantErr: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, mode := range []string{"FetchPage", "Walk", "WalkFrom", "BoundedWalk"} {
				t.Run(mode, func(t *testing.T) {
					t.Parallel()
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					q := Query{Limit: 1}
					var requested []string
					var continuationAttempts, closed int
					c, err := New("http://bog.test", nil)
					if err != nil {
						t.Fatal(err)
					}
					c.MaxPageBytes = tc.maxBytes
					c.http.Transport = queryRoundTripper(func(req *http.Request) (*http.Response, error) {
						cursor := req.URL.Query().Get("cursor")
						requested = append(requested, cursor)
						if len(requested) > 3 {
							return nil, errors.New("unexpected extra query request")
						}
						body, next, length := "", fixtureCursor, int64(0)
						var closeErr error
						cancelOnClose := false
						if cursor == "" {
							if req.URL.Query().Get("page") != "1" {
								t.Errorf("unexpected first request %s", req.URL)
							}
						} else {
							continuationAttempts++
							if cursor != fixtureCursor {
								t.Errorf("unexpected continuation %s", req.URL)
							}
							if continuationAttempts == 1 {
								body, next, length = tc.body, tc.next, tc.length
								closeErr, cancelOnClose = tc.closeErr, tc.cancel
							} else {
								next = ""
							}
						}
						if req.URL.Query().Get("limit") != "1" {
							t.Errorf("unexpected request limit in %s", req.URL)
						}
						response := &http.Response{
							StatusCode: http.StatusOK,
							ProtoMajor: 1, ProtoMinor: 1,
							Header:        make(http.Header),
							ContentLength: length,
							Body: queryResponseBody{strings.NewReader(body), func() error {
								closed++
								if cancelOnClose {
									cancel()
								}
								return closeErr
							}},
						}
						if next != "" {
							response.Header.Set(nextCursorHeader, next)
						}
						return response, nil
					})
					var delivered []string
					consume := func(p Page) error {
						delivered = append(delivered, p.NextCursor)
						if len(p.Rows) != 0 {
							t.Errorf("consumer received rows %v", p.Rows)
						}
						return nil
					}
					var walk *BoundedWalk
					var page Page
					cursor := fixtureCursor
					switch mode {
					case "FetchPage":
						page, err = c.FetchPage(ctx, q, cursor)
					case "Walk":
						cursor, err = c.Walk(ctx, q, consume)
					case "WalkFrom":
						cursor, err = c.WalkFrom(ctx, q, cursor, consume)
					case "BoundedWalk":
						walk, err = c.NewBoundedWalk(q, 2)
						if err != nil {
							t.Fatal(err)
						}
						var first Page
						first, err = walk.NextPage(ctx)
						if err != nil || first.NextCursor != fixtureCursor || len(first.Rows) != 0 {
							t.Fatalf("first page %+v, error %v", first, err)
						}
						page, err = walk.NextPage(ctx)
					}

					var protocol *HTTPError
					if !errors.As(err, &protocol) || protocol.StatusCode != http.StatusOK ||
						protocol.Retryable() || protocol.CursorRejected {
						t.Fatalf("failure returned %v, want HTTP 200 protocol error", err)
					}
					if tc.wantErr != nil && !errors.Is(err, tc.wantErr) || tc.wantDetail != "" && !strings.Contains(err.Error(), tc.wantDetail) {
						t.Fatalf("failure returned %v, want %v %q", err, tc.wantErr, tc.wantDetail)
					}
					if page.Rows != nil || page.NextCursor != "" || cursor != fixtureCursor {
						t.Fatalf("failure returned page %+v and cursor %q", page, cursor)
					}
					var wantDelivered []string
					if mode == "Walk" {
						wantDelivered = []string{fixtureCursor}
					}
					if !slices.Equal(delivered, wantDelivered) {
						t.Fatalf("delivered %q before retry, want %q", delivered, wantDelivered)
					}

					switch mode {
					case "FetchPage":
						page, err = c.FetchPage(t.Context(), q, cursor)
					case "Walk", "WalkFrom":
						cursor, err = c.WalkFrom(t.Context(), q, cursor, consume)
						wantDelivered = append(wantDelivered, "")
						if cursor != "" || !slices.Equal(delivered, wantDelivered) {
							t.Fatalf("retry delivered %q and returned cursor %q", delivered, cursor)
						}
					case "BoundedWalk":
						page, err = walk.NextPage(t.Context())
					}
					if err != nil || len(page.Rows) != 0 || page.NextCursor != "" {
						t.Fatalf("retry returned page %+v and error %v", page, err)
					}
					if walk != nil {
						if _, err := walk.NextPage(t.Context()); !errors.Is(err, io.EOF) {
							t.Fatalf("after retry got %v, want EOF", err)
						}
					}
					wantRequests := []string{fixtureCursor, fixtureCursor}
					if mode == "Walk" || mode == "BoundedWalk" {
						wantRequests = append([]string{""}, wantRequests...)
					}
					if !slices.Equal(requested, wantRequests) || closed != len(wantRequests) {
						t.Fatalf("requested %q, closed %d responses, want %q", requested, closed, wantRequests)
					}
				})
			}
		})
	}
}
