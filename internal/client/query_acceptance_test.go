package client

import (
	"context"
	"encoding/base64"
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

type queryReadFunc func([]byte) (int, error)

func (f queryReadFunc) Read(p []byte) (int, error) {
	return f(p)
}

func TestQueryFailureBeforeAcceptance(t *testing.T) {
	closeFailure := errors.New("response close failed")
	// A read error can use the same wording as an overflow diagnostic.
	readFailure := errors.New("response exceeds size limit")
	cancelCause := errors.New("caller stopped accepting pages")
	row := contractRow("complete")
	for _, tc := range []struct {
		name       string
		body       string
		length     int64
		maxBytes   int64
		next       string
		status     int
		chunked    bool
		noRead     bool
		readErr    error
		cancel     bool
		closeErr   error
		wantCauses []error
		wantDetail string
	}{
		{name: "too many rows", body: row + row, length: int64(2 * len(row)), next: fixtureNext,
			wantCauses: []error{ErrMalformedPage}, wantDetail: "exceeds requested limit 1"},
		{name: "declared overflow with close failure", length: int64(len(row)), maxBytes: int64(len(row) - 1), next: fixtureNext,
			noRead: true, closeErr: closeFailure, wantCauses: []error{ErrPageBytes, ErrPageInterrupted, closeFailure}},
		{name: "streamed overflow", body: row, length: -1, chunked: true, maxBytes: int64(len(row) - 1), next: fixtureNext,
			wantCauses: []error{ErrPageBytes}},
		{name: "streamed overflow on interrupted read", body: row, length: -1, chunked: true, maxBytes: int64(len(row) - 1),
			next: fixtureNext, readErr: readFailure, wantCauses: []error{ErrPageBytes, ErrPageInterrupted, readFailure}},
		{name: "truncated body at record boundary", body: row, length: int64(len(row) + 1), next: fixtureNext,
			wantCauses: []error{ErrPageInterrupted, io.ErrUnexpectedEOF}},
		{name: "interrupted read at byte ceiling", body: row, length: -1, chunked: true, maxBytes: int64(len(row)), next: fixtureNext,
			readErr: readFailure, wantCauses: []error{ErrPageInterrupted, readFailure}},
		{name: "length shorter than body", body: row, length: int64(len(row) - 1), next: fixtureNext,
			wantCauses: []error{ErrMalformedPage}},
		{name: "unverifiable framing", body: row, length: -1, next: fixtureNext,
			noRead: true, wantCauses: []error{ErrMalformedPage}, wantDetail: "unverifiable framing"},
		{name: "malformed record", body: row + "{oops}\n", length: int64(len(row) + len("{oops}\n")), next: fixtureNext,
			wantCauses: []error{ErrMalformedPage}, wantDetail: "decode query record"},
		{name: "repeated cursor on empty page", next: fixtureCursor, wantCauses: []error{ErrMalformedPage}, wantDetail: "does not advance"},
		{name: "malformed cursor encoding", body: row, length: int64(len(row)), next: "!",
			wantCauses: []error{ErrMalformedPage, base64.CorruptInputError(0)}, wantDetail: "invalid next cursor header"},
		{name: "close failure after complete body", body: row, length: int64(len(row)), next: fixtureNext,
			closeErr: closeFailure, wantCauses: []error{ErrPageInterrupted, closeFailure}},
		{name: "cancellation after complete body", body: row, length: int64(len(row)), next: fixtureNext,
			cancel: true, wantCauses: []error{context.Canceled, cancelCause}},
		{name: "cancellation with close failure", body: row, length: int64(len(row)), next: fixtureNext,
			cancel: true, closeErr: closeFailure, wantCauses: []error{context.Canceled, cancelCause, ErrPageInterrupted, closeFailure}},
		{name: "read canceled by transport", body: row, length: -1, chunked: true, next: fixtureNext,
			readErr: context.DeadlineExceeded, wantCauses: []error{context.DeadlineExceeded}},
		{name: "capacity rejection", body: "query capacity exhausted\n", length: int64(len("query capacity exhausted\n")),
			status: http.StatusServiceUnavailable, maxBytes: int64(len(row) - 1)},
		{name: "invalid cursor rejection", body: "invalid cursor\n", length: int64(len("invalid cursor\n")), status: http.StatusBadRequest},
		{name: "server response limit rejection", body: "query record exceeds response byte limit\n",
			length: int64(len("query record exceeds response byte limit\n")), status: http.StatusUnprocessableEntity},
		{name: "canceled capacity rejection", length: -1, chunked: true, status: http.StatusServiceUnavailable,
			cancel: true, readErr: context.Canceled, wantCauses: []error{context.Canceled, cancelCause}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, mode := range []string{"FetchPage", "Walk", "WalkFrom", "BoundedWalk"} {
				t.Run(mode, func(t *testing.T) {
					t.Parallel()
					ctx, cancel := context.WithCancelCause(t.Context())
					defer cancel(nil)
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
						body, next := contractRow("first"), fixtureCursor
						failure := false
						status := http.StatusOK
						if cursor == "" {
							if req.URL.Query().Get("page") != "1" {
								t.Errorf("unexpected first request %s", req.URL)
							}
						} else if cursor == fixtureCursor {
							continuationAttempts++
							failure = continuationAttempts == 1
							body, next = contractRow("last"), ""
						} else {
							t.Errorf("unexpected continuation %s", req.URL)
						}
						length := int64(len(body))
						if failure {
							body, next, length = tc.body, tc.next, tc.length
							if tc.status != 0 {
								status = tc.status
							}
						}
						if req.URL.Query().Get("limit") != "1" {
							t.Errorf("unexpected request limit in %s", req.URL)
						}
						var reader io.Reader = strings.NewReader(body)
						if failure && (tc.noRead || tc.readErr != nil) {
							reader = queryReadFunc(func(p []byte) (int, error) {
								if tc.noRead {
									t.Error("read a response that should have been rejected before reading")
									return 0, io.EOF
								}
								if len(p) < len(body) {
									t.Errorf("test reader buffer holds %d of %d bytes", len(p), len(body))
								}
								return copy(p, body), tc.readErr
							})
						}
						response := &http.Response{
							StatusCode: status,
							ProtoMajor: 1, ProtoMinor: 1,
							Header:        make(http.Header),
							ContentLength: length,
							Body: queryResponseBody{reader, func() error {
								closed++
								if failure {
									if tc.cancel {
										cancel(cancelCause)
									}
									return tc.closeErr
								}
								return nil
							}},
						}
						if failure && tc.chunked {
							response.TransferEncoding = []string{"chunked"}
						}
						if next != "" {
							response.Header.Set(nextCursorHeader, next)
						}
						return response, nil
					})
					var delivered []string
					consume := func(p Page) error {
						for _, row := range p.Rows {
							delivered = append(delivered, row.Line)
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
						if err != nil || first.NextCursor != fixtureCursor || len(first.Rows) != 1 || first.Rows[0].Line != "first" {
							t.Fatalf("first page %+v, error %v", first, err)
						}
						page, err = walk.NextPage(ctx)
					}

					var protocol *HTTPError
					wantStatus := tc.status
					if wantStatus == 0 {
						wantStatus = http.StatusOK
					}
					if !errors.As(err, &protocol) || protocol.StatusCode != wantStatus ||
						protocol.Retryable() != (wantStatus == http.StatusServiceUnavailable) ||
						protocol.CursorRejected != (wantStatus == http.StatusBadRequest) {
						t.Fatalf("failure returned %v, want HTTP %d", err, wantStatus)
					}
					if wantStatus != http.StatusOK && protocol.Body != tc.body {
						t.Errorf("diagnostic body %q, want %q", protocol.Body, tc.body)
					}
					if mode == "FetchPage" && wantStatus != http.StatusOK && !tc.cancel {
						if direct, ok := err.(*HTTPError); !ok || direct.Cause != nil {
							t.Errorf("clean refusal returned %v, want unwrapped HTTPError", err)
						}
					}
					for _, cause := range tc.wantCauses {
						if !errors.Is(err, cause) {
							t.Errorf("failure returned %v, missing cause %v", err, cause)
						}
					}
					for _, kind := range []error{ErrPageBytes, ErrMalformedPage, ErrPageInterrupted, context.Canceled, context.DeadlineExceeded} {
						if errors.Is(err, kind) != slices.Contains(tc.wantCauses, kind) {
							t.Errorf("failure classification %v for %v disagrees with %v", kind, err, tc.wantCauses)
						}
					}
					if errors.Is(err, ErrPageBytes) {
						diagnostic, _, _ := strings.Cut(protocol.Cause.Error(), "\n")
						if diagnostic != "read query page: response exceeds size limit" {
							t.Errorf("overflow diagnostic %q", diagnostic)
						}
					}
					if tc.wantDetail != "" && !strings.Contains(err.Error(), tc.wantDetail) {
						t.Errorf("failure returned %v, want detail %q", err, tc.wantDetail)
					}
					if page.Rows != nil || page.NextCursor != "" || cursor != fixtureCursor {
						t.Fatalf("failure returned page %+v and cursor %q", page, cursor)
					}
					var wantDelivered []string
					if mode == "Walk" {
						wantDelivered = []string{"first"}
					}
					if !slices.Equal(delivered, wantDelivered) {
						t.Fatalf("delivered %q before retry, want %q", delivered, wantDelivered)
					}

					// The retry must replay the same cursor.
					switch mode {
					case "FetchPage":
						page, err = c.FetchPage(t.Context(), q, cursor)
					case "Walk", "WalkFrom":
						cursor, err = c.WalkFrom(t.Context(), q, cursor, consume)
						wantDelivered = append(wantDelivered, "last")
						if cursor != "" || !slices.Equal(delivered, wantDelivered) {
							t.Fatalf("retry delivered %q and returned cursor %q", delivered, cursor)
						}
					case "BoundedWalk":
						page, err = walk.NextPage(t.Context())
					}
					if err != nil || page.NextCursor != "" || ((mode == "FetchPage" || mode == "BoundedWalk") &&
						(len(page.Rows) != 1 || page.Rows[0].Line != "last")) {
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

func TestFetchPageCancellationBeforeResponse(t *testing.T) {
	t.Parallel()
	stop := errors.New("stop before headers")
	for _, beforeRequest := range []bool{false, true} {
		ctx, cancel := context.WithCancelCause(t.Context())
		c, err := New("http://bog.test", nil)
		if err != nil {
			cancel(nil)
			t.Fatal(err)
		}
		c.http.Transport = queryRoundTripper(func(req *http.Request) (*http.Response, error) {
			if beforeRequest {
				t.Error("issued a request with an already canceled context")
			}
			cancel(stop)
			return nil, context.Cause(req.Context())
		})
		if beforeRequest {
			cancel(stop)
		}
		page, err := c.FetchPage(ctx, Query{}, "")
		cancel(nil)

		var status *HTTPError
		if page.Rows != nil || page.NextCursor != "" || !errors.Is(err, context.Canceled) || !errors.Is(err, stop) ||
			errors.As(err, &status) || errors.Is(err, ErrPageInterrupted) {
			t.Errorf("beforeRequest=%t: page %+v, error %v", beforeRequest, page, err)
		}
	}
}
