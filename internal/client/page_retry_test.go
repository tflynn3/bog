package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestBoundedPageRetry(t *testing.T) {
	row := contractRow("visible")
	for _, tc := range []struct {
		name          string
		status        int
		body          string
		length        int64
		maxBytes      int64
		closeErr      error
		retries       int
		recoverAfter  int
		wantAttempts  int
		wantError     error
		wantLastError error
	}{
		{name: "same cursor after unavailable", status: 503, retries: 1, recoverAfter: 1, wantAttempts: 2},
		{name: "same cursor after incomplete delivery", status: 200, body: row, length: int64(len(row) + 1),
			retries: 1, recoverAfter: 1, wantAttempts: 2},
		{name: "same cursor after truncated frame with malformed suffix", status: 200, body: row + "{\n", length: int64(len(row) + 3),
			retries: 1, recoverAfter: 1, wantAttempts: 2},
		{name: "same cursor after failed close", status: 200, body: row, closeErr: io.ErrClosedPipe,
			retries: 1, recoverAfter: 1, wantAttempts: 2},
		{name: "retry exhaustion retains last error", status: 200, body: row, length: int64(len(row) + 1),
			retries: 2, wantAttempts: 3, wantError: ErrPageRetryExhausted, wantLastError: io.ErrUnexpectedEOF},
		{name: "zero retries", status: 503, wantAttempts: 1, wantError: ErrPageRetryExhausted},
		{name: "no retries on malformed delivery", status: 200, body: `{"oops"}` + "\n", closeErr: io.ErrClosedPipe,
			retries: 2, wantAttempts: 1, wantError: ErrMalformedPage, wantLastError: ErrPageInterrupted},
		{name: "no retries when size limit and interrupted", status: 200, body: row, maxBytes: int64(len(row) - 1),
			closeErr: io.ErrClosedPipe, retries: 2, wantAttempts: 1, wantError: ErrPageBytes, wantLastError: ErrPageInterrupted},
		{name: "no retries on expired capture", status: 410, body: "query capture expired\n",
			retries: 2, wantAttempts: 1, wantError: ErrCaptureExpired},
		{name: "no retries on invalid cursor", status: 400, body: "invalid cursor\n",
			retries: 2, wantAttempts: 1, wantError: ErrInvalidCursor},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, err := New("http://bog.test", nil)
			if err != nil {
				t.Fatal(err)
			}
			c.MaxPageBytes = tc.maxBytes
			var targets []string
			var closeCount int
			c.http.Transport = queryRoundTripper(func(r *http.Request) (*http.Response, error) {
				targets = append(targets, r.URL.RawQuery)
				if len(targets) == 1 {
					response := retryResponse(200, "", 0, nil, &closeCount)
					response.Header.Set(nextCursorHeader, fixtureCursor)
					return response, nil
				}
				if tc.recoverAfter > 0 && len(targets)-1 > tc.recoverAfter {
					return retryResponse(200, row, 0, nil, &closeCount), nil
				}
				return retryResponse(tc.status, tc.body, tc.length, tc.closeErr, &closeCount), nil
			})
			walk, err := c.NewBoundedWalk(Query{Limit: 1, Contains: "visible"}, 2)
			if err != nil {
				t.Fatal(err)
			}
			policy := PageRetry{MaxRetries: tc.retries}
			first, err := walk.NextPageWithRetry(t.Context(), policy)
			if err != nil || first.NextCursor != fixtureCursor || len(first.Rows) != 0 {
				t.Fatalf("first page %+v, %v", first, err)
			}
			page, err := walk.NextPageWithRetry(t.Context(), policy)
			if !errors.Is(err, tc.wantError) || (tc.wantLastError != nil && !errors.Is(err, tc.wantLastError)) {
				t.Fatalf("page %+v, error %v, want %v and %v", page, err, tc.wantError, tc.wantLastError)
			}
			if err == nil {
				if len(page.Rows) != 1 || page.Rows[0].Line != "visible" || page.NextCursor != "" {
					t.Fatalf("accepted page %+v", page)
				}
				if _, err := walk.NextPageWithRetry(t.Context(), policy); !errors.Is(err, io.EOF) {
					t.Fatalf("terminal walk returned %v", err)
				}
			} else {
				if page.Rows != nil || page.NextCursor != "" || walk.pages != 1 || walk.cursor != fixtureCursor {
					t.Fatalf("failed attempt exposed %+v or advanced walk %+v", page, walk)
				}
				var response *HTTPError
				if !errors.As(err, &response) || response.StatusCode != tc.status {
					t.Fatalf("error %v loses HTTP %d", err, tc.status)
				}
			}
			if len(targets) != 1+tc.wantAttempts || closeCount != len(targets) {
				t.Fatalf("requests %v, %d response bodies closed, want %d continuation attempts", targets, closeCount, tc.wantAttempts)
			}
			if strings.Contains(targets[0], "cursor=") || !strings.Contains(targets[0], "page=1") ||
				!strings.Contains(targets[1], "cursor="+fixtureCursor) ||
				slices.ContainsFunc(targets[1:], func(target string) bool { return target != targets[1] }) {
				t.Fatalf("unexpected retry targets %v", targets)
			}
		})
	}
}

func TestBoundedPageRetryCancellation(t *testing.T) {
	c, err := New("http://bog.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	fetched := make(chan struct{}, 1)
	attempts, closeCount := 0, 0
	c.http.Transport = queryRoundTripper(func(r *http.Request) (*http.Response, error) {
		attempts++
		if attempts == 1 {
			fetched <- struct{}{}
		}
		return retryResponse(http.StatusServiceUnavailable, "query capacity exhausted\n", 0, nil, &closeCount), nil
	})
	walk, err := c.NewBoundedWalk(Query{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("operator stopped the drill")
	deadline, stopDeadline := context.WithTimeout(t.Context(), 2*time.Second)
	defer stopDeadline()
	ctx, cancel := context.WithCancelCause(deadline)
	defer cancel(nil)
	go func() {
		select {
		case <-fetched:
			cancel(cause)
		case <-ctx.Done():
		}
	}()
	page, err := walk.NextPageWithRetry(ctx, PageRetry{MaxRetries: 3, Delay: time.Hour})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, cause) || page.Rows != nil ||
		page.NextCursor != "" || attempts != 1 || closeCount != 1 {
		t.Fatalf("page %+v, error %v, attempts %d, bodies closed %d", page, err, attempts, closeCount)
	}
}

func TestCaptureExpiryRequiresCanonicalContinuationResponse(t *testing.T) {
	const body = "query capture expired\n"
	for _, tc := range []struct {
		name     string
		cursor   string
		body     string
		status   int
		length   int64
		closeErr error
		want     bool
	}{
		{name: "valid", cursor: fixtureCursor, body: body, status: 410, want: true},
		{name: "bare HTTP 410", cursor: fixtureCursor, body: "gone\n", status: 410},
		{name: "wrong status", cursor: fixtureCursor, body: body, status: 400},
		{name: "first page", body: body, status: 410},
		{name: "close delimited", cursor: fixtureCursor, body: body, status: 410, length: -1},
		{name: "truncated", cursor: fixtureCursor, body: body, status: 410, length: int64(len(body) + 1)},
		{name: "close failure", cursor: fixtureCursor, body: body, status: 410, closeErr: io.ErrClosedPipe},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var closeCount int
			page, err := acceptQueryResponse(t.Context(), retryResponse(tc.status, tc.body, tc.length, tc.closeErr, &closeCount), 0, 0, tc.cursor, nil)
			if errors.Is(err, ErrCaptureExpired) != tc.want || errors.Is(err, ErrInvalidCursor) ||
				err == nil || page.Rows != nil || page.NextCursor != "" || closeCount != 1 {
				t.Fatalf("page %+v, error %v, bodies closed %d, want expiry=%t", page, err, closeCount, tc.want)
			}
		})
	}
}

func retryResponse(status int, body string, length int64, closeErr error, closeCount *int) *http.Response {
	if length == 0 {
		length = int64(len(body))
	}
	return &http.Response{
		StatusCode: status, ProtoMajor: 1, ContentLength: length, Header: make(http.Header),
		Body: queryResponseBody{strings.NewReader(body), func() error {
			(*closeCount)++
			return closeErr
		}},
	}
}
