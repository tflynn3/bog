package client

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"bog/internal/store"
)

// Query specifies the immutable bounds and filter of a snapshot walk.
type Query struct {
	Start, End time.Time
	Contains   string

	// A positive Limit is a per-page row ceiling. Zero uses the server default.
	Limit int
}
type Row = store.Row

// Page contains a fully validated page and its opaque continuation token.
type Page struct {
	Rows       []Row
	NextCursor string
}

const nextCursorHeader = "Bog-Next-Cursor"

// ErrPageBytes means an HTTP 200 query response exceeded the configured
// encoded page byte ceiling.
var ErrPageBytes = errors.New("query response exceeds page byte ceiling")

// pageSizeError preserves the query diagnostic while exposing ErrPageBytes.
type pageSizeError struct{}

func (pageSizeError) Error() string { return "response exceeds size limit" }
func (pageSizeError) Unwrap() error { return ErrPageBytes }

// ErrMalformedPage means an HTTP 200 response violated the query page protocol.
var ErrMalformedPage = errors.New("malformed query page")

// ErrPageInterrupted means delivery of an HTTP 200 page failed while reading
// or closing its body. Context cancellation uses the standard context errors.
var ErrPageInterrupted = errors.New("query page delivery interrupted")

// ErrCaptureExpired indicates that Bog authenticated the cursor but its
// capture is expired or unavailable.
var ErrCaptureExpired = errors.New("query capture expired")

// ErrInvalidCursor indicates that Bog rejected a continuation cursor.
var ErrInvalidCursor = errors.New("invalid cursor")

var errCursorCycle = fmt.Errorf("%w: next cursor does not advance pagination: cursor cycle", ErrMalformedPage)

func interruptedPage(ctx context.Context, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Cause(ctx)) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrPageInterrupted, err)
}

// withQueryCancellation retains an independent failure and exposes the
// context error even if the transport reported only a custom cancel cause.
func withQueryCancellation(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		cause := context.Cause(ctx)
		if !errors.Is(ctxErr, cause) && !errors.Is(err, cause) {
			err = joinQueryErrors(err, cause)
		}
		if !errors.Is(err, ctxErr) {
			err = joinQueryErrors(err, ctxErr)
		}
	}
	return err
}

func joinQueryErrors(err, additional error) error {
	if err == nil {
		return additional
	}
	return errors.Join(err, additional)
}

// FetchPage requests a first page when cursor is empty, or a continuation
// otherwise. It returns the zero Page if the response fails validation or the
// context is canceled before acceptance.
func (c *Client) FetchPage(ctx context.Context, q Query, cursor string) (Page, error) {
	return c.fetchPage(ctx, q, cursor, nil)
}

func queryValues(q Query) url.Values {
	v := make(url.Values)
	v.Set("start", q.Start.UTC().Format(time.RFC3339Nano))
	v.Set("end", q.End.UTC().Format(time.RFC3339Nano))
	v.Set("contains", q.Contains)
	if q.Limit != 0 {
		v.Set("limit", strconv.Itoa(q.Limit))
	}
	return v
}

// When opening is supplied, capture the exact accepted NDJSON payload.
func (c *Client) fetchPage(ctx context.Context, q Query, cursor string, opening *[]byte) (Page, error) {
	if err := withQueryCancellation(ctx, nil); err != nil {
		return Page{}, err
	}
	req, err := c.request(ctx, http.MethodGet, "/query", nil)
	if err != nil {
		return Page{}, err
	}
	v := queryValues(q)
	if cursor == "" {
		v.Set("page", "1")
	} else {
		v.Set("cursor", cursor)
	}
	req.URL.RawQuery = v.Encode()
	resp, err := c.http.Do(req)
	if err != nil {
		return Page{}, withQueryCancellation(ctx, err)
	}
	return acceptQueryResponse(ctx, resp, c.MaxPageBytes, q.Limit, cursor, opening)
}

func acceptQueryResponse(ctx context.Context, resp *http.Response, maxBytes int64, maxRows int, cursor string, opening *[]byte) (Page, error) {
	// Acceptance happens only after the entire response has been read, validated
	// and closed. Keep the page local until then.
	if resp.StatusCode != http.StatusOK {
		e := statusError(resp)
		e.QueryRetryable = resp.StatusCode == http.StatusServiceUnavailable
		closeErr := resp.Body.Close()
		if cursor != "" && framed(resp) && e.Cause == nil && closeErr == nil &&
			(resp.ContentLength < 0 || resp.ContentLength == int64(len(e.Body))) {
			e.CursorRejected = resp.StatusCode == http.StatusBadRequest && e.Body == "invalid cursor\n"
			e.captureExpired = resp.StatusCode == http.StatusGone && e.Body == "query capture expired\n"
		}
		if closeErr != nil {
			return Page{}, withQueryCancellation(ctx, errors.Join(e, closeErr))
		}
		return Page{}, withQueryCancellation(ctx, e)
	}
	if maxBytes == 0 {
		maxBytes = DefaultMaxPage
	}

	page, err := decodeQueryPage(ctx, resp, maxBytes, maxRows, cursor, opening)
	if closeErr := resp.Body.Close(); closeErr != nil {
		err = joinQueryErrors(err, fmt.Errorf("close query page: %w", interruptedPage(ctx, closeErr)))
	}
	err = withQueryCancellation(ctx, err)
	if err != nil {
		return Page{}, &HTTPError{StatusCode: resp.StatusCode, Cause: err}
	}
	return page, nil
}

func readNextCursor(resp *http.Response, cursor string) (string, error) {
	if _, present := resp.Trailer[nextCursorHeader]; present {
		return "", errors.New("unexpected next cursor trailer")
	}
	// For fixed-length HTTP/1 responses net/http leaves the declaration in Header.
	for _, declaration := range resp.Header.Values("Trailer") {
		for name := range strings.SplitSeq(declaration, ",") {
			if strings.EqualFold(strings.TrimSpace(name), nextCursorHeader) {
				return "", errors.New("unexpected next cursor trailer")
			}
		}
	}
	values := resp.Header.Values(nextCursorHeader)
	if len(values) == 0 {
		return "", nil
	}
	next := values[0]
	if len(values) != 1 {
		return "", errors.New("invalid next cursor header")
	}
	if err := validateCursor(next); err != nil {
		return "", err
	}
	if next == cursor {
		return "", errors.New("next cursor does not advance pagination")
	}
	return next, nil
}

// Validate only the transport representation. The issuing server owns the
// payload, authenticity, and lifetime of the opaque token.
func validateCursor(next string) error {
	const maxCursorBytes = 2048
	if len(next) > base64.RawURLEncoding.EncodedLen(maxCursorBytes) {
		return errors.New("invalid next cursor header")
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(next)
	if err != nil {
		return fmt.Errorf("invalid next cursor header: %w", err)
	}
	if len(decoded) <= sha256.Size || len(decoded) > maxCursorBytes || base64.RawURLEncoding.EncodeToString(decoded) != next {
		return errors.New("invalid next cursor header")
	}
	return nil
}

// Walk delivers validated pages in order. It advances only after consume succeeds;
// the returned cursor identifies the page to retry if a request or consume fails.
func (c *Client) Walk(ctx context.Context, q Query, consume func(Page) error) (string, error) {
	return c.WalkFrom(ctx, q, "", consume)
}

// WalkFrom resumes a walk at cursor. It rejects a page before delivery if its
// next cursor repeats any cursor used in this invocation.
func (c *Client) WalkFrom(ctx context.Context, q Query, cursor string, consume func(Page) error) (string, error) {
	if consume == nil {
		return cursor, errors.New("nil page consumer")
	}
	seen := make(map[string]struct{})
	for {
		if cursor != "" {
			seen[cursor] = struct{}{}
		}
		p, err := c.FetchPage(ctx, q, cursor)
		if err != nil {
			return cursor, err
		}
		if _, repeated := seen[p.NextCursor]; repeated {
			return cursor, &HTTPError{
				StatusCode: http.StatusOK,
				Cause:      errCursorCycle,
			}
		}
		if err := withQueryCancellation(ctx, nil); err != nil {
			return cursor, err
		}
		if err = consume(p); err != nil {
			return cursor, err
		}
		if p.NextCursor == "" {
			return "", nil
		}
		cursor = p.NextCursor
	}
}
