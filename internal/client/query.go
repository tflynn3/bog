package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

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

var errCursorCycle = errors.New("next cursor does not advance pagination: cursor cycle")

func readQueryBody(r *http.Response, limit int64) ([]byte, error) {
	if !framed(r) {
		return nil, errors.New("query response has unverifiable framing")
	}
	encodings := r.Header.Values("Content-Encoding")
	if r.Uncompressed || len(encodings) > 1 || (len(encodings) == 1 && !strings.EqualFold(encodings[0], "identity")) {
		return nil, errors.New("unsupported query content encoding")
	}
	if r.ContentLength >= 0 && r.ContentLength > limit && limit >= 0 {
		return nil, errors.New("response exceeds size limit")
	}
	body, err := readBounded(r.Body, limit)
	if err != nil {
		return nil, err
	}
	if r.ContentLength >= 0 && int64(len(body)) != r.ContentLength {
		return nil, io.ErrUnexpectedEOF
	}
	return body, nil
}

// FetchPage requests a first page when cursor is empty, or a continuation
// otherwise. It returns the zero Page if the response fails validation or the
// context is canceled before acceptance.
func (c *Client) FetchPage(ctx context.Context, q Query, cursor string) (Page, error) {
	if err := ctx.Err(); err != nil {
		return Page{}, err
	}
	req, err := c.request(ctx, http.MethodGet, "/query", nil)
	if err != nil {
		return Page{}, err
	}
	v := req.URL.Query()
	v.Set("start", q.Start.UTC().Format(time.RFC3339Nano))
	v.Set("end", q.End.UTC().Format(time.RFC3339Nano))
	v.Set("contains", q.Contains)
	if q.Limit != 0 {
		v.Set("limit", strconv.Itoa(q.Limit))
	}
	if cursor == "" {
		v.Set("page", "1")
	} else {
		v.Set("cursor", cursor)
	}
	req.URL.RawQuery = v.Encode()
	resp, err := c.http.Do(req)
	if err != nil {
		return Page{}, err
	}
	// Acceptance happens only after the entire response has been read, validated
	// and closed. Keep the page local until then.
	if resp.StatusCode != http.StatusOK {
		e := statusError(resp)
		e.QueryRetryable = resp.StatusCode == http.StatusServiceUnavailable
		e.CursorRejected = cursor != "" && resp.StatusCode == http.StatusBadRequest && framed(resp) && e.Cause == nil && e.Body == "invalid cursor\n"
		if err := resp.Body.Close(); err != nil {
			return Page{}, errors.Join(e, err)
		}
		return Page{}, e
	}
	limit := c.MaxPageBytes
	if limit == 0 {
		limit = DefaultMaxPage
	}

	page, err := decodeQueryPage(ctx, resp, limit, q.Limit, cursor)
	if closeErr := resp.Body.Close(); closeErr != nil {
		err = errors.Join(err, closeErr)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return Page{}, &HTTPError{StatusCode: resp.StatusCode, Cause: err}
	}
	return page, nil
}

func decodeQueryPage(ctx context.Context, resp *http.Response, maxBytes int64, maxRows int, cursor string) (Page, error) {
	body, err := readQueryBody(resp, maxBytes)
	if err != nil {
		return Page{}, fmt.Errorf("read query page: %w", err)
	}
	rows, err := decodeQueryRows(ctx, body, maxRows)
	if err != nil {
		return Page{}, err
	}
	next, err := readNextCursor(resp, cursor)
	if err != nil {
		return Page{}, err
	}
	return Page{Rows: rows, NextCursor: next}, nil
}

func decodeQueryRows(ctx context.Context, body []byte, limit int) ([]Row, error) {
	var rows []Row
	for len(body) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		line, rest, found := bytes.Cut(body, []byte{'\n'})
		if !found || len(line) == 0 || !utf8.Valid(line) {
			return nil, errors.New("invalid query record framing or UTF-8")
		}
		body = rest
		var record struct {
			TS   *string `json:"ts"`
			Line *string `json:"line"`
		}
		if err := jsonv2.Unmarshal(line, &record, jsonv2.RejectUnknownMembers(true)); err != nil {
			return nil, fmt.Errorf("decode query record: %w", err)
		}
		if record.TS == nil || record.Line == nil {
			return nil, errors.New("missing query record field")
		}
		ts, err := store.ParseTime(*record.TS)
		if err != nil {
			return nil, fmt.Errorf("decode query timestamp: %w", err)
		}
		if limit > 0 && len(rows) == limit {
			return nil, fmt.Errorf("unexpected row %d: exceeds requested limit %d", len(rows)+1, limit)
		}
		rows = append(rows, Row{TS: ts, Line: *record.Line})
	}
	return rows, nil
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
	const maxCursorBytes = 2048
	next := values[0]
	if len(values) != 1 || len(next) > base64.RawURLEncoding.EncodedLen(maxCursorBytes) {
		return "", errors.New("invalid next cursor header")
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(next)
	if err != nil || len(decoded) <= sha256.Size || len(decoded) > maxCursorBytes || base64.RawURLEncoding.EncodeToString(decoded) != next {
		return "", errors.New("invalid next cursor header")
	}
	if next == cursor {
		return "", errors.New("next cursor does not advance pagination")
	}
	return next, nil
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
		if err := ctx.Err(); err != nil {
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
