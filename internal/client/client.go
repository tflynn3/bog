// Package client implements Bog's HTTP query and ingest protocols.
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
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"bog/internal/store"
)

const MaxBatch = store.MaxBatch
const DefaultMaxPage = 32 << 20
const maxError = 8192

// Client reuses a transport and never follows redirects. MaxPageBytes bounds
// the encoded size of a query page; zero selects DefaultMaxPage. Set it before
// using the client concurrently.
type Client struct {
	base         *url.URL
	http         *http.Client
	MaxPageBytes int64
}

// New creates a client for a Bog server URL. A nil transport uses a private,
// reusable clone of the default transport. A supplied transport is used
// directly and may be shared with other clients.
func New(base string, transport *http.Transport) (*Client, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("invalid Bog base URL")
	}
	if transport == nil {
		if t, ok := http.DefaultTransport.(*http.Transport); ok {
			transport = t.Clone()
		} else {
			transport = (&http.Transport{Proxy: http.ProxyFromEnvironment}).Clone()
		}
	}
	return &Client{base: u, http: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// CloseIdleConnections closes idle connections on the underlying transport.
// It is safe to call concurrently with requests and leaves active requests
// alone. If the transport is shared, this also closes its other idle connections.
// The client remains usable afterward.
func (c *Client) CloseIdleConnections() {
	c.http.CloseIdleConnections()
}

// HTTPError retains an HTTP status and a bounded diagnostic body.
type HTTPError struct {
	StatusCode     int
	Body           string
	Cause          error
	QueryRetryable bool
	CursorRejected bool
}

func (e *HTTPError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("Bog HTTP %d: %s: %v", e.StatusCode, e.Body, e.Cause)
	}
	return fmt.Sprintf("Bog HTTP %d: %s", e.StatusCode, e.Body)
}
func (e *HTTPError) Unwrap() error { return e.Cause }

// Retryable reports a query HTTP 503. A capacity rejection may clear on retry;
// a work-budget rejection may require a smaller limit or a larger server budget.
func (e *HTTPError) Retryable() bool { return e.QueryRetryable }

// Query specifies the immutable bounds and filter of a snapshot walk.
type Query struct {
	Start, End time.Time
	Contains   string
	Limit      int
}
type Row = store.Row

// Page contains a fully validated page and its opaque continuation token.
type Page struct {
	Rows       []Row
	NextCursor string
}

func (c *Client) endpoint(path string) string {
	u := *c.base
	escaped := u.EscapedPath()
	prefix := strings.TrimRight(escaped, "/")
	// Remove only literal trailing separators. An escaped slash belongs
	// to the base path segment, even when its decoded value ends in '/'.
	u.Path = u.Path[:len(u.Path)-(len(escaped)-len(prefix))] + path
	u.RawPath = prefix + path
	return u.String()
}
func (c *Client) request(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint(path), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "identity")
	return req, nil
}
func framed(r *http.Response) bool {
	return r.ProtoMajor >= 2 || r.ContentLength >= 0 || (len(r.TransferEncoding) == 1 && r.TransferEncoding[0] == "chunked")
}

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
func readBounded(r io.Reader, n int64) ([]byte, error) {
	if n < 0 || n == math.MaxInt64 {
		return nil, errors.New("invalid response size limit")
	}
	b, err := io.ReadAll(io.LimitReader(r, n+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > n {
		return nil, errors.New("response exceeds size limit")
	}
	return b, nil
}
func statusError(r *http.Response) *HTTPError {
	b, err := readBounded(r.Body, maxError)
	return &HTTPError{StatusCode: r.StatusCode, Body: string(b), Cause: err}
}

// FetchPage requests a first page when cursor is empty, or a continuation
// otherwise. No rows or cursor are returned until the entire response validates.
func (c *Client) FetchPage(ctx context.Context, q Query, cursor string) (page Page, resultErr error) {
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
	defer func() {
		if e := resp.Body.Close(); e != nil {
			page = Page{}
			resultErr = errors.Join(resultErr, e)
		}
		if resultErr != nil {
			var e *HTTPError
			if !errors.As(resultErr, &e) {
				resultErr = &HTTPError{StatusCode: resp.StatusCode, Cause: resultErr}
			}
		}
	}()
	if resp.StatusCode != http.StatusOK {
		e := statusError(resp)
		e.QueryRetryable = resp.StatusCode == http.StatusServiceUnavailable
		e.CursorRejected = cursor != "" && resp.StatusCode == http.StatusBadRequest && framed(resp) && e.Cause == nil && e.Body == "invalid cursor\n"
		return Page{}, e
	}
	limit := c.MaxPageBytes
	if limit == 0 {
		limit = DefaultMaxPage
	}
	b, err := readQueryBody(resp, limit)
	if err != nil {
		return Page{}, fmt.Errorf("read query page: %w", err)
	}
	if _, present := resp.Trailer["Bog-Next-Cursor"]; present {
		return Page{}, errors.New("unexpected next cursor trailer")
	}
	var rows []Row
	for len(b) > 0 {
		line, rest, found := bytes.Cut(b, []byte{'\n'})
		if !found || len(line) == 0 || !utf8.Valid(line) {
			return Page{}, errors.New("invalid query record framing or UTF-8")
		}
		b = rest
		var record struct {
			TS   *string `json:"ts"`
			Line *string `json:"line"`
		}
		if err := jsonv2.Unmarshal(line, &record, jsonv2.RejectUnknownMembers(true)); err != nil {
			return Page{}, fmt.Errorf("decode query record: %w", err)
		}
		if record.TS == nil || record.Line == nil {
			return Page{}, errors.New("missing query record field")
		}
		ts, err := store.ParseTime(*record.TS)
		if err != nil {
			return Page{}, fmt.Errorf("decode query timestamp: %w", err)
		}
		rows = append(rows, Row{TS: ts, Line: *record.Line})
	}
	values := resp.Header.Values("Bog-Next-Cursor")
	if len(values) > 1 || (len(values) == 1 && values[0] == "") {
		return Page{}, errors.New("invalid next cursor header")
	}
	next := ""
	if len(values) == 1 {
		next = values[0]
		if len(next) > base64.RawURLEncoding.EncodedLen(2048) {
			return Page{}, errors.New("invalid next cursor header")
		}
		decoded, err := base64.RawURLEncoding.Strict().DecodeString(next)
		if err != nil || len(decoded) <= sha256.Size || len(decoded) > 2048 || base64.RawURLEncoding.EncodeToString(decoded) != next {
			return Page{}, errors.New("invalid next cursor header")
		}
		if next == cursor {
			return Page{}, errors.New("next cursor does not advance pagination")
		}
		if len(rows) == 0 {
			return Page{}, errors.New("empty query page with next cursor")
		}
	}
	return Page{Rows: rows, NextCursor: next}, nil
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
				Cause:      errors.New("next cursor does not advance pagination: cursor cycle"),
			}
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

type IngestOutcome int

const (
	IngestUnknown IngestOutcome = iota
	IngestCommitted
	IngestRejected
	IngestUncommitted
)

// Ingest sends a whole batch once. Unknown means it may have committed and
// must not be retried without accepting the possibility of duplicates.
func (c *Client) Ingest(ctx context.Context, batch []byte) (outcome IngestOutcome, resultErr error) {
	if err := ctx.Err(); err != nil {
		return IngestRejected, err
	}
	if len(batch) > MaxBatch {
		return IngestRejected, errors.New("batch exceeds 4 MiB")
	}
	// A non-rewindable body prevents net/http replay, even on a reused connection.
	body := io.NopCloser(bytes.NewReader(bytes.Clone(batch)))
	req, err := c.request(ctx, http.MethodPost, "/ingest", body)
	if err != nil {
		return IngestRejected, err
	}
	req.ContentLength = int64(len(batch))
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, err := c.http.Do(req)
	if err != nil {
		return IngestUnknown, err
	}
	defer func() {
		if e := resp.Body.Close(); e != nil {
			resultErr = errors.Join(resultErr, e)
		}
	}()
	if resp.StatusCode == http.StatusNoContent {
		return IngestCommitted, nil
	}
	e := statusError(resp)
	switch resp.StatusCode {
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge:
		return IngestRejected, e
	case http.StatusServiceUnavailable:
		if framed(resp) && e.Cause == nil && (e.Body == "busy\n" || e.Body == "WAL unavailable\n") {
			return IngestUncommitted, e
		}
	}
	return IngestUnknown, e
}
