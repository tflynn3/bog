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
// reusable clone of the default transport.
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

// Retryable reports whether a query returned a transient HTTP 503. Callers
// can retry from the cursor returned by Walk or WalkFrom.
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
	// Trim only literal separators. An escaped slash is part of its segment.
	u.Path = u.Path[:len(u.Path)-(len(escaped)-len(prefix))] + path
	u.RawPath = prefix + path
	return u.String()
}

// CloseIdleConnections closes idle connections in the client's transport.
func (c *Client) CloseIdleConnections() { c.http.CloseIdleConnections() }

func (c *Client) request(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint(path), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "identity")
	return req, nil
}
func framed(r *http.Response) bool {
	// net/http checks chunk terminators and HTTP/2 stream termination while
	// reading the body. An HTTP/1.x connection close alone proves no length.
	return r.ProtoMajor >= 2 || r.ContentLength >= 0 || (len(r.TransferEncoding) == 1 && r.TransferEncoding[0] == "chunked")
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
	b, err := readResponseBody(r, maxError)
	return &HTTPError{StatusCode: r.StatusCode, Body: string(b), Cause: err}
}

func readResponseBody(r *http.Response, limit int64) ([]byte, error) {
	if r.ContentLength > limit && limit >= 0 {
		return nil, errors.New("response exceeds size limit")
	}
	b, err := readBounded(r.Body, limit)
	if err != nil {
		return nil, err
	}
	if r.ContentLength >= 0 && int64(len(b)) != r.ContentLength {
		if int64(len(b)) < r.ContentLength {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, errors.New("response exceeds declared Content-Length")
	}
	return b, nil
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
	if !framed(resp) {
		return Page{}, errors.New("query response has unverifiable framing")
	}
	encodings := resp.Header.Values("Content-Encoding")
	if resp.Uncompressed || len(encodings) > 1 || (len(encodings) == 1 && !strings.EqualFold(encodings[0], "identity")) {
		return Page{}, errors.New("unsupported query content encoding")
	}
	limit := c.MaxPageBytes
	if limit == 0 {
		limit = DefaultMaxPage
	}
	b, err := readResponseBody(resp, limit)
	if err != nil {
		return Page{}, fmt.Errorf("read query page: %w", err)
	}
	if _, ok := resp.Trailer["Bog-Next-Cursor"]; ok {
		return Page{}, errors.New("unexpected next cursor trailer")
	}
	return decodePage(b, resp.Header.Values("Bog-Next-Cursor"))
}

func decodePage(b []byte, cursorValues []string) (Page, error) {
	next, err := decodeNextCursor(cursorValues)
	if err != nil {
		return Page{}, err
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
	return Page{Rows: rows, NextCursor: next}, nil
}

func decodeNextCursor(values []string) (string, error) {
	if len(values) == 0 {
		return "", nil
	}
	if len(values) == 1 && len(values[0]) <= base64.RawURLEncoding.EncodedLen(2048) {
		next := values[0]
		decoded, err := base64.RawURLEncoding.Strict().DecodeString(next)
		if err == nil && len(decoded) > sha256.Size && len(decoded) <= 2048 && base64.RawURLEncoding.EncodeToString(decoded) == next {
			return next, nil
		}
	}
	return "", errors.New("invalid next cursor header")
}

// Walk delivers validated pages in order. It advances only after consume succeeds;
// the returned cursor identifies the page to retry if a request or consume fails.
func (c *Client) Walk(ctx context.Context, q Query, consume func(Page) error) (string, error) {
	return c.WalkFrom(ctx, q, "", consume)
}

// WalkFrom resumes a walk at cursor.
func (c *Client) WalkFrom(ctx context.Context, q Query, cursor string, consume func(Page) error) (string, error) {
	if consume == nil {
		return cursor, errors.New("nil page consumer")
	}
	for {
		p, err := c.FetchPage(ctx, q, cursor)
		if err != nil {
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
