// Package client implements Bog's HTTP query and ingest protocols.
package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"

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
	captureExpired bool
}

func (e *HTTPError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("Bog HTTP %d: %s: %v", e.StatusCode, e.Body, e.Cause)
	}
	return fmt.Sprintf("Bog HTTP %d: %s", e.StatusCode, e.Body)
}
func (e *HTTPError) Unwrap() error { return e.Cause }

func (e *HTTPError) Is(target error) bool {
	return target == ErrCaptureExpired && e.captureExpired ||
		target == ErrInvalidCursor && e.CursorRejected
}

// Retryable reports a query HTTP 503. A capacity rejection may clear on retry;
// a work-budget rejection may require a smaller limit or a larger server budget.
func (e *HTTPError) Retryable() bool { return e.QueryRetryable }

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
		if framed(resp) && e.Cause == nil && (resp.ContentLength < 0 || int64(len(e.Body)) == resp.ContentLength) && (e.Body == "busy\n" || e.Body == "WAL unavailable\n" || e.Body == "disk full\n") {
			return IngestUncommitted, e
		}
	}
	return IngestUnknown, e
}
