// Package client provides a page-validated Bog HTTP client.
package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"bog/internal/store"
)

const MaxIngestBytes = 4 << 20
const DefaultMaxPageBytes = 32 << 20
const maxErrorBytes = 4096

// Row is a query record. TS retains nanosecond precision.
type Row = store.Row

// HTTPError preserves the server's status and a bounded diagnostic body.
type HTTPError struct {
	StatusCode int
	Body       string
	Cause      error
}

func (e *HTTPError) Error() string { return fmt.Sprintf("bog: HTTP %d: %s", e.StatusCode, e.Body) }
func (e *HTTPError) Unwrap() error { return e.Cause }

// Client reuses the supplied HTTP client's transport. Redirects are never followed.
// MaxPageBytes defaults to 32 MiB when zero; increase it for a server configured
// with a larger response limit. Configure Client before concurrent use.
type Client struct {
	base         *url.URL
	http         *http.Client
	MaxPageBytes int64
}

func New(baseURL string, hc *http.Client) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("bog: invalid base URL: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("bog: invalid base URL")
	}
	if hc == nil {
		hc = http.DefaultClient
	}
	copyClient := *hc
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{base: u, http: &copyClient}, nil
}
func (c *Client) endpoint(path string) string {
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + path
	u.RawPath = ""
	return u.String()
}

// Query specifies the immutable bounds and filter of a snapshot. Limit zero uses
// the server default; otherwise it must be between 1 and 1000.
type Query struct {
	Start, End time.Time
	Contains   string
	Limit      int
}

// Snapshot tracks a single walk. It must not be used concurrently.
type Snapshot struct {
	client        *Client
	query         Query
	cursor        string
	started, done bool
}

func (c *Client) Snapshot(q Query) (*Snapshot, error) {
	if q.End.Before(q.Start) || q.Limit < 0 || q.Limit > 1000 {
		return nil, errors.New("bog: invalid query")
	}
	return &Snapshot{client: c, query: q}, nil
}

// Next returns one fully validated page. done is true for the terminal page.
// After an error, the same page can be requested again with the original cursor.
func (s *Snapshot) Next(ctx context.Context) (rows []Row, done bool, err error) {
	if s.done {
		return nil, true, nil
	}
	q := url.Values{"start": {s.query.Start.Format(time.RFC3339Nano)}, "end": {s.query.End.Format(time.RFC3339Nano)}, "contains": {s.query.Contains}}
	if s.query.Limit != 0 {
		q.Set("limit", strconv.Itoa(s.query.Limit))
	}
	if s.started {
		q.Set("cursor", s.cursor)
	} else {
		q.Set("page", "1")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.client.endpoint("/query")+"?"+q.Encode(), nil)
	if err != nil {
		return nil, false, err
	}
	resp, err := s.client.http.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("bog: query: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, false, responseError(resp)
	}
	limit := s.client.MaxPageBytes
	if limit == 0 {
		limit = DefaultMaxPageBytes
	}
	if limit < 0 || limit == int64(^uint64(0)>>1) {
		return nil, false, errors.New("bog: invalid page size limit")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, false, fmt.Errorf("bog: read page: %w", err)
	}
	if int64(len(body)) > limit {
		return nil, false, errors.New("bog: page exceeds size limit")
	}
	if resp.ContentLength >= 0 && int64(len(body)) != resp.ContentLength {
		return nil, false, io.ErrUnexpectedEOF
	}
	if !utf8.Valid(body) || (len(body) > 0 && body[len(body)-1] != '\n') {
		return nil, false, errors.New("bog: invalid NDJSON framing")
	}
	for len(body) > 0 {
		var line []byte
		line, body, _ = bytes.Cut(body, []byte{'\n'})
		if len(bytes.TrimSpace(line)) == 0 {
			return nil, false, errors.New("bog: empty NDJSON record")
		}
		decoded, e := store.Decode(line)
		if e != nil || len(decoded) != 1 {
			return nil, false, fmt.Errorf("bog: invalid NDJSON record: %w", e)
		}
		rows = append(rows, decoded[0])
	}
	cursors := resp.Header.Values("Bog-Next-Cursor")
	if len(cursors) > 1 || (len(cursors) == 1 && cursors[0] == "") {
		return nil, false, errors.New("bog: invalid continuation header")
	}
	s.started = true
	if len(cursors) == 0 {
		s.done = true
	} else {
		s.cursor = cursors[0]
	}
	return rows, s.done, nil
}
func responseError(resp *http.Response) error {
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBytes))
	return &HTTPError{StatusCode: resp.StatusCode, Body: string(b), Cause: err}
}

// IngestOutcome describes the authoritative result of an entire batch.
type IngestOutcome uint8

const (
	IngestUnknown IngestOutcome = iota
	IngestCommitted
	IngestRejected
	IngestUncommitted
)

// Ingest sends a batch exactly once, without redirect or automatic body replay.
// An unknown outcome must not be retried blindly: it may already be committed.
func (c *Client) Ingest(ctx context.Context, batch []byte) (IngestOutcome, error) {
	if len(batch) > MaxIngestBytes {
		return IngestRejected, errors.New("bog: batch exceeds 4 MiB")
	}
	if err := ctx.Err(); err != nil {
		return IngestUncommitted, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("/ingest"), bytes.NewReader(batch))
	if err != nil {
		return IngestUncommitted, err
	}
	// Prevent the transport from replaying the body even if configured to consider
	// requests idempotent. Bog does not support idempotency keys.
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, err := c.http.Do(req)
	if err != nil {
		return IngestUnknown, fmt.Errorf("bog: ingest outcome unknown: %w", err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNoContent:
		return IngestCommitted, nil
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge:
		return IngestRejected, responseError(resp)
	case http.StatusServiceUnavailable:
		return IngestUncommitted, responseError(resp)
	default:
		return IngestUnknown, responseError(resp)
	}
}
