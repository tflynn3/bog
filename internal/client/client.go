// Package client provides a page-validated Bog HTTP client.
package client

import (
	"bytes"
	"context"
	"encoding/json"
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
	if resp.ProtoMajor == 1 && resp.ContentLength < 0 && !hasChunked(resp.TransferEncoding) {
		return nil, false, errors.New("bog: query response has no verifiable framing")
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
		decoded, e := decodeRow(line)
		if e != nil {
			return nil, false, fmt.Errorf("bog: invalid NDJSON record: %w", e)
		}
		rows = append(rows, decoded)
	}
	cursors := resp.Header.Values("Bog-Next-Cursor")
	if len(cursors) > 1 || (len(cursors) == 1 && cursors[0] == "") {
		return nil, false, errors.New("bog: invalid continuation header")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, fmt.Errorf("bog: query: %w", err)
	}
	s.started = true
	if len(cursors) == 0 {
		s.done = true
	} else {
		s.cursor = cursors[0]
	}
	return rows, s.done, nil
}
func hasChunked(encodings []string) bool {
	for _, encoding := range encodings {
		if strings.EqualFold(encoding, "chunked") {
			return true
		}
	}
	return false
}

// decodeRow validates the query representation independently of the ingest batch limit.
func decodeRow(line []byte) (Row, error) {
	if !json.Valid(line) || !validSurrogates(line) {
		return Row{}, errors.New("invalid JSON string encoding")
	}
	d := json.NewDecoder(bytes.NewReader(line))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return Row{}, errors.New("expected object")
	}
	var timestamp, text string
	seen := map[string]bool{}
	for d.More() {
		token, err = d.Token()
		if err != nil {
			return Row{}, err
		}
		key, ok := token.(string)
		if !ok || seen[key] || (key != "ts" && key != "line") {
			return Row{}, errors.New("invalid field")
		}
		seen[key] = true
		var raw json.RawMessage
		if err = d.Decode(&raw); err != nil {
			return Row{}, err
		}
		var value string
		if len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &value) != nil {
			return Row{}, errors.New("field must be string")
		}
		if key == "ts" {
			timestamp = value
		} else {
			text = value
		}
	}
	if _, err = d.Token(); err != nil {
		return Row{}, err
	}
	var extra any
	if err = d.Decode(&extra); err != io.EOF {
		return Row{}, errors.New("trailing data")
	}
	if !seen["ts"] || !seen["line"] {
		return Row{}, errors.New("missing field")
	}
	ts, err := store.ParseTime(timestamp)
	if err != nil {
		return Row{}, err
	}
	return Row{TS: ts, Line: text}, nil
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

func validSurrogates(b []byte) bool {
	inString := false
	for i := 0; i < len(b); i++ {
		if !inString {
			if b[i] == '"' {
				inString = true
			}
			continue
		}
		if b[i] == '"' {
			inString = false
			continue
		}
		if b[i] != '\\' {
			continue
		}
		i++
		if b[i] != 'u' {
			continue
		}
		n := hexQuad(b[i+1 : i+5])
		i += 4
		if n >= 0xdc00 && n <= 0xdfff {
			return false
		}
		if n >= 0xd800 && n <= 0xdbff {
			if i+6 >= len(b) || b[i+1] != '\\' || b[i+2] != 'u' {
				return false
			}
			low := hexQuad(b[i+3 : i+7])
			if low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		}
	}
	return true
}

func hexQuad(b []byte) uint16 {
	var n uint16
	for _, c := range b {
		n <<= 4
		switch {
		case c >= '0' && c <= '9':
			n += uint16(c - '0')
		case c >= 'a' && c <= 'f':
			n += uint16(c - 'a' + 10)
		default:
			n += uint16(c - 'A' + 10)
		}
	}
	return n
}
