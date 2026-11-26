// Package client provides bounded, page-at-a-time access to a bog HTTP server.
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
)

var (
	// ErrRetryable means the query was rejected temporarily and may be retried.
	ErrRetryable = errors.New("query temporarily unavailable")
	// ErrAmbiguous means an ingest request may have been committed without an acknowledgement.
	ErrAmbiguous = errors.New("ingest outcome ambiguous")
)

type Row struct {
	TS   time.Time `json:"ts"`
	Line string    `json:"line"`
}

// Query describes a snapshot page. Cursor is empty for the first page; callers
// own it and should replace it only after a successful Page call.
type Query struct {
	Start, End time.Time
	Contains   string
	Limit      int
	Cursor     string
}

type Page struct {
	Rows       []Row
	NextCursor string
}

type Client struct {
	base        *url.URL
	http        *http.Client
	maxResponse int64
}

// New creates a client with an explicit maximum query response body size.
// The supplied HTTP client is copied so redirect policy changes do not affect its caller.
func New(baseURL string, hc *http.Client, maxResponseBytes int64) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("invalid base URL")
	}
	if maxResponseBytes <= 0 || maxResponseBytes == int64(^uint64(0)>>1) {
		return nil, errors.New("invalid response limit")
	}
	if hc == nil {
		hc = http.DefaultClient
	}
	copied := *hc
	// Never follow redirects: in particular, a POST must be sent only once.
	copied.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{base: u, http: &copied, maxResponse: maxResponseBytes}, nil
}

func (c *Client) endpoint(path string) string {
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + path
	u.RawPath = ""
	return u.String()
}

// FetchPage returns a complete validated page or a zero Page on any failure.
func (c *Client) FetchPage(ctx context.Context, q Query) (Page, error) {
	u, err := url.Parse(c.endpoint("/query"))
	if err != nil {
		return Page{}, err
	}
	values := u.Query()
	values.Set("start", q.Start.Format(time.RFC3339Nano))
	values.Set("end", q.End.Format(time.RFC3339Nano))
	values.Set("contains", q.Contains)
	if q.Limit != 0 {
		values.Set("limit", strconv.Itoa(q.Limit))
	}
	if q.Cursor == "" {
		values.Set("page", "1")
	} else {
		values.Set("cursor", q.Cursor)
	}
	u.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return Page{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return Page{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusServiceUnavailable {
		return Page{}, ErrRetryable
	}
	if resp.StatusCode != http.StatusOK {
		return Page{}, fmt.Errorf("query: HTTP %d", resp.StatusCode)
	}
	// HTTP/1 EOF-delimited bodies cannot distinguish a complete page from a disconnect.
	if resp.ProtoMajor < 2 && resp.ContentLength < 0 && !chunked(resp.TransferEncoding) {
		return Page{}, errors.New("query response has no explicit framing")
	}
	if resp.ContentLength > c.maxResponse {
		return Page{}, errors.New("query response exceeds limit")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, c.maxResponse+1))
	if err != nil {
		return Page{}, fmt.Errorf("read query response: %w", err)
	}
	if int64(len(body)) > c.maxResponse {
		return Page{}, errors.New("query response exceeds limit")
	}
	if resp.ContentLength >= 0 && int64(len(body)) != resp.ContentLength {
		return Page{}, errors.New("incomplete query response")
	}
	rows, err := decodeRows(body)
	if err != nil {
		return Page{}, err
	}
	return Page{Rows: rows, NextCursor: resp.Header.Get("Bog-Next-Cursor")}, nil
}

func chunked(encodings []string) bool {
	return len(encodings) > 0 && strings.EqualFold(encodings[len(encodings)-1], "chunked")
}

func decodeRows(body []byte) ([]Row, error) {
	if len(body) == 0 {
		return nil, nil
	}
	if !utf8.Valid(body) || body[len(body)-1] != '\n' {
		return nil, errors.New("invalid NDJSON response")
	}
	var rows []Row
	for _, line := range bytes.Split(body[:len(body)-1], []byte{'\n'}) {
		var record struct {
			TS   *time.Time `json:"ts"`
			Line *string    `json:"line"`
		}
		if len(bytes.TrimSpace(line)) == 0 || json.Unmarshal(line, &record) != nil || record.TS == nil || record.Line == nil {
			return nil, errors.New("invalid NDJSON record")
		}
		rows = append(rows, Row{TS: *record.TS, Line: *record.Line})
	}
	return rows, nil
}

// Ingest sends one batch. Only 204 acknowledges it. A transport failure is
// ambiguous because the server may have committed the body before disconnecting.
func (c *Client) Ingest(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("/ingest"), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrAmbiguous, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("ingest: HTTP %d", resp.StatusCode)
	}
	return nil
}
