// Package bogclient provides a bounded, streaming client for Bog's HTTP API.
package bogclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const MaxBatch = 4 << 20

type RejectedError struct {
	Status  int
	Message string
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("write rejected (%d): %s", e.Status, e.Message)
}

type UnknownOutcomeError struct{ Err error }

func (e *UnknownOutcomeError) Error() string { return "write outcome unknown: " + e.Err.Error() }
func (e *UnknownOutcomeError) Unwrap() error { return e.Err }

type CursorInvalidError struct{ Message string }

func (e *CursorInvalidError) Error() string { return "cursor invalidated: " + e.Message }

type Client struct {
	base *url.URL
	http *http.Client
}

// New uses a reusable HTTP transport and rejects redirects, which could move a cursor to another process.
func New(endpoint string) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("invalid endpoint %q", endpoint)
	}
	return &Client{base: u, http: &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone(), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *Client) endpoint(path string) string {
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + path
	u.RawPath = ""
	return u.String()
}
func message(r *http.Response) string {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	return strings.TrimSpace(string(b))
}

// Ingest sends exactly one atomic batch. It never retries an ambiguous outcome.
func (c *Client) Ingest(ctx context.Context, batch []byte) error {
	if len(batch) == 0 || len(batch) > MaxBatch {
		return &RejectedError{0, "batch must contain 1 to 4194304 bytes"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("/ingest"), bytes.NewReader(batch))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, err := c.http.Do(req)
	if err != nil {
		return &UnknownOutcomeError{err}
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	msg := message(resp)
	// Bog rejects malformed and oversized requests before admission. A 503 may
	// instead originate from an intermediary after an upstream write, so its
	// outcome cannot be established from the HTTP status alone.
	switch resp.StatusCode {
	case 400, 413:
		return &RejectedError{resp.StatusCode, msg}
	}
	return &UnknownOutcomeError{fmt.Errorf("HTTP %d: %s", resp.StatusCode, msg)}
}

type Query struct {
	Start, End time.Time
	Contains   string
	Limit      int
}

// Query streams the response without buffering the result set. The caller closes body.
// Pass the returned cursor to continue the same snapshot at this same endpoint.
func (c *Client) Query(ctx context.Context, q Query, cursor string) (body io.ReadCloser, next string, err error) {
	u, _ := url.Parse(c.endpoint("/query"))
	values := url.Values{}
	values.Set("start", q.Start.Format(time.RFC3339Nano))
	values.Set("end", q.End.Format(time.RFC3339Nano))
	if q.Contains != "" {
		values.Set("contains", q.Contains)
	}
	if q.Limit != 0 {
		values.Set("limit", strconv.Itoa(q.Limit))
	}
	if cursor == "" {
		values.Set("page", "1")
	} else {
		values.Set("cursor", cursor)
	}
	u.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, "", err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode != 200 {
		defer resp.Body.Close()
		msg := message(resp)
		if cursor != "" && resp.StatusCode == 400 {
			return nil, "", &CursorInvalidError{msg}
		}
		return nil, "", fmt.Errorf("query HTTP %d: %s", resp.StatusCode, msg)
	}
	return resp.Body, resp.Header.Get("Bog-Next-Cursor"), nil
}
