// Package bogclient implements the Bog HTTP protocol.
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
)

const MaxBatch = 4 << 20

// RejectedError indicates that the server confirmed the batch was not admitted.
type RejectedError struct {
	Status  int
	Message string
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("ingest rejected (%d): %s", e.Status, e.Message)
}

// UnknownOutcomeError indicates that a batch may have been committed. Retrying can duplicate rows.
type UnknownOutcomeError struct{ Err error }

func (e *UnknownOutcomeError) Error() string { return "ingest outcome unknown: " + e.Err.Error() }
func (e *UnknownOutcomeError) Unwrap() error { return e.Err }

// CursorError means a continuation was rejected, including after a server restart.
type CursorError struct{ Message string }

func (e *CursorError) Error() string { return "cursor invalid or query rejected: " + e.Message }

type Client struct {
	base *url.URL
	http *http.Client
}

// New creates a client pinned to one endpoint. The supplied HTTP client is reused.
func New(endpoint string, hc *http.Client) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("invalid endpoint")
	}
	if hc == nil {
		hc = http.DefaultClient
	}
	return &Client{base: u, http: hc}, nil
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

// Ingest sends one atomic NDJSON batch and never retries an uncertain write.
func (c *Client) Ingest(ctx context.Context, batch []byte) error {
	if len(batch) == 0 || len(batch) > MaxBatch {
		return &RejectedError{Message: "batch must be 1 to 4 MiB bytes"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("/ingest"), bytes.NewReader(batch))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	// Redirects could silently resubmit a write. Use a shallow client copy.
	hc := *c.http
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := hc.Do(req)
	if err != nil {
		return &UnknownOutcomeError{err}
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	msg := message(resp)
	switch resp.StatusCode {
	case 400, 413, 503:
		return &RejectedError{resp.StatusCode, msg}
	default:
		return &UnknownOutcomeError{fmt.Errorf("HTTP %d: %s", resp.StatusCode, msg)}
	}
}

type Query struct {
	Start, End, Contains string
	Limit                int
	Cursor               string
	Page                 bool
}

// Query streams one page into dst and returns its continuation token.
// Continue with the same Client and endpoint; cursors are process-local.
func (c *Client) Query(ctx context.Context, q Query, dst io.Writer) (string, error) {
	v := url.Values{"start": {q.Start}, "end": {q.End}}
	if q.Contains != "" {
		v.Set("contains", q.Contains)
	}
	if q.Limit != 0 {
		v.Set("limit", strconv.Itoa(q.Limit))
	}
	if q.Cursor != "" {
		v.Set("cursor", q.Cursor)
	} else if q.Page {
		v.Set("page", "1")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint("/query")+"?"+v.Encode(), nil)
	if err != nil {
		return "", err
	}
	hc := *c.http
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg := message(resp)
		if q.Cursor != "" && resp.StatusCode == 400 {
			return "", &CursorError{msg}
		}
		return "", fmt.Errorf("query HTTP %d: %s", resp.StatusCode, msg)
	}
	_, err = io.Copy(dst, resp.Body)
	if err != nil {
		return "", err
	}
	return resp.Header.Get("Bog-Next-Cursor"), nil
}
