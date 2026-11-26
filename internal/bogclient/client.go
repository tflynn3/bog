// Package bogclient provides a bounded client for the Bog HTTP API.
package bogclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const MaxBatch = 4 << 20

type Client struct {
	base  *url.URL
	http  *http.Client
	slots chan struct{}
}

// Rejected indicates that the server confirmed the batch was not admitted.
type Rejected struct {
	Status  int
	Message string
}

func (e *Rejected) Error() string {
	return fmt.Sprintf("bog rejected request (%d): %s", e.Status, e.Message)
}

// UnknownOutcome means the server may have committed the batch. Do not retry blindly.
type UnknownOutcome struct{ Err error }

func (e *UnknownOutcome) Error() string { return "bog write outcome unknown: " + e.Err.Error() }
func (e *UnknownOutcome) Unwrap() error { return e.Err }

// CursorInvalid indicates that a continuation was rejected, including after restart.
type CursorInvalid struct{ Message string }

func (e *CursorInvalid) Error() string { return "bog cursor invalid: " + e.Message }

func New(endpoint string, hc *http.Client) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("invalid Bog endpoint %q", endpoint)
	}
	if hc == nil {
		hc = http.DefaultClient
	}
	// Redirects can move a cursor to another process or replay a write.
	copyClient := *hc
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{base: u, http: &copyClient, slots: make(chan struct{}, 16)}, nil
}
func (c *Client) endpoint(path string) string {
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + path
	u.RawPath = ""
	return u.String()
}
func (c *Client) Health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint("/healthz"), nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return responseError(resp)
	}
	return nil
}
func responseError(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return &Rejected{resp.StatusCode, strings.TrimSpace(string(b))}
}
func (c *Client) Ingest(ctx context.Context, batch []byte) error {
	if len(batch) == 0 || len(batch) > MaxBatch {
		return &Rejected{0, "batch must contain 1 to 4194304 bytes"}
	}
	select {
	case c.slots <- struct{}{}:
		defer func() { <-c.slots }()
	case <-ctx.Done():
		return ctx.Err()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("/ingest"), bytes.NewReader(batch))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, err := c.http.Do(req)
	if err != nil {
		return &UnknownOutcome{err}
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	// The documented Bog API guarantees these responses are non-admission.
	if resp.StatusCode == 400 || resp.StatusCode == 413 || resp.StatusCode == 503 {
		return responseError(resp)
	}
	return &UnknownOutcome{responseError(resp)}
}

type Query struct {
	Start, End, Contains string
	Limit                int
}

// Query streams a stable snapshot to dst. The endpoint must identify one process,
// not a load-balanced service. A restarted process rejects its old cursors.
func (c *Client) Query(ctx context.Context, q Query, dst io.Writer) error {
	cursor := ""
	for {
		values := url.Values{"start": {q.Start}, "end": {q.End}, "contains": {q.Contains}}
		if q.Limit != 0 {
			values.Set("limit", fmt.Sprint(q.Limit))
		}
		if cursor == "" {
			values.Set("page", "1")
		} else {
			values.Set("cursor", cursor)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint("/query")+"?"+values.Encode(), nil)
		if err != nil {
			return err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			e := responseError(resp)
			resp.Body.Close()
			if cursor != "" && resp.StatusCode == 400 {
				return &CursorInvalid{e.Error()}
			}
			return e
		}
		next := resp.Header.Get("Bog-Next-Cursor")
		_, err = io.Copy(dst, resp.Body)
		closeErr := resp.Body.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if next == "" {
			return nil
		}
		if next == cursor {
			return fmt.Errorf("bog returned repeated cursor")
		}
		cursor = next
	}
}
