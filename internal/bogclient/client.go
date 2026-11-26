// Package bogclient implements the standalone Bog HTTP protocol.
package bogclient

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const MaxBatch = 4 << 20

type RejectedError struct {
	Status  int
	Message string
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("ingest rejected (%d): %s", e.Status, e.Message)
}

type UnknownOutcomeError struct{ Err error }

func (e *UnknownOutcomeError) Error() string { return "ingest outcome unknown: " + e.Err.Error() }
func (e *UnknownOutcomeError) Unwrap() error { return e.Err }

type CursorError struct{ Message string }

func (e *CursorError) Error() string { return "cursor invalidated or rejected: " + e.Message }

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func New(base string) *Client {
	return &Client{BaseURL: strings.TrimRight(base, "/"), HTTP: &http.Client{Transport: http.DefaultTransport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}
func (c *Client) request(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/x-ndjson")
	}
	return c.HTTP.Do(req)
}
func message(r *http.Response) string {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	return strings.TrimSpace(string(b))
}
func (c *Client) Health(ctx context.Context) error {
	r, err := c.request(ctx, "GET", "/healthz", nil)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return fmt.Errorf("health: HTTP %d: %s", r.StatusCode, message(r))
	}
	return nil
}

// IngestBatch sends one atomic batch. Never automatically retry an unknown outcome.
func (c *Client) IngestBatch(ctx context.Context, batch []byte) error {
	if len(batch) == 0 || len(batch) > MaxBatch {
		return &RejectedError{Message: "empty or oversized batch"}
	}
	r, err := c.request(ctx, "POST", "/ingest", bytes.NewReader(batch))
	if err != nil {
		return &UnknownOutcomeError{err}
	}
	defer r.Body.Close()
	if r.StatusCode == 204 {
		return nil
	}
	msg := message(r)
	// These responses are explicitly non-admission or confirmed rollback in the standalone server.
	if r.StatusCode == 400 || r.StatusCode == 413 || r.StatusCode == 503 || r.StatusCode == 405 {
		return &RejectedError{r.StatusCode, msg}
	}
	return &UnknownOutcomeError{fmt.Errorf("HTTP %d: %s", r.StatusCode, msg)}
}

// Ingest streams complete lines into bounded batches. Previously acknowledged batches
// remain committed if a later line or batch fails.
func (c *Client) Ingest(ctx context.Context, in io.Reader) error {
	reader := bufio.NewReader(in)
	batch := make([]byte, 0, MaxBatch)
	for {
		line, err := reader.ReadSlice('\n')
		// ReadSlice can return fragments; retain at most one maximum-sized line.
		row := append([]byte(nil), line...)
		for err == bufio.ErrBufferFull {
			if len(row) > MaxBatch {
				return &RejectedError{Message: "line exceeds 4 MiB"}
			}
			line, err = reader.ReadSlice('\n')
			row = append(row, line...)
		}
		if err != nil && err != io.EOF {
			return err
		}
		if len(row) > MaxBatch {
			return &RejectedError{Message: "line exceeds 4 MiB"}
		}
		if len(batch)+len(row) > MaxBatch {
			if e := c.IngestBatch(ctx, batch); e != nil {
				return e
			}
			batch = batch[:0]
		}
		batch = append(batch, row...)
		if err == io.EOF {
			break
		}
	}
	if len(bytes.TrimSpace(batch)) != 0 {
		return c.IngestBatch(ctx, batch)
	}
	return nil
}

// Query writes each response page as it arrives, preserving server order and duplicates.
// BaseURL must identify one process for the entire cursor walk.
func (c *Client) Query(ctx context.Context, start, end, contains string, limit int, out io.Writer) error {
	q := url.Values{"start": {start}, "end": {end}, "contains": {contains}, "limit": {fmt.Sprint(limit)}, "page": {"1"}}
	cursor := ""
	for {
		r, err := c.request(ctx, "GET", "/query?"+q.Encode(), nil)
		if err != nil {
			return err
		}
		if r.StatusCode != 200 {
			msg := message(r)
			r.Body.Close()
			if cursor != "" && r.StatusCode == 400 {
				return &CursorError{msg}
			}
			return fmt.Errorf("query HTTP %d: %s", r.StatusCode, msg)
		}
		next := r.Header.Get("Bog-Next-Cursor")
		_, err = io.Copy(out, r.Body)
		closeErr := r.Body.Close()
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
			return fmt.Errorf("server repeated cursor")
		}
		cursor = next
		q.Del("page")
		q.Set("cursor", cursor)
	}
}
