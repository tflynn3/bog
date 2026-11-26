// Package bogclient implements the standalone Bog HTTP contract.
package bogclient

import (
	"bufio"
	"bytes"
	"context"
	"errors"
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
	return fmt.Sprintf("write rejected (%d): %s", e.Status, e.Message)
}

type UnknownOutcomeError struct{ Err error }

func (e *UnknownOutcomeError) Error() string { return "write outcome unknown: " + e.Err.Error() }
func (e *UnknownOutcomeError) Unwrap() error { return e.Err }

type CursorError struct{ Message string }

func (e *CursorError) Error() string { return "query cursor invalid or expired: " + e.Message }

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func New(base string) *Client {
	return &Client{BaseURL: strings.TrimRight(base, "/"), HTTP: &http.Client{}}
}
func (c *Client) request(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return nil, err
	}
	return c.HTTP.Do(req)
}
func (c *Client) Health(ctx context.Context) error {
	r, e := c.request(ctx, http.MethodGet, "/healthz", nil)
	if e != nil {
		return e
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return fmt.Errorf("health: %s", r.Status)
	}
	return nil
}
func (c *Client) Ingest(ctx context.Context, batch []byte) error {
	if len(batch) == 0 || len(batch) > MaxBatch {
		return &RejectedError{0, "batch must contain 1 to 4194304 bytes"}
	}
	r, e := c.request(ctx, http.MethodPost, "/ingest", bytes.NewReader(batch))
	if e != nil {
		return &UnknownOutcomeError{e}
	}
	defer r.Body.Close()
	if r.StatusCode == http.StatusNoContent {
		return nil
	}
	b, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	// The standalone server explicitly guarantees non-admission or rollback on these responses.
	if r.StatusCode == 400 || r.StatusCode == 413 || r.StatusCode == 503 || r.StatusCode == 405 {
		return &RejectedError{r.StatusCode, strings.TrimSpace(string(b))}
	}
	return &UnknownOutcomeError{fmt.Errorf("unexpected HTTP status %s", r.Status)}
}

// IngestStream sends sequential bounded batches. An error stops the stream; previous batches may have committed.
func (c *Client) IngestStream(ctx context.Context, input io.Reader) error {
	reader := bufio.NewReader(input)
	batch := make([]byte, 0, MaxBatch)
	var line []byte
	for {
		part, err := reader.ReadSlice('\n')
		if len(line)+len(part) > MaxBatch {
			return &RejectedError{0, "NDJSON line exceeds wire limit"}
		}
		line = append(line, part...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil && err != io.EOF {
			return err
		}
		if len(bytes.TrimSpace(line)) > 0 {
			if len(batch)+len(line) > MaxBatch {
				if e := c.Ingest(ctx, batch); e != nil {
					return e
				}
				batch = batch[:0]
			}
			batch = append(batch, line...)
		}
		line = line[:0]
		if err == io.EOF {
			if len(batch) > 0 {
				return c.Ingest(ctx, batch)
			}
			return nil
		}
	}
}

type Query struct {
	Start, End, Contains string
	Limit                int
}

// Query streams pages in server order; use a direct process address, not a load balancer.
func (c *Client) Query(ctx context.Context, q Query, out io.Writer) error {
	v := url.Values{"start": {q.Start}, "end": {q.End}, "contains": {q.Contains}, "page": {"1"}}
	if q.Limit != 0 {
		v.Set("limit", fmt.Sprint(q.Limit))
	}
	cursor := ""
	for {
		if cursor != "" {
			v.Del("page")
			v.Set("cursor", cursor)
		}
		r, e := c.request(ctx, http.MethodGet, "/query?"+v.Encode(), nil)
		if e != nil {
			return e
		}
		if r.StatusCode != 200 {
			b, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
			r.Body.Close()
			if cursor != "" && r.StatusCode == 400 {
				return &CursorError{strings.TrimSpace(string(b))}
			}
			return fmt.Errorf("query: %s: %s", r.Status, strings.TrimSpace(string(b)))
		}
		next := r.Header.Get("Bog-Next-Cursor")
		_, e = io.Copy(out, r.Body)
		r.Body.Close()
		if e != nil {
			return e
		}
		if next == "" {
			return nil
		}
		if next == cursor {
			return errors.New("query: repeated cursor")
		}
		cursor = next
	}
}
