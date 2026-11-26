// Package bogclient implements the standalone Bog HTTP protocol.
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
	"time"
)

const MaxBatch = 4 << 20

type RejectedError struct {
	Status int
	Detail string
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("ingest rejected (%d): %s", e.Status, e.Detail)
}

type UnknownOutcomeError struct{ Err error }

func (e *UnknownOutcomeError) Error() string { return "ingest outcome unknown: " + e.Err.Error() }
func (e *UnknownOutcomeError) Unwrap() error { return e.Err }

type CursorError struct{ Detail string }

func (e *CursorError) Error() string { return "cursor invalid or expired: " + e.Detail }

type Client struct {
	base string
	http *http.Client
}

func New(raw string, client *http.Client) (*Client, error) {
	u, err := url.Parse(raw)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("invalid server URL")
	}
	if client == nil {
		client = &http.Client{Transport: http.DefaultTransport, Timeout: 30 * time.Second}
	}
	// Redirects could move a cursor to a different process or replay a write.
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{strings.TrimRight(u.String(), "/"), &copyClient}, nil
}
func (c *Client) request(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return nil, err
	}
	return c.http.Do(req)
}
func detail(r *http.Response) string {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	return strings.TrimSpace(string(b))
}
func (c *Client) Health(ctx context.Context) error {
	r, e := c.request(ctx, "GET", "/healthz", nil)
	if e != nil {
		return e
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return fmt.Errorf("health: HTTP %d: %s", r.StatusCode, detail(r))
	}
	return nil
}

// Ingest submits exactly one atomic batch. Never retry an unknown outcome.
func (c *Client) Ingest(ctx context.Context, batch []byte) error {
	if len(batch) == 0 || len(batch) > MaxBatch {
		return fmt.Errorf("batch size must be 1..%d bytes", MaxBatch)
	}
	r, e := c.request(ctx, "POST", "/ingest", bytes.NewReader(batch))
	if e != nil {
		return &UnknownOutcomeError{e}
	}
	defer r.Body.Close()
	if r.StatusCode == http.StatusNoContent {
		return nil
	}
	d := detail(r)
	// These statuses are confirmed non-admission in the standalone server contract.
	if r.StatusCode == 400 || r.StatusCode == 413 || (r.StatusCode == 503 && (d == "busy" || d == "WAL unavailable")) {
		return &RejectedError{r.StatusCode, d}
	}
	return &UnknownOutcomeError{fmt.Errorf("HTTP %d: %s", r.StatusCode, d)}
}

// IngestStream sends bounded batches sequentially; each batch is independently atomic.
func (c *Client) IngestStream(ctx context.Context, src io.Reader) error {
	r := bufio.NewReaderSize(src, 64*1024)
	batch := make([]byte, 0, MaxBatch)
	var line []byte
	flush := func() error {
		if len(bytes.TrimSpace(batch)) == 0 {
			batch = batch[:0]
			return nil
		}
		err := c.Ingest(ctx, batch)
		batch = batch[:0]
		return err
	}
	for {
		part, err := r.ReadSlice('\n')
		if len(line)+len(part) > MaxBatch {
			return errors.New("NDJSON line exceeds 4 MiB")
		}
		line = append(line, part...)
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil && err != io.EOF {
			return err
		}
		if len(batch)+len(line) > MaxBatch {
			if e := flush(); e != nil {
				return e
			}
		}
		batch = append(batch, line...)
		line = line[:0]
		if err == io.EOF {
			return flush()
		}
	}
}

type Query struct {
	Start, End, Contains string
	Limit                int
}

// Query streams each page to dst, retaining the same origin throughout the walk.
func (c *Client) Query(ctx context.Context, q Query, dst io.Writer) error {
	v := url.Values{"start": {q.Start}, "end": {q.End}, "limit": {fmt.Sprint(q.Limit)}}
	if q.Limit == 0 {
		v.Set("limit", "500")
	}
	if q.Contains != "" {
		v.Set("contains", q.Contains)
	}
	v.Set("page", "1")
	for {
		r, e := c.request(ctx, "GET", "/query?"+v.Encode(), nil)
		if e != nil {
			return e
		}
		if r.StatusCode != 200 {
			d := detail(r)
			r.Body.Close()
			if v.Has("cursor") && r.StatusCode == 400 {
				return &CursorError{d}
			}
			return fmt.Errorf("query: HTTP %d: %s", r.StatusCode, d)
		}
		next := r.Header.Get("Bog-Next-Cursor")
		_, e = io.Copy(dst, r.Body)
		closeErr := r.Body.Close()
		if e != nil {
			return e
		}
		if closeErr != nil {
			return closeErr
		}
		if next == "" {
			return nil
		}
		v.Del("page")
		v.Set("cursor", next)
	}
}
