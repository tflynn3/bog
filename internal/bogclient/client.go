// Package bogclient implements the Bog HTTP protocol.
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

func (e *CursorInvalidError) Error() string { return "cursor invalid: " + e.Message }

type Client struct {
	base *url.URL
	http *http.Client
}

func New(endpoint string, hc *http.Client) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("invalid endpoint")
	}
	if hc == nil {
		hc = &http.Client{Transport: http.DefaultTransport}
	}
	// Redirects could move a cursor to another process or replay a write.
	copyClient := *hc
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{u, &copyClient}, nil
}
func (c *Client) endpoint(path string) string {
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + path
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
	if resp.StatusCode != 200 {
		return fmt.Errorf("health: %s", resp.Status)
	}
	return nil
}

// IngestBatch submits one atomic batch. It never retries an ambiguous write.
func (c *Client) IngestBatch(ctx context.Context, b []byte) error {
	if len(b) > MaxBatch {
		return &RejectedError{413, "batch exceeds 4 MiB"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("/ingest"), bytes.NewReader(b))
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
	msg, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if readErr != nil {
		return &UnknownOutcomeError{readErr}
	}
	// Only server-defined admission/validation failures are known not to commit.
	if resp.StatusCode == 400 || resp.StatusCode == 413 || (resp.StatusCode == 503 && (strings.TrimSpace(string(msg)) == "busy" || strings.TrimSpace(string(msg)) == "WAL unavailable")) {
		return &RejectedError{resp.StatusCode, strings.TrimSpace(string(msg))}
	}
	return &UnknownOutcomeError{fmt.Errorf("HTTP %s: %s", resp.Status, strings.TrimSpace(string(msg)))}
}

// Ingest reads newline-delimited records and submits bounded batches sequentially.
// Earlier batches may have committed when an error is returned.
func (c *Client) Ingest(ctx context.Context, r io.Reader) error {
	br := bufio.NewReader(r)
	batch := make([]byte, 0, MaxBatch)
	record := make([]byte, 0, 4096)
	for {
		part, err := br.ReadSlice('\n')
		if len(record)+len(part) > MaxBatch {
			return &RejectedError{413, "record exceeds 4 MiB"}
		}
		record = append(record, part...)
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil && err != io.EOF {
			return err
		}
		if len(batch)+len(record) > MaxBatch {
			if e := c.IngestBatch(ctx, batch); e != nil {
				return e
			}
			batch = batch[:0]
		}
		batch = append(batch, record...)
		record = record[:0]
		if err == io.EOF {
			if len(batch) > 0 {
				return c.IngestBatch(ctx, batch)
			}
			return nil
		}
	}
}

type Query struct {
	Start, End time.Time
	Contains   string
	Limit      int
}

// Query streams the snapshot in server order. Endpoint must identify one process.
func (c *Client) Query(ctx context.Context, q Query, out io.Writer) error {
	if q.Limit == 0 {
		q.Limit = 500
	}
	if q.Limit < 1 || q.Limit > 1000 || q.End.Before(q.Start) {
		return errors.New("invalid query bounds or limit")
	}
	v := url.Values{"start": {q.Start.Format(time.RFC3339Nano)}, "end": {q.End.Format(time.RFC3339Nano)}, "limit": {strconv.Itoa(q.Limit)}}
	if q.Contains != "" {
		v.Set("contains", q.Contains)
	}
	v.Set("page", "1")
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint("/query")+"?"+v.Encode(), nil)
		if err != nil {
			return err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode != 200 {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			if v.Get("cursor") != "" && resp.StatusCode == 400 && strings.TrimSpace(string(b)) == "invalid cursor" {
				return &CursorInvalidError{strings.TrimSpace(string(b))}
			}
			return fmt.Errorf("query: %s: %s", resp.Status, strings.TrimSpace(string(b)))
		}
		next := resp.Header.Get("Bog-Next-Cursor")
		_, err = io.Copy(out, resp.Body)
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
		v.Del("page")
		v.Set("cursor", next)
	}
}
