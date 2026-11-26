// Package bogclient provides a bounded client for the Bog HTTP API.
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

func (e *CursorError) Error() string { return "cursor invalid or expired: " + e.Message }

type Client struct {
	base *url.URL
	http *http.Client
}

func New(raw string, transport http.RoundTripper) (*Client, error) {
	u, err := url.Parse(raw)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("invalid Bog URL")
	}
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &Client{base: u, http: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *Client) endpoint(path string) string {
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + path
	u.RawPath = ""
	return u.String()
}
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
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 400 && resp.StatusCode < 500 || resp.StatusCode == http.StatusServiceUnavailable {
		return &RejectedError{resp.StatusCode, strings.TrimSpace(string(msg))}
	}
	return &UnknownOutcomeError{fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))}
}

// IngestStream sends consecutive whole-line batches. Each acknowledged batch is
// committed independently; an error does not roll back preceding batches.
func (c *Client) IngestStream(ctx context.Context, src io.Reader) error {
	r := bufio.NewReaderSize(src, 64*1024)
	batch := make([]byte, 0, MaxBatch)
	for {
		line, err := r.ReadSlice('\n')
		for errors.Is(err, bufio.ErrBufferFull) {
			if len(line) > MaxBatch {
				return &RejectedError{0, "line exceeds 4 MiB"}
			}
			fragment := append([]byte(nil), line...)
			for errors.Is(err, bufio.ErrBufferFull) {
				var part []byte
				part, err = r.ReadSlice('\n')
				if len(fragment)+len(part) > MaxBatch {
					return &RejectedError{0, "line exceeds 4 MiB"}
				}
				fragment = append(fragment, part...)
			}
			line = fragment
		}
		if err != nil && err != io.EOF {
			return err
		}
		if len(line) > MaxBatch {
			return &RejectedError{0, "line exceeds 4 MiB"}
		}
		if len(batch)+len(line) > MaxBatch {
			if e := c.Ingest(ctx, batch); e != nil {
				return e
			}
			batch = batch[:0]
		}
		batch = append(batch, line...)
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

// Query streams each page directly to dst, retaining the same origin for cursors.
func (c *Client) Query(ctx context.Context, q Query, dst io.Writer) error {
	v := url.Values{"start": {q.Start}, "end": {q.End}, "page": {"1"}}
	if q.Contains != "" {
		v.Set("contains", q.Contains)
	}
	if q.Limit != 0 {
		v.Set("limit", fmt.Sprint(q.Limit))
	}
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint("/query")+"?"+v.Encode(), nil)
		if err != nil {
			return err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			if v.Get("cursor") != "" && resp.StatusCode == http.StatusBadRequest {
				return &CursorError{strings.TrimSpace(string(b))}
			}
			return fmt.Errorf("query HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
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
		v.Del("page")
		v.Set("cursor", next)
	}
}
