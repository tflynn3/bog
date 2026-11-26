// Package bogclient implements the Bog HTTP protocol.
package bogclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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

func (e *CursorInvalidError) Error() string { return "cursor invalid or expired: " + e.Message }

type Client struct {
	base string
	http *http.Client
}

func New(base string, transport http.RoundTripper) (*Client, error) {
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("invalid base URL")
	}
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &Client{strings.TrimRight(base, "/"), &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *Client) request(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return nil, err
	}
	return c.http.Do(req)
}
func responseMessage(r *http.Response) string {
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
		return fmt.Errorf("health: HTTP %d: %s", r.StatusCode, responseMessage(r))
	}
	return nil
}

// Ingest sends one atomic batch. No automatic retries are made.
func (c *Client) Ingest(ctx context.Context, batch []byte) error {
	if len(batch) == 0 || len(batch) > MaxBatch {
		return &RejectedError{0, "batch must be between 1 byte and 4 MiB"}
	}
	r, err := c.request(ctx, "POST", "/ingest", bytes.NewReader(batch))
	if err != nil {
		return &UnknownOutcomeError{err}
	}
	defer r.Body.Close()
	if r.StatusCode == http.StatusNoContent {
		return nil
	}
	msg := responseMessage(r)
	// Only the direct Bog protocol guarantees these statuses mean no admission.
	if r.StatusCode == 400 || r.StatusCode == 413 || r.StatusCode == 503 {
		return &RejectedError{r.StatusCode, msg}
	}
	return &UnknownOutcomeError{fmt.Errorf("HTTP %d: %s", r.StatusCode, msg)}
}

// IngestStream splits NDJSON only at line boundaries. Earlier batches remain
// committed if a later batch fails; each individual request is atomic.
func (c *Client) IngestStream(ctx context.Context, src io.Reader) error {
	reader := bufio.NewReaderSize(src, 64*1024)
	batch := make([]byte, 0, MaxBatch)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > MaxBatch {
			return &RejectedError{0, "NDJSON line exceeds 4 MiB"}
		}
		if len(batch)+len(line) > MaxBatch {
			if e := c.Ingest(ctx, batch); e != nil {
				return e
			}
			batch = batch[:0]
		}
		batch = append(batch, line...)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if len(bytes.TrimSpace(batch)) > 0 {
		return c.Ingest(ctx, batch)
	}
	return nil
}

// readLine limits memory even when the input contains an unterminated line.
func readLine(r *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		part, err := r.ReadSlice('\n')
		if len(line)+len(part) > MaxBatch {
			return nil, &RejectedError{0, "NDJSON line exceeds 4 MiB"}
		}
		line = append(line, part...)
		if err != bufio.ErrBufferFull {
			return line, err
		}
	}
}

type Query struct {
	Start, End time.Time
	Contains   string
	Limit      int
}

// Query streams each row in server order, using process-local snapshot cursors.
func (c *Client) Query(ctx context.Context, q Query, emit func(json.RawMessage) error) error {
	params := url.Values{"start": {q.Start.Format(time.RFC3339Nano)}, "end": {q.End.Format(time.RFC3339Nano)}, "contains": {q.Contains}}
	if q.Limit != 0 {
		params.Set("limit", strconv.Itoa(q.Limit))
	}
	params.Set("page", "1")
	for {
		r, err := c.request(ctx, "GET", "/query?"+params.Encode(), nil)
		if err != nil {
			return err
		}
		if r.StatusCode != 200 {
			msg := responseMessage(r)
			r.Body.Close()
			if params.Has("cursor") && r.StatusCode == 400 {
				return &CursorInvalidError{msg}
			}
			return fmt.Errorf("query: HTTP %d: %s", r.StatusCode, msg)
		}
		next := r.Header.Get("Bog-Next-Cursor")
		dec := json.NewDecoder(r.Body)
		for {
			var row json.RawMessage
			err = dec.Decode(&row)
			if err == io.EOF {
				break
			}
			if err != nil {
				r.Body.Close()
				return err
			}
			if err = emit(row); err != nil {
				r.Body.Close()
				return err
			}
		}
		if err := r.Body.Close(); err != nil {
			return err
		}
		if next == "" {
			return nil
		}
		params.Del("page")
		params.Set("cursor", next)
	}
}
