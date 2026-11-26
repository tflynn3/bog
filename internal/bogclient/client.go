// Package bogclient implements the Bog HTTP protocol.
package bogclient

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const MaxBatch = 4 << 20

type Client struct {
	BaseURL string
	HTTP    *http.Client
	// Retry503 requires a direct Bog server with no intermediary synthesizing responses.
	Retry503 bool
}

func New(base string) *Client {
	return &Client{BaseURL: strings.TrimRight(base, "/"), HTTP: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}
func (c *Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}
func (c *Client) request(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return nil, err
	}
	return c.httpClient().Do(req)
}

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

type CursorInvalidError struct{ Message string }

func (e *CursorInvalidError) Error() string { return "cursor invalid or expired: " + e.Message }
func (c *Client) Health(ctx context.Context) error {
	r, e := c.request(ctx, "GET", "/healthz", nil)
	if e != nil {
		return e
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return fmt.Errorf("health: %s", r.Status)
	}
	return nil
}

// Ingest sends one atomic batch. A received 503 from a directly connected Bog
// server confirms non-admission; intermediaries must not synthesize 503 responses.
func (c *Client) Ingest(ctx context.Context, batch []byte) error {
	if len(batch) == 0 || len(batch) > MaxBatch {
		return &RejectedError{0, "batch must contain 1 to 4 MiB bytes"}
	}
	for attempt := 0; ; attempt++ {
		r, e := c.request(ctx, "POST", "/ingest", bytes.NewReader(batch))
		if e != nil {
			return &UnknownOutcomeError{e}
		}
		msg, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
		r.Body.Close()
		if r.StatusCode == http.StatusNoContent {
			return nil
		}
		if r.StatusCode == http.StatusServiceUnavailable && c.Retry503 && attempt < 4 {
			delay := time.Duration(50*(1<<attempt)) * time.Millisecond
			delay += time.Duration(rand.Int64N(int64(delay/2 + 1)))
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return &RejectedError{r.StatusCode, string(msg)}
			case <-timer.C:
			}
			continue
		}
		if r.StatusCode == http.StatusBadRequest || r.StatusCode == http.StatusRequestEntityTooLarge || r.StatusCode == http.StatusMethodNotAllowed || r.StatusCode == http.StatusServiceUnavailable && c.Retry503 {
			return &RejectedError{r.StatusCode, strings.TrimSpace(string(msg))}
		}
		return &UnknownOutcomeError{fmt.Errorf("unexpected HTTP status %s", r.Status)}
	}
}

type Query struct {
	Start, End time.Time
	Contains   string
	Limit      int
}

// Query streams the snapshot as NDJSON to w, preserving order and duplicates.
// The base URL must address the same Bog process for the entire walk.
func (c *Client) Query(ctx context.Context, q Query, w io.Writer) error {
	v := url.Values{"start": {q.Start.Format(time.RFC3339Nano)}, "end": {q.End.Format(time.RFC3339Nano)}, "contains": {q.Contains}, "page": {"1"}}
	if q.Limit != 0 {
		v.Set("limit", strconv.Itoa(q.Limit))
	}
	cursor := ""
	for {
		if cursor != "" {
			v.Del("page")
			v.Set("cursor", cursor)
		}
		r, e := c.request(ctx, "GET", "/query?"+v.Encode(), nil)
		if e != nil {
			return e
		}
		if r.StatusCode != 200 {
			b, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
			r.Body.Close()
			if cursor != "" && r.StatusCode == 400 {
				return &CursorInvalidError{strings.TrimSpace(string(b))}
			}
			return fmt.Errorf("query: %s: %s", r.Status, strings.TrimSpace(string(b)))
		}
		next := r.Header.Get("Bog-Next-Cursor")
		_, e = io.Copy(w, r.Body)
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
		if next == cursor {
			return errors.New("server repeated cursor")
		}
		cursor = next
	}
}

// IngestNDJSON streams complete lines in batches no larger than MaxBatch.
// Earlier batches may already be committed when an error is returned.
func (c *Client) IngestNDJSON(ctx context.Context, input io.Reader) error {
	br := bufio.NewReader(input)
	batch := make([]byte, 0, MaxBatch)
	line := make([]byte, 0, 4096)
	for {
		fragment, err := br.ReadSlice('\n')
		if len(line)+len(fragment) > MaxBatch {
			return &RejectedError{0, "NDJSON line exceeds 4 MiB"}
		}
		line = append(line, fragment...)
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil && err != io.EOF {
			return err
		}
		if len(batch)+len(line) > MaxBatch {
			if e := c.Ingest(ctx, batch); e != nil {
				return e
			}
			batch = batch[:0]
		}
		batch = append(batch, line...)
		line = line[:0]
		if err == io.EOF {
			break
		}
	}
	if len(bytes.TrimSpace(batch)) > 0 {
		return c.Ingest(ctx, batch)
	}
	return nil
}

// Row describes a decoded query result.
type Row struct {
	TS   time.Time `json:"ts"`
	Line string    `json:"line"`
}
