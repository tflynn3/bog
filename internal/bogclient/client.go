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
	Status  int
	Message string
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("ingest rejected (%d): %s", e.Status, e.Message)
}

type UnknownOutcomeError struct{ Err error }

func (e *UnknownOutcomeError) Error() string { return "ingest outcome unknown: " + e.Err.Error() }
func (e *UnknownOutcomeError) Unwrap() error { return e.Err }

// StreamError reports how many complete batches were acknowledged before a stream failed.
type StreamError struct {
	AcknowledgedBatches int
	Err                 error
}

func (e *StreamError) Error() string {
	return fmt.Sprintf("ingest stream failed after %d acknowledged batches: %v", e.AcknowledgedBatches, e.Err)
}
func (e *StreamError) Unwrap() error { return e.Err }

type CursorInvalidError struct{ Message string }

func (e *CursorInvalidError) Error() string { return "cursor invalid or expired: " + e.Message }

type Client struct {
	base *url.URL
	http *http.Client
}

func New(raw string, transport http.RoundTripper) (*Client, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return nil, errors.New("invalid server URL")
	}
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &Client{u, &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
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
		return fmt.Errorf("health: HTTP %d: %s", resp.StatusCode, message(resp))
	}
	return nil
}

// Ingest sends one atomic batch. No automatic retry is safe without deduplication.
func (c *Client) Ingest(ctx context.Context, batch []byte) error {
	if len(batch) == 0 || len(batch) > MaxBatch {
		return fmt.Errorf("batch must be between 1 and %d bytes", MaxBatch)
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
	// These are validation responses from Bog. Other responses may come from an intermediary.
	if resp.StatusCode == 400 || resp.StatusCode == 413 {
		return &RejectedError{resp.StatusCode, msg}
	}
	return &UnknownOutcomeError{fmt.Errorf("HTTP %d: %s", resp.StatusCode, msg)}
}

// IngestStream submits bounded batches serially. Previously acknowledged batches remain committed if a later batch fails.
func (c *Client) IngestStream(ctx context.Context, in io.Reader) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 64*1024), MaxBatch+1)
	batch := make([]byte, 0, MaxBatch)
	acknowledged := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := c.Ingest(ctx, batch); err != nil {
			return err
		}
		acknowledged++
		batch = batch[:0]
		return nil
	}
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line)+1 > MaxBatch {
			return &StreamError{acknowledged, fmt.Errorf("NDJSON line exceeds batch limit")}
		}
		if len(batch)+len(line)+1 > MaxBatch {
			if err := flush(); err != nil {
				return &StreamError{acknowledged, err}
			}
		}
		batch = append(batch, line...)
		batch = append(batch, '\n')
	}
	if err := scanner.Err(); err != nil {
		return &StreamError{acknowledged, fmt.Errorf("read NDJSON: %w", err)}
	}
	if err := flush(); err != nil {
		return &StreamError{acknowledged, err}
	}
	return nil
}

type Query struct {
	Start, End time.Time
	Contains   string
	Limit      int
}

// QueryStream writes each page as received, preserving server order and duplicates.
// The configured URL must identify one process for the duration of the walk.
func (c *Client) QueryStream(ctx context.Context, q Query, out io.Writer) error {
	if q.Limit < 1 || q.Limit > 1000 || q.End.Before(q.Start) {
		return errors.New("invalid query bounds or limit")
	}
	values := url.Values{"start": {q.Start.Format(time.RFC3339Nano)}, "end": {q.End.Format(time.RFC3339Nano)}, "contains": {q.Contains}, "limit": {fmt.Sprint(q.Limit)}, "page": {"1"}}
	cursor := ""
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint("/query")+"?"+values.Encode(), nil)
		if err != nil {
			return err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode != 200 {
			msg := message(resp)
			resp.Body.Close()
			if cursor != "" && resp.StatusCode == 400 {
				return &CursorInvalidError{msg}
			}
			return fmt.Errorf("query: HTTP %d: %s", resp.StatusCode, msg)
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
		if next == cursor {
			return errors.New("server repeated cursor")
		}
		cursor = next
		values.Del("page")
		values.Set("cursor", cursor)
	}
}
