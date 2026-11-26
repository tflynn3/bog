// Package bogclient implements the Bog HTTP API without retrying ambiguous writes.
package bogclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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

type Row struct {
	TS   time.Time `json:"ts"`
	Line string    `json:"line"`
}
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
	base *url.URL
	http *http.Client
}

func New(address string, hc *http.Client) (*Client, error) {
	u, err := url.Parse(address)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("invalid Bog URL")
	}
	if hc == nil {
		hc = http.DefaultClient
	}
	// Redirects can move a cursor to another process or turn a POST into a GET.
	copyClient := *hc
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{u, &copyClient}, nil
}
func (c *Client) endpoint(path string) *url.URL {
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + path
	return &u
}
func message(r *http.Response) string {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	return strings.TrimSpace(string(b))
}

// Ingest sends exactly one atomic batch. Transport errors and server errors that
// may follow admission are deliberately classified as unknown and never retried.
func (c *Client) Ingest(ctx context.Context, batch []byte) error {
	if len(batch) == 0 || len(batch) > MaxBatch {
		return &RejectedError{0, "batch must contain 1 to 4 MiB bytes"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("/ingest").String(), bytes.NewReader(batch))
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
	// Bog validates these requests before appending. 503 is not assumed safe
	// through intermediaries, which may generate it after forwarding a request.
	if resp.StatusCode == 400 || resp.StatusCode == 413 || resp.StatusCode == 405 {
		return &RejectedError{resp.StatusCode, msg}
	}
	return &UnknownOutcomeError{fmt.Errorf("HTTP %d: %s", resp.StatusCode, msg)}
}

type Query struct {
	Start, End time.Time
	Contains   string
	Limit      int
}

// Query streams each page before requesting the next. Callers should use a
// direct, process-specific address: Bog cursors cannot survive process changes.
func (c *Client) Query(ctx context.Context, q Query, visit func(Row) error) error {
	if q.Limit == 0 {
		q.Limit = 500
	}
	if q.Limit < 1 || q.Limit > 1000 || q.End.Before(q.Start) {
		return errors.New("invalid query")
	}
	cursor := ""
	for {
		u := c.endpoint("/query")
		v := u.Query()
		v.Set("start", q.Start.Format(time.RFC3339Nano))
		v.Set("end", q.End.Format(time.RFC3339Nano))
		v.Set("contains", q.Contains)
		v.Set("limit", strconv.Itoa(q.Limit))
		if cursor == "" {
			v.Set("page", "1")
		} else {
			v.Set("cursor", cursor)
		}
		u.RawQuery = v.Encode()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			msg := message(resp)
			resp.Body.Close()
			if cursor != "" && resp.StatusCode == 400 {
				return &CursorInvalidError{msg}
			}
			return fmt.Errorf("query HTTP %d: %s", resp.StatusCode, msg)
		}
		next := resp.Header.Get("Bog-Next-Cursor")
		dec := json.NewDecoder(bufio.NewReader(resp.Body))
		for {
			var row Row
			err = dec.Decode(&row)
			if err == io.EOF {
				break
			}
			if err != nil {
				resp.Body.Close()
				return err
			}
			if err = visit(row); err != nil {
				resp.Body.Close()
				return err
			}
		}
		if err = resp.Body.Close(); err != nil {
			return err
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

// IngestStream batches complete NDJSON lines without splitting a row. A batch
// is atomic; the stream as a whole may already be partially committed on error.
func (c *Client) IngestStream(ctx context.Context, input io.Reader) error {
	r := bufio.NewReader(input)
	batch := make([]byte, 0, MaxBatch)
	for {
		line, err := r.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) { // Accumulate a long line while retaining the hard wire bound.
			part := append([]byte(nil), line...)
			for errors.Is(err, bufio.ErrBufferFull) {
				if len(part) > MaxBatch {
					return &RejectedError{0, "row exceeds 4 MiB"}
				}
				line, err = r.ReadSlice('\n')
				part = append(part, line...)
			}
			line = part
		}
		if err != nil && err != io.EOF {
			return err
		}
		if len(line) > MaxBatch {
			return &RejectedError{0, "row exceeds 4 MiB"}
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
	}
	if len(bytes.TrimSpace(batch)) != 0 {
		return c.Ingest(ctx, batch)
	}
	return nil
}
