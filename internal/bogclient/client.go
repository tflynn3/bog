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

// Rejected means the server confirmed that a batch was not admitted.
type Rejected struct {
	Status  int
	Message string
}

func (e *Rejected) Error() string {
	return fmt.Sprintf("ingest rejected (%d): %s", e.Status, e.Message)
}

// UnknownOutcome means the batch may have committed. Do not retry without deduplication.
type UnknownOutcome struct{ Err error }

func (e *UnknownOutcome) Error() string { return "ingest outcome unknown: " + e.Err.Error() }
func (e *UnknownOutcome) Unwrap() error { return e.Err }

// CursorInvalid indicates a continuation was rejected, including after restart.
type CursorInvalid struct{ Message string }

func (e *CursorInvalid) Error() string { return "query cursor invalid or expired: " + e.Message }

type Client struct {
	base *url.URL
	http *http.Client
}

func New(address string, hc *http.Client) (*Client, error) {
	u, err := url.Parse(address)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("invalid server URL")
	}
	if hc == nil {
		hc = &http.Client{}
	}
	copyClient := *hc
	// Redirects can move a pagination walk to another process and can replay writes.
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{u, &copyClient}, nil
}
func (c *Client) endpoint(path string) string {
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + path
	u.RawPath = ""
	return u.String()
}
func (c *Client) Health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "GET", c.endpoint("/healthz"), nil)
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
func (c *Client) Ingest(ctx context.Context, batch []byte) error {
	if len(batch) == 0 || len(batch) > MaxBatch {
		return &Rejected{0, "batch must contain 1 to 4194304 bytes"}
	}
	req, err := http.NewRequestWithContext(ctx, "POST", c.endpoint("/ingest"), bytes.NewReader(batch))
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
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	msg := strings.TrimSpace(string(body))
	if resp.StatusCode == 503 || resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return &Rejected{resp.StatusCode, msg}
	}
	return &UnknownOutcome{fmt.Errorf("HTTP %s: %s", resp.Status, msg)}
}

// IngestStream sends bounded, sequential batches without splitting a line. An error
// can follow already committed batches; callers must not replay the whole stream.
func (c *Client) IngestStream(ctx context.Context, input io.Reader) error {
	r := bufio.NewReaderSize(input, 64*1024)
	batch := make([]byte, 0, MaxBatch)
	line := make([]byte, 0, 64*1024)
	for {
		fragment, err := r.ReadSlice('\n')
		if len(line)+len(fragment) > MaxBatch {
			return fmt.Errorf("input line exceeds %d bytes", MaxBatch)
		}
		line = append(line, fragment...)
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil && err != io.EOF {
			return err
		}
		if len(line) > 0 {
			if len(batch)+len(line) > MaxBatch {
				if e := c.Ingest(ctx, batch); e != nil {
					return e
				}
				batch = batch[:0]
			}
			batch = append(batch, line...)
			line = line[:0]
		}
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

// Query streams a stable paginated walk to out. Address must identify one process.
func (c *Client) Query(ctx context.Context, q Query, out io.Writer) error {
	cursor := ""
	for {
		u, _ := url.Parse(c.endpoint("/query"))
		v := u.Query()
		v.Set("start", q.Start)
		v.Set("end", q.End)
		v.Set("contains", q.Contains)
		if q.Limit != 0 {
			v.Set("limit", fmt.Sprint(q.Limit))
		}
		if cursor == "" {
			v.Set("page", "1")
		} else {
			v.Set("cursor", cursor)
		}
		u.RawQuery = v.Encode()
		req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
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
			if cursor != "" && resp.StatusCode == 400 {
				return &CursorInvalid{strings.TrimSpace(string(b))}
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
		if next == cursor {
			return errors.New("server repeated cursor")
		}
		cursor = next
	}
}
