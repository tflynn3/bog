package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// httpExchange carries the received response and transport timings. The body
// is always closed before exchange returns, including after a partial read.
type httpExchange struct {
	attempted bool
	response  *http.Response
	body      []byte
	headersAt *time.Duration
	bodyAt    *time.Duration
}

func exchange(ctx context.Context, client *http.Client, addr, method, path string, data []byte, limit int, start time.Time) (httpExchange, error) {
	var result httpExchange
	if err := ctx.Err(); err != nil {
		return result, err
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://"+addr+path, bytes.NewReader(data))
	if err != nil {
		return result, err
	}
	result.attempted = true
	resp, err := client.Do(req)
	if err != nil {
		return result, err
	}
	headersAt := time.Since(start)
	result.response, result.headersAt = resp, &headersAt
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, int64(limit)+1))
	result.body = body
	if len(body) > limit {
		readErr = errors.Join(readErr, fmt.Errorf("response exceeds %d bytes", limit))
	}
	if readErr == nil {
		bodyAt := time.Since(start)
		result.bodyAt = &bodyAt
	}
	return result, errors.Join(readErr, resp.Body.Close())
}

func (r httpExchange) expectStatus(method, path string, status int) error {
	if r.response.StatusCode != status {
		path, _, _ = strings.Cut(path, "?")
		return fmt.Errorf("%s %s: HTTP %d, expected %d (response %.200q)", method, path, r.response.StatusCode, status, r.body)
	}
	return nil
}
