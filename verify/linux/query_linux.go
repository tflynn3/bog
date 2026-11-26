package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func queryPath(cursor string, limit int) string {
	params := url.Values{"start": {epoch.Format(time.RFC3339)}, "end": {epoch.Add(time.Hour).Format(time.RFC3339)}, "limit": {strconv.Itoa(limit)}}
	if cursor == "" {
		params.Set("page", "1")
	} else {
		params.Set("cursor", cursor)
	}
	return "/query?" + params.Encode()
}

// query records one attempt after both transport and response validation finish.
func (s *server) query(ctx context.Context, cursor string, limit, status int) (_ []row, _ string, err error) {
	kind := "fresh_page"
	switch {
	case status == http.StatusBadRequest:
		kind = "stale_cursor"
	case cursor != "":
		kind = "continuation"
	}

	path := queryPath(cursor, limit)
	start := time.Now()
	result, transportErr := s.exchange(ctx, http.MethodGet, path, nil, start)
	if s.diagnostics != nil {
		lifetime := len(s.diagnostics.Candidates)
		defer func() {
			q := result.queryObservation(lifetime, time.Since(start), transportErr)
			s.diagnostics.observeQuery(kind, q, err)
		}()
	}
	if transportErr != nil {
		return nil, "", transportErr
	}
	if err := result.expectStatus(http.MethodGet, path, status); err != nil {
		return nil, "", err
	}
	resp := result.response
	if resp.ContentLength < 0 && resp.ProtoMajor < 2 &&
		(len(resp.TransferEncoding) != 1 || resp.TransferEncoding[0] != "chunked") {
		return nil, "", errors.New("query response has unverifiable HTTP framing")
	}
	next := resp.Header.Values("Bog-Next-Cursor")
	if _, ok := resp.Trailer["Bog-Next-Cursor"]; ok {
		return nil, "", errors.New("unexpected query cursor trailer")
	}
	if status == http.StatusBadRequest {
		if len(next) != 0 {
			return nil, "", errors.New("expected exact invalid cursor rejection without a continuation")
		}
		if !bytes.Equal(result.body, []byte("invalid cursor\n")) {
			return nil, "", fmt.Errorf("expected exact invalid cursor rejection, got %.200q", result.body)
		}
		return nil, "", nil
	}
	contentType := resp.Header.Values("Content-Type")
	if len(contentType) != 1 || contentType[0] != "application/x-ndjson" {
		return nil, "", errors.New("expected query Content-Type application/x-ndjson")
	}
	return decodeQueryPage(result.body, next, cursor, limit)
}

func (r httpExchange) queryObservation(lifetime int, elapsed time.Duration, transportErr error) queryResponse {
	q := queryResponse{Lifetime: lifetime, Elapsed: elapsed, Admission: "unknown", Phase: "headers"}
	if r.response == nil {
		return q
	}
	q.observeHeaders(r.response, r.headersAt)
	q.observeBody(r.body, r.bodyAt, transportErr)
	return q
}

func decodeQueryPage(body []byte, next []string, cursor string, limit int) ([]row, string, error) {
	var rows []row
	for len(body) != 0 {
		line, rest, found := bytes.Cut(body, []byte{'\n'})
		if !found {
			return nil, "", errors.New("query ended with an unterminated NDJSON record")
		}
		if len(rows) == limit {
			return nil, "", errors.New("query returned more rows than requested")
		}
		if text := string(line); strings.ToValidUTF8(text, "") != text {
			return nil, "", fmt.Errorf("query NDJSON row %d contains invalid UTF-8", len(rows)+1)
		}
		var record struct {
			TS   *time.Time `json:"ts,case:strict"`
			Line *string    `json:"line,case:strict"`
		}
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, "", fmt.Errorf("decode query NDJSON row %d: %w", len(rows)+1, err)
		}
		if record.TS == nil || record.Line == nil {
			return nil, "", fmt.Errorf("query NDJSON row %d is missing ts or line", len(rows)+1)
		}
		rows = append(rows, row{TS: *record.TS, Line: *record.Line})
		body = rest
	}
	if len(next) > 1 || (len(next) == 1 && (next[0] == "" || len(rows) == 0)) {
		return nil, "", errors.New("invalid query page continuation")
	}
	if len(next) == 1 {
		if next[0] == cursor {
			return nil, "", errors.New("query repeated a cursor")
		}
		return rows, next[0], nil
	}
	return rows, "", nil
}

func (s *server) page(ctx context.Context, cursor string, limit int) ([]row, string, error) {
	return s.query(ctx, cursor, limit, http.StatusOK)
}

func (s *server) capture(ctx context.Context) (string, error) {
	rows, cursor, err := s.page(ctx, "", 1)
	if err != nil {
		return "", err
	}
	if len(rows) != 1 || cursor == "" {
		return "", errors.New("expected one row and an issuer cursor")
	}
	rows, _, err = s.page(ctx, cursor, 1)
	if err == nil && len(rows) != 1 {
		err = errors.New("issuer cursor yielded no continuation row")
	}
	return cursor, err
}

func (s *server) resume(ctx context.Context, cursor string) error {
	if err := s.start(ctx); err != nil {
		return err
	}
	return s.rejectCursor(ctx, cursor)
}

func (s *server) rejectCursor(ctx context.Context, cursor string) error {
	if cursor == "" {
		return errors.New("missing pre-restart cursor")
	}
	_, _, err := s.query(ctx, cursor, 1, http.StatusBadRequest)
	return err
}

func (s *server) reconcile(ctx context.Context, l ledger, cursor string, got []row) (reconciliation, error) {
	seen := make(map[string]bool)
	maxRows := 0
	for _, b := range l {
		maxRows += len(b.rows)
	}
	for i := 0; i <= maxRows; i++ {
		page, next, err := s.page(ctx, cursor, 1+i%3)
		if err != nil {
			return reconciliation{}, err
		}
		got = append(got, page...)
		if len(got) > maxRows {
			break
		}
		if next == "" {
			return l.reconcile(got)
		}
		if seen[next] {
			return reconciliation{}, errors.New("query repeated a cursor")
		}
		seen[next] = true
		cursor = next
	}
	return reconciliation{}, errors.New("query exceeded the fixture row/page bound")
}
