package main

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

type row struct {
	TS   time.Time `json:"ts"`
	Line string    `json:"line"`
}

type service struct {
	base   string
	client *http.Client
}

func newService(base string) service {
	return service{base, &http.Client{
		Timeout:       10 * time.Second,
		Transport:     &http.Transport{MaxIdleConnsPerHost: 4},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func (s service) request(ctx context.Context, method, path string, body io.Reader) (res *http.Response, data []byte, err error) {
	req, err := http.NewRequestWithContext(ctx, method, s.base+path, body)
	if err != nil {
		return nil, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/x-ndjson")
	}
	res, err = s.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer func() { err = errors.Join(err, res.Body.Close()) }()
	const maxBody = 8 << 20
	data, err = io.ReadAll(io.LimitReader(res.Body, maxBody+1))
	if err == nil && len(data) > maxBody {
		err = errors.New("response exceeds 8 MiB")
	}
	return res, data, err
}

func (s service) ready(ctx context.Context, exited <-chan struct{}) error {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("readiness: %w", errors.Join(ctx.Err(), last))
		case <-exited:
			return fmt.Errorf("child exited before readiness: %w", errors.Join(errors.New("early exit"), last))
		default:
		}
		res, data, err := s.request(ctx, http.MethodGet, "/ready", nil)
		if err == nil && res.StatusCode == http.StatusOK {
			select {
			case <-exited:
				return errors.New("child exited during readiness")
			default:
				return nil
			}
		}
		last = err
		if err == nil {
			last = fmt.Errorf("HTTP %d: %.200s", res.StatusCode, data)
		}
		select {
		case <-ctx.Done():
		case <-exited:
		case <-ticker.C:
		}
	}
}

func (s service) ingest(ctx context.Context, body []byte) (time.Duration, error) {
	start := time.Now()
	res, data, err := s.request(ctx, http.MethodPost, "/ingest", bytes.NewReader(body))
	if err == nil && (res.StatusCode != http.StatusNoContent || len(data) != 0) {
		err = fmt.Errorf("ingest HTTP %d: %.200s", res.StatusCode, data)
	}
	return time.Since(start), err
}

type filter struct {
	start, end time.Time
	literal    string
}

type walk struct {
	filter filter
	want   []int
	offset int
	pages  int
	cursor string
	seen   map[string]bool
}

func (w *walk) page(ctx context.Context, s service, f fixture) (time.Duration, error) {
	limit := [...]int{1, 193, 509}[w.pages%3]
	q := url.Values{"start": {w.filter.start.Format(time.RFC3339Nano)}, "end": {w.filter.end.Format(time.RFC3339Nano)},
		"contains": {w.filter.literal}, "limit": {strconv.Itoa(limit)}}
	if w.pages == 0 {
		q.Set("page", "1")
	} else {
		q.Set("cursor", w.cursor)
	}
	start := time.Now()
	res, data, err := s.request(ctx, http.MethodGet, "/query?"+q.Encode(), nil)
	elapsed := time.Since(start)
	if err != nil {
		return elapsed, err
	}
	if res.StatusCode != http.StatusOK || res.Header.Get("Content-Type") != "application/x-ndjson" {
		return elapsed, fmt.Errorf("query HTTP %d: %.200s", res.StatusCode, data)
	}
	if len(data) != 0 && data[len(data)-1] != '\n' {
		return elapsed, errors.New("query ended with an unterminated NDJSON row")
	}
	cursors := res.Header.Values("Bog-Next-Cursor")
	if len(cursors) > 1 || len(res.Trailer.Values("Bog-Next-Cursor")) != 0 ||
		(len(cursors) == 1 && (cursors[0] == "" || w.seen[cursors[0]])) {
		return elapsed, errors.New("query returned an invalid or repeated cursor")
	}
	scan := bufio.NewScanner(bytes.NewReader(data))
	count := 0
	for scan.Scan() {
		var got row
		if err := json.Unmarshal(scan.Bytes(), &got); err != nil {
			return elapsed, err
		}
		if w.offset >= len(w.want) || count >= limit {
			return elapsed, fmt.Errorf("unexpected row at offset %d on page %d", w.offset, w.pages+1)
		}
		want := f.row(w.want[w.offset])
		if !got.TS.Equal(want.TS) || got.Line != want.Line {
			return elapsed, fmt.Errorf("row mismatch at offset %d (append ordinal %d)", w.offset, w.want[w.offset])
		}
		w.offset++
		count++
	}
	if err := scan.Err(); err != nil {
		return elapsed, err
	}
	if (w.offset < len(w.want)) != (len(cursors) == 1) || (len(cursors) == 1 && count == 0) {
		return elapsed, fmt.Errorf("query did not advance or terminate correctly at offset %d of %d", w.offset, len(w.want))
	}
	w.cursor = ""
	if len(cursors) == 1 {
		w.cursor = cursors[0]
		w.seen[w.cursor] = true
	}
	w.pages++
	return elapsed, nil
}

type compaction struct {
	Generation    uint64 `json:"generation"`
	CoveredRows   int    `json:"covered_rows"`
	CommittedRows int    `json:"committed_rows"`
	State         string `json:"state"`
}

func (s service) compact(ctx context.Context, path, action string, cutoff int) (compaction, error) {
	var c compaction
	u, err := url.ParseRequestURI(path)
	if err != nil || u.Host != "" || !strings.HasPrefix(path, "/") || u.RawQuery != "" {
		return c, errors.New("invalid compaction control path")
	}
	method := http.MethodGet
	if action != "status" {
		method = http.MethodPost
	}
	res, data, err := s.request(ctx, method, path+"?"+url.Values{"action": {action}, "cutoff": {strconv.Itoa(cutoff)}}.Encode(), nil)
	if err != nil {
		return c, err
	}
	if res.StatusCode != http.StatusOK {
		return c, fmt.Errorf("live compaction %s HTTP %d: %.200s", action, res.StatusCode, data)
	}
	if err := json.Unmarshal(data, &c); err != nil {
		return c, fmt.Errorf("decode compaction %s: %w", action, err)
	}
	return c, nil
}

func (s service) waitCompaction(ctx context.Context, path string, cutoff int, generation uint64) (compaction, error) {
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()
	for {
		c, err := s.compact(ctx, path, "status", cutoff)
		if err != nil {
			return c, err
		}
		switch c.State {
		case "running":
		case "complete":
			if c.Generation <= generation || c.CoveredRows != cutoff || c.CommittedRows < cutoff {
				return c, fmt.Errorf("unexpected compaction cutoff or generation: %+v", c)
			}
			return c, nil
		default:
			return c, fmt.Errorf("unexpected compaction state %q", c.State)
		}
		select {
		case <-ctx.Done():
			return c, ctx.Err()
		case <-ticker.C:
		}
	}
}
