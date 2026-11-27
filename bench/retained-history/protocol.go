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
	base              string
	client            *http.Client
	queryPhases       map[string]*queryPhase
	readMemoryCurrent func(context.Context) (uint64, error)
}

func newService(base string) service {
	return service{base: base, client: &http.Client{
		Timeout:       10 * time.Second,
		Transport:     &http.Transport{MaxIdleConnsPerHost: 4},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func (s service) request(ctx context.Context, method, path string, body io.Reader) (res *http.Response, data []byte, err error) {
	return s.requestObserved(ctx, method, path, body, nil)
}

func (s service) requestObserved(ctx context.Context, method, path string, body io.Reader, q *queryAttempt) (res *http.Response, data []byte, err error) {
	return s.requestBounded(ctx, method, path, body, q, 8<<20)
}

func (s service) requestBounded(ctx context.Context, method, path string, body io.Reader, q *queryAttempt, maxBody int64) (res *http.Response, data []byte, err error) {
	start := time.Now()
	if q != nil {
		budget := s.client.Timeout
		if deadline, ok := ctx.Deadline(); ok && (budget == 0 || deadline.Sub(start) < budget) {
			budget = max(0, deadline.Sub(start))
		}
		*q = queryAttempt{Admission: "unknown", Phase: "headers", Budget: budget}
		defer func() { q.Elapsed = time.Since(start) }()
	}
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
	if q != nil {
		q.observeHeaders(res, time.Since(start))
	}
	defer func() { err = errors.Join(err, res.Body.Close()) }()
	data, err = io.ReadAll(io.LimitReader(res.Body, maxBody+1))
	if err == nil && int64(len(data)) > maxBody {
		err = fmt.Errorf("response exceeds %d bytes", maxBody)
	}
	if q != nil {
		q.BodyBytes = len(data)
		if err == nil {
			elapsed := time.Since(start)
			q.Body = &elapsed
			q.observeRejection(data)
			q.Phase = "close"
		}
	}
	return res, data, err
}

func (s service) ready(ctx context.Context, exited <-chan struct{}) error {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-exited:
			cancel()
		case <-ctx.Done():
		}
	}()
	defer func() {
		cancel()
		<-done
	}()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		select {
		case <-exited:
			return fmt.Errorf("child exited before readiness: %w", errors.Join(errors.New("early exit"), last))
		default:
		}
		if err := ctx.Err(); err != nil {
			select {
			case <-exited:
				return fmt.Errorf("child exited before readiness: %w", errors.Join(errors.New("early exit"), last))
			default:
				return fmt.Errorf("readiness: %w", errors.Join(err, last))
			}
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
	filter           filter
	want             []int
	offset           int
	pages            int
	pageLimit        int
	firstResumeLimit int
	cursor           string
	seen             map[string]bool
	phase            string
}

func (w *walk) page(ctx context.Context, s service, f fixture) (_ time.Duration, err error) {
	var attempt queryAttempt
	defer func() { recordQuery(s.queryPhases, w.phase, attempt, err) }()
	limit := [...]int{1, 193, 509}[w.pages%3]
	if w.pageLimit > 0 {
		limit = w.pageLimit
	}
	if w.pages == 1 && w.firstResumeLimit > 0 {
		limit = w.firstResumeLimit
	}
	q := url.Values{"start": {w.filter.start.Format(time.RFC3339Nano)}, "end": {w.filter.end.Format(time.RFC3339Nano)},
		"contains": {w.filter.literal}, "limit": {strconv.Itoa(limit)}}
	if w.pages == 0 {
		q.Set("page", "1")
	} else {
		q.Set("cursor", w.cursor)
	}
	start := time.Now()
	res, data, err := s.requestObserved(ctx, http.MethodGet, "/query?"+q.Encode(), nil, &attempt)
	elapsed := time.Since(start)
	if err != nil {
		return elapsed, err
	}
	attempt.Phase = "validation"
	if res.StatusCode != http.StatusOK || res.Header.Get("Content-Type") != "application/x-ndjson" {
		return elapsed, fmt.Errorf("query HTTP %d (%s)", res.StatusCode, attempt.Admission)
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

// exhaust returns the request that produced the terminal page, including its
// input cursor. Replaying this copy leaves the completed walk unchanged.
func (w *walk) exhaust(ctx context.Context, s service, f fixture) (walk, error) {
	for {
		request := *w
		if _, err := w.page(ctx, s, f); err != nil {
			return walk{}, err
		}
		if w.cursor == "" {
			return request, nil
		}
	}
}

type compaction struct {
	Generation    uint64 `json:"generation"`
	CoveredRows   int    `json:"covered_rows"`
	CommittedRows int    `json:"committed_rows"`
	State         string `json:"state"`
}

type compactionReply struct {
	compaction
	Metrics json.RawMessage `json:"metrics"`
	memory  *memoryCurrentSample
}

func (s service) compact(ctx context.Context, path, action string, cutoff int) (compactionReply, error) {
	var reply compactionReply
	method := http.MethodGet
	if action != "status" {
		method = http.MethodPost
	}
	res, data, err := s.controlRequest(ctx, method, path, url.Values{"action": {action}, "cutoff": {strconv.Itoa(cutoff)}})
	if err != nil {
		return reply, err
	}
	if res.StatusCode != http.StatusOK {
		return reply, fmt.Errorf("live compaction %s HTTP %d: %.200s", action, res.StatusCode, data)
	}
	// Presence matters even when the accepted cutoff and initial generation
	// are zero. Decode the required fields separately from optional metrics.
	var wire struct {
		State         *string         `json:"state"`
		Generation    *uint64         `json:"generation"`
		CoveredRows   *int            `json:"covered_rows"`
		CommittedRows *int            `json:"committed_rows"`
		Metrics       json.RawMessage `json:"metrics"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return reply, fmt.Errorf("decode compaction %s: %w", action, err)
	}
	if wire.State == nil || wire.Generation == nil || wire.CoveredRows == nil || wire.CommittedRows == nil ||
		(*wire.State != "running" && *wire.State != "complete") ||
		*wire.CoveredRows < 0 || *wire.CommittedRows < *wire.CoveredRows {
		return reply, fmt.Errorf("invalid compaction %s state or row counters", action)
	}
	return compactionReply{compaction: compaction{*wire.Generation, *wire.CoveredRows, *wire.CommittedRows, *wire.State}, Metrics: wire.Metrics}, nil
}

func (s service) controlRequest(ctx context.Context, method, path string, params url.Values) (*http.Response, []byte, error) {
	u, err := url.ParseRequestURI(path)
	if err != nil || u.Host != "" || !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, nil, errors.New("invalid compaction control path")
	}
	return s.requestBounded(ctx, method, path+"?"+params.Encode(), nil, nil, 64<<10)
}

func (s service) retireCapture(ctx context.Context, path, cursor string) error {
	if cursor == "" {
		return errors.New("capture retirement requires a continuation cursor")
	}
	params := url.Values{"action": {"retire"}, "cursor": {cursor}}
	res, data, err := s.controlRequest(ctx, http.MethodPost, path, params)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusNoContent || len(data) != 0 {
		return fmt.Errorf("retire capture: HTTP %d, body bytes %d", res.StatusCode, len(data))
	}
	return nil
}

func (s service) waitCompaction(ctx context.Context, path string, cutoff int, generation uint64) (compaction, error) {
	ticker := time.NewTicker(compactionPollInterval)
	defer ticker.Stop()
	for {
		c, err := s.compact(ctx, path, "status", cutoff)
		if err != nil {
			return c.compaction, err
		}
		if c.State == "complete" {
			return c.compaction, c.validatePublication(cutoff, generation)
		}
		select {
		case <-ctx.Done():
			return c.compaction, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c compaction) validatePublication(cutoff int, previous uint64) error {
	if c.State != "complete" || c.Generation <= previous || c.CoveredRows != cutoff || c.CommittedRows < cutoff {
		return fmt.Errorf("unexpected compaction cutoff or generation: %+v", c)
	}
	return nil
}
