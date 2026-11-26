package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	seedRows      = 320
	payloadBytes  = 32 << 10
	batchRows     = 32
	receiveWindow = 4096
	queryDeadline = 2 * time.Second
	nextCursor    = "Bog-Next-Cursor"
	fixtureStart  = "2020-01-01T00:00:00Z"
	fixtureEnd    = "2020-01-01T00:10:00Z"
	budgetFilter  = "absent-from-fixture"
)

type row struct {
	TS   string `json:"ts"`
	Line string `json:"line"`
}

type response struct {
	status       int
	contentType  string
	cursor       string
	cursorHeader bool
	framed       bool
	body         []byte
}

func request(ctx context.Context, client *http.Client, method, target string, body io.Reader) (result response, err error) {
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return result, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/x-ndjson")
	}
	res, err := client.Do(req)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, res.Body.Close()) }()
	const limit = 32 << 20
	data, err := io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil {
		return result, err
	}
	if len(data) > limit {
		return result, errors.New("oversized HTTP response")
	}
	cursors, header := res.Header[nextCursor]
	if len(cursors) > 1 || hasCursorTrailer(res) {
		return result, errors.New("multiple cursor headers or unexpected cursor trailer")
	}
	return response{
		status: res.StatusCode, contentType: res.Header.Get("Content-Type"), cursor: res.Header.Get(nextCursor),
		cursorHeader: header, body: data,
		framed: res.ProtoMajor >= 2 || res.ContentLength >= 0 ||
			(len(res.TransferEncoding) == 1 && res.TransferEncoding[0] == "chunked"),
	}, nil
}

func queryPath(cursor string, limit int, filter string) string {
	q := url.Values{"start": {fixtureStart}, "end": {fixtureEnd}, "limit": {strconv.Itoa(limit)}}
	if cursor == "" {
		q.Set("page", "1")
	} else {
		q.Set("cursor", cursor)
	}
	if filter != "" {
		q.Set("contains", filter)
	}
	return "/query?" + q.Encode()
}

func ingest(ctx context.Context, client *http.Client, base string, rows []row) (int, error) {
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			return 0, err
		}
	}
	res, err := request(ctx, client, http.MethodPost, base+"/ingest", &body)
	if err == nil && res.status != http.StatusNoContent {
		err = fmt.Errorf("ingest returned HTTP %d: %.200s", res.status, res.body)
	}
	return res.status, err
}

type rejection struct {
	HTTPStatus int   `json:"http_status"`
	NDJSONRows int   `json:"ndjson_rows"`
	NextCursor bool  `json:"next_cursor"`
	ElapsedMS  int64 `json:"elapsed_ms"`
}

func requireRejection(ctx context.Context, client *http.Client, target string) (rejection, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, queryDeadline)
	defer cancel()
	res, err := request(ctx, client, http.MethodGet, target, nil)
	if err != nil {
		return rejection{}, err
	}
	evidence := rejection{HTTPStatus: res.status, NextCursor: res.cursorHeader, ElapsedMS: time.Since(start).Milliseconds()}
	for line := range bytes.SplitSeq(res.body, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) > 0 && (line[0] == '{' || strings.HasPrefix(res.contentType, "application/x-ndjson")) {
			evidence.NDJSONRows++
		}
	}
	if res.status != http.StatusServiceUnavailable || evidence.NDJSONRows != 0 || evidence.NextCursor || time.Since(start) >= queryDeadline {
		return evidence, fmt.Errorf("expected HTTP 503 with no NDJSON or cursor within two seconds, got %+v", evidence)
	}
	return evidence, nil
}

func requireStaleCursorRejection(ctx context.Context, client *http.Client, base, cursor string) error {
	if cursor == "" {
		return errors.New("missing cursor from previous candidate")
	}
	ctx, cancel := context.WithTimeout(ctx, queryDeadline)
	defer cancel()
	res, err := request(ctx, client, http.MethodGet, base+queryPath(cursor, 1, ""), nil)
	if err != nil {
		return err
	}
	if res.status != http.StatusBadRequest || !res.framed || res.cursorHeader ||
		strings.HasPrefix(strings.ToLower(res.contentType), "application/x-ndjson") || string(res.body) != "invalid cursor\n" {
		return fmt.Errorf("expected framed HTTP 400 invalid cursor without continuation, got HTTP %d (framed=%t, cursor header=%t, content type=%q): %.200q",
			res.status, res.framed, res.cursorHeader, res.contentType, res.body)
	}
	return nil
}

func checkRows(res response, want []row) (int, error) {
	if res.status != http.StatusOK || !strings.HasPrefix(res.contentType, "application/x-ndjson") {
		return 0, fmt.Errorf("expected NDJSON HTTP 200, got HTTP %d: %.200s", res.status, res.body)
	}
	scan := bufio.NewScanner(bytes.NewReader(res.body))
	scan.Buffer(make([]byte, 64<<10), 128<<10)
	n := 0
	for scan.Scan() {
		var got row
		if err := json.Unmarshal(scan.Bytes(), &got); err != nil {
			return n, fmt.Errorf("row %d: %w", n, err)
		}
		if n == len(want) || got != want[n] {
			return n, fmt.Errorf("unexpected row at snapshot offset %d", n)
		}
		n++
	}
	return n, scan.Err()
}

func requireSnapshot(ctx context.Context, client *http.Client, base, cursor string, want []row) error {
	seen := make(map[string]bool)
	for {
		if seen[cursor] {
			return errors.New("snapshot cursor repeated")
		}
		seen[cursor] = true
		res, err := request(ctx, client, http.MethodGet, base+queryPath(cursor, 1000, ""), nil)
		if err != nil {
			return err
		}
		// A disconnected writer may need a short interval to release its slot.
		if res.status == http.StatusServiceUnavailable && len(seen) == 1 {
			delete(seen, cursor)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(20 * time.Millisecond):
				continue
			}
		}
		n, err := checkRows(res, want)
		if err != nil {
			return err
		}
		want = want[n:]
		if res.cursor == "" {
			if res.cursorHeader || len(want) != 0 {
				return fmt.Errorf("invalid snapshot termination with %d expected rows remaining", len(want))
			}
			return nil
		}
		if n == 0 || len(want) == 0 {
			return errors.New("cursor returned without additional snapshot rows")
		}
		cursor = res.cursor
	}
}

func serve(ctx context.Context, binary string, client *http.Client, flags []string, check func(string) error) (err error) {
	dir, err := os.MkdirTemp("", "bog-release-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	return serveDirectory(ctx, binary, dir, client, flags, check)
}

func serveDirectory(ctx context.Context, binary, dir string, client *http.Client, flags []string, check func(string) error) (err error) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return err
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, binary, append([]string{"-data-dir", dir, "-addr", addr}, flags...)...)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var killOnce sync.Once
	var killErr error
	cmd.Cancel = func() error {
		killOnce.Do(func() {
			killErr = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			if errors.Is(killErr, syscall.ESRCH) {
				killErr = os.ErrProcessDone
			}
		})
		return killErr
	}
	cmd.WaitDelay = 3 * time.Second
	if err := cmd.Start(); err != nil {
		return err
	}
	cleanGroup := func() error {
		err := cmd.Cancel()
		if errors.Is(err, os.ErrProcessDone) {
			return nil
		}
		return err
	}
	done := make(chan struct{})
	var waitErr, observeErr error
	go func() {
		waitErr, observeErr = waitCandidate(cmd, cleanGroup)
		close(done)
	}()
	defer func() {
		// The leader may have exited while its group is still running.
		killErr := cleanGroup()
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()
		select {
		case <-done:
			var exited *exec.ExitError
			if errors.As(waitErr, &exited) {
				if status, ok := exited.Sys().(syscall.WaitStatus); ok && status.Signal() == syscall.SIGKILL {
					err = errors.Join(err, killErr, observeErr)
					return
				}
			}
			err = errors.Join(err, killErr, observeErr, fmt.Errorf("candidate exited unexpectedly: %v", waitErr))
		case <-timer.C:
			err = errors.Join(err, killErr, errors.New("timed out reaping candidate"))
		}
	}()

	base := "http://" + addr
	startup, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		select {
		case <-done:
			return fmt.Errorf("candidate exited before health check: %v", waitErr)
		default:
		}
		res, probeErr := request(startup, client, http.MethodGet, base+"/healthz", nil)
		if probeErr == nil {
			if res.status != http.StatusOK {
				return fmt.Errorf("startup health returned HTTP %d", res.status)
			}
			select {
			case <-done:
				return fmt.Errorf("candidate exited during health check: %v", waitErr)
			default:
			}
			if err := startup.Err(); err != nil {
				return fmt.Errorf("startup health: %w", err)
			}
			return check(base)
		}
		if err := startup.Err(); err != nil {
			return fmt.Errorf("startup health: %w (last probe: %v)", err, probeErr)
		}
		var netErr *net.OpError
		if !errors.As(probeErr, &netErr) || !errors.Is(netErr, syscall.ECONNREFUSED) {
			return fmt.Errorf("startup health: %w", probeErr)
		}
		select {
		case <-startup.Done():
			return fmt.Errorf("startup health: %w (last probe: %v)", startup.Err(), probeErr)
		case <-done:
			return fmt.Errorf("candidate exited before health check: %v", waitErr)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func record(cases map[string]outcome, name string, evidence any, err error) error {
	raw, encodeErr := json.Marshal(evidence)
	err = errors.Join(err, encodeErr)
	result := outcome{Status: "pass", Evidence: raw}
	if err != nil {
		detail := err.Error()
		result.Status, result.Detail = "fail", &detail
		err = fmt.Errorf("%s: %w", name, err)
	}
	cases[name] = result
	return err
}

func probeAdmission(ctx context.Context, binary string, cases map[string]outcome) error {
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport, Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	seed := make([]row, seedRows)
	start, err := time.Parse(time.RFC3339, fixtureStart)
	if err != nil {
		return err
	}
	padding := strings.Repeat("x", payloadBytes)
	for i := range seed {
		seed[i] = row{start.Add(time.Duration(i) * time.Second).Format(time.RFC3339), fmt.Sprintf("seed-%04d-%s", i, padding)}
	}

	saturationErr := serve(ctx, binary, client, []string{"-query-concurrency=1"}, func(base string) error {
		return probeSaturation(ctx, client, base, seed, cases)
	})
	if _, ok := cases["one_slot_saturation"]; !ok {
		saturationErr = record(cases, "one_slot_saturation", nil, saturationErr)
	}
	var budgetProof struct {
		rejection
		FixedBudgetWalk budgetWalkProof `json:"fixed_budget_walk"`
	}
	rejectionErr := serve(ctx, binary, client, []string{"-query-concurrency=1", "-query-work-budget=1"}, func(base string) error {
		if _, err := ingest(ctx, client, base, seed[:batchRows]); err != nil {
			return err
		}
		result, err := requireRejection(ctx, client, base+queryPath("", 1, budgetFilter))
		budgetProof.rejection = result
		return err
	})
	if rejectionErr != nil {
		rejectionErr = fmt.Errorf("budget-1 absent filter: %w", rejectionErr)
	}
	walk, walkErr := probeBudgetWalk(ctx, binary, client)
	budgetProof.FixedBudgetWalk = walk
	budgetErr := record(cases, "work_budget", budgetProof, errors.Join(rejectionErr, walkErr))
	return errors.Join(saturationErr, budgetErr)
}

func probeSaturation(ctx context.Context, client *http.Client, base string, seed []row, cases map[string]outcome) error {
	for offset := 0; offset < len(seed); offset += batchRows {
		if _, err := ingest(ctx, client, base, seed[offset:offset+batchRows]); err != nil {
			return err
		}
	}
	first, err := request(ctx, client, http.MethodGet, base+queryPath("", 1, ""), nil)
	if err != nil {
		return err
	}
	n, err := checkRows(first, seed[:1])
	if err != nil {
		return err
	}
	if n != 1 || first.cursor == "" {
		return errors.New("first page did not yield one row and an opaque cursor")
	}

	control, err := request(ctx, client, http.MethodGet, base+queryPath("", 1, budgetFilter), nil)
	if err != nil {
		return err
	}
	if _, err := checkRows(control, nil); err != nil {
		return fmt.Errorf("work budget control: %w", err)
	}
	if control.cursorHeader {
		return errors.New("unexpected cursor for empty work budget control")
	}

	target := queryPath(first.cursor, 1000, "")
	competing := base + queryPath(first.cursor, 1, "")
	appendRow := row{seed[len(seed)/2].TS, "concurrent-backfill"}
	var status int
	var appendErr error
	attempted := false
	evidence, holdErr := holdContinuation(ctx, strings.TrimPrefix(base, "http://"), target, func() (rejection, error) {
		rejected, rejectErr := requireRejection(ctx, client, competing)
		ingestCtx, cancel := context.WithTimeout(ctx, queryDeadline)
		defer cancel()
		attempted = true
		status, appendErr = ingest(ingestCtx, client, base, []row{appendRow})
		return rejected, rejectErr
	})
	saturationErr := record(cases, "one_slot_saturation", evidence, holdErr)
	var ingestErr error
	if attempted {
		if !evidence.BackpressuredAfter {
			appendErr = errors.Join(appendErr, errors.New("continuation did not remain backpressured through ingest"))
		}
		ingestErr = record(cases, "ingest_during_saturation", map[string]int{"http_status": status}, appendErr)
	}
	if status != http.StatusNoContent {
		return errors.Join(saturationErr, ingestErr)
	}
	resumeCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	resumeErr := requireSnapshot(resumeCtx, client, base, first.cursor, seed[1:])
	if resumeErr == nil {
		// Confirm the acknowledged backfill is visible to a fresh snapshot.
		fresh, err := request(resumeCtx, client, http.MethodGet, base+queryPath("", 1, appendRow.Line), nil)
		resumeErr = err
		if err == nil {
			n, err = checkRows(fresh, []row{appendRow})
			resumeErr = err
			if err == nil && (n != 1 || fresh.cursorHeader) {
				resumeErr = errors.New("fresh query did not return exactly the acknowledged backfill")
			}
		}
	}
	resumeErr = record(cases, "cursor_resume", map[string]int{"expected_snapshot_rows": len(seed), "expected_replayed_rows": len(seed) - 1}, resumeErr)
	return errors.Join(saturationErr, ingestErr, resumeErr)
}
