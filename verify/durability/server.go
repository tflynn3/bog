package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
)

const timeout = 8 * time.Second

var probeClient = http.Client{
	Timeout:       3 * time.Second,
	Transport:     &http.Transport{DisableKeepAlives: true},
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

type fixtureServer struct {
	binary, dir, addr string
	process           *child
}

type child struct {
	cmd  *exec.Cmd
	log  logTail
	done chan struct{}
	err  error // Published with log when done closes.
}

// Only Cmd's output copier writes the log. Readers wait for done to close.
type logTail []byte

func (l *logTail) Write(p []byte) (int, error) {
	const limit = 16 << 10
	n := len(p)
	if n >= limit {
		*l = append((*l)[:0], p[n-limit:]...)
	} else {
		if excess := len(*l) + n - limit; excess > 0 {
			*l = (*l)[:copy(*l, (*l)[excess:])]
		}
		*l = append(*l, p...)
	}
	return n, nil
}

func (l logTail) String() string { return string(l) }

func (s *fixtureServer) launch() error {
	if s.process != nil {
		select {
		case <-s.process.done:
			s.process = nil
		default:
			return errors.New("previous candidate is still running")
		}
	}
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return err
	}
	s.addr = l.Addr().String()
	if err := l.Close(); err != nil {
		return err
	}
	p := &child{cmd: exec.Command(s.binary, "-addr", s.addr, "-data-dir", s.dir), done: make(chan struct{})}
	p.cmd.Dir, p.cmd.WaitDelay = s.dir, time.Second
	p.cmd.Stdout, p.cmd.Stderr = &p.log, &p.log
	if err := p.cmd.Start(); err != nil {
		return err
	}
	s.process = p
	go func() {
		p.err = p.cmd.Wait()
		if len(p.log) != 0 {
			log.Printf("%s candidate output (pid %d):\n%s", filepath.Base(s.dir), p.cmd.Process.Pid, p.log)
		}
		close(p.done)
	}()
	return nil
}

func (s *fixtureServer) start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.launch(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	tick := time.NewTicker(15 * time.Millisecond)
	defer tick.Stop()
	var probeErr error
	for {
		select {
		case <-s.process.done:
			return fmt.Errorf("candidate exited during startup: %v", s.process.err)
		case <-ctx.Done():
			return fmt.Errorf("waiting for health: %w (last probe: %v)", ctx.Err(), probeErr)
		case <-tick.C:
			_, _, probeErr = s.want(ctx, "/healthz", nil, http.StatusOK)
			if probeErr == nil {
				select {
				case <-s.process.done:
					continue
				default:
					return nil
				}
			}
		}
	}
}

func (s *fixtureServer) stop() error {
	if s.process == nil {
		return nil
	}
	select {
	case <-s.process.done:
		return nil
	default:
		return s.signal(syscall.SIGKILL)
	}
}

func (s *fixtureServer) signal(sig syscall.Signal) error {
	p := s.process
	if p == nil {
		return errors.New("no candidate process to signal")
	}
	signalErr := p.cmd.Process.Signal(sig)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.done:
		if sig == syscall.SIGKILL && p.cmd.ProcessState.Sys().(syscall.WaitStatus).Signal() == sig {
			return signalErr
		}
		if sig == syscall.SIGTERM && p.err == nil && signalErr == nil {
			return nil
		}
		return errors.Join(signalErr, fmt.Errorf("candidate did not stop as expected after %v (exit %v)", sig, p.err))
	case <-timer.C:
		killErr := p.cmd.Process.Kill()
		select {
		case <-p.done:
			return errors.Join(fmt.Errorf("candidate did not exit after %v", sig), signalErr, killErr)
		case <-time.After(timeout):
			return errors.Join(errors.New("candidate could not be reaped"), signalErr, killErr)
		}
	}
}

func (s *fixtureServer) want(ctx context.Context, path string, body []byte, status int) ([]byte, http.Header, error) {
	method := http.MethodGet
	if body != nil {
		method = http.MethodPost
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://"+s.addr+path, bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	res, err := probeClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	data, err := readBody(res)
	if err == nil && res.StatusCode != status {
		err = fmt.Errorf("%s %s: expected HTTP %d, got %s (%.200s)", method, path, status, res.Status, data)
	}
	return data, res.Header, err
}

func readBody(res *http.Response) ([]byte, error) {
	const maxBody = 4 << 20
	body, err := io.ReadAll(io.LimitReader(res.Body, maxBody+1))
	err = errors.Join(err, res.Body.Close())
	if len(body) > maxBody {
		err = errors.Join(err, errors.New("oversized HTTP response"))
	}
	return body, err
}

type row struct {
	Time time.Time `json:"ts"`
	Line string    `json:"line"`
}

func entry(second int, line string) row {
	return row{time.Date(2025, 1, 1, 0, 0, second, 0, time.UTC), line}
}

func payload(rows []row) ([]byte, error) {
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			return nil, err
		}
	}
	return body.Bytes(), nil
}

func (s *fixtureServer) ingest(ctx context.Context, rows ...row) error {
	body, err := payload(rows)
	if err != nil {
		return err
	}
	_, _, err = s.want(ctx, "/ingest", body, http.StatusNoContent)
	return err
}

func (s *fixtureServer) page(ctx context.Context, cursor string, status int) ([]byte, string, error) {
	q := url.Values{"start": {"2025-01-01T00:00:00Z"}, "end": {"2025-01-02T00:00:00Z"}, "limit": {"1"}}
	if cursor == "" {
		q.Set("page", "1")
	} else {
		q.Set("cursor", cursor)
	}
	body, headers, err := s.want(ctx, "/query?"+q.Encode(), nil, status)
	return body, headers.Get("Bog-Next-Cursor"), err
}

func (s *fixtureServer) cursor(ctx context.Context) (string, error) {
	body, token, err := s.page(ctx, "", http.StatusOK)
	if err == nil && (token == "" || len(body) == 0) {
		err = errors.New("expected a nonempty page with a continuation cursor")
	}
	return token, err
}

func (s *fixtureServer) walk(ctx context.Context, cursor string, choices ...[]row) error {
	maxRows := 0
	for _, expected := range choices {
		maxRows = max(maxRows, len(expected))
	}
	var got []row
	seen := map[string]bool{}
	for range maxRows + 1 {
		body, next, err := s.page(ctx, cursor, http.StatusOK)
		if err != nil {
			return err
		}
		if len(body) == 0 && cursor != "" {
			return errors.New("empty continuation page")
		}
		if len(body) != 0 {
			var r row
			if !bytes.HasSuffix(body, []byte("\n")) {
				return errors.New("unterminated NDJSON query row")
			}
			if err := json.Unmarshal(body, &r); err != nil {
				return fmt.Errorf("query page must contain one NDJSON row: %w", err)
			}
			got = append(got, r)
		}
		if next == "" {
			for _, expected := range choices {
				if slices.Equal(got, expected) {
					return nil
				}
			}
			var mismatches []string
			for _, expected := range choices {
				i := 0
				for i < min(len(got), len(expected)) && got[i] == expected[i] {
					i++
				}
				mismatches = append(mismatches, fmt.Sprintf("matched first %d of %d expected rows", i, len(expected)))
			}
			return fmt.Errorf("cursor walk differs from oracle (got %d rows, %s)", len(got), strings.Join(mismatches, ", "))
		}
		if seen[next] || len(body) == 0 {
			return errors.New("cursor walk failed to advance")
		}
		seen[next], cursor = true, next
	}
	return errors.New("cursor walk exceeded oracle length")
}

func (s *fixtureServer) restart(ctx context.Context, cursor string) error {
	if err := s.start(ctx); err != nil {
		return err
	}
	body, next, err := s.page(ctx, cursor, http.StatusBadRequest)
	if err == nil && (next != "" || strings.TrimSpace(string(body)) != "invalid cursor") {
		return errors.New("stale cursor did not receive a clean invalid-cursor response")
	}
	return err
}

// A 100 Continue confirms that the handler is waiting for the request body.
// Send the requested prefix and kill before reading the final response.
func (s *fixtureServer) interrupt(ctx context.Context, batch []byte, sendBytes int) (status int, err error) {
	dial := net.Dialer{Timeout: timeout}
	conn, err := dial.DialContext(ctx, "tcp", s.addr)
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	deadline := time.Now().Add(timeout)
	if end, ok := ctx.Deadline(); ok && end.Before(deadline) {
		deadline = end
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return 0, err
	}
	if _, err := fmt.Fprintf(conn, "POST /ingest HTTP/1.1\r\nHost: %s\r\nContent-Length: %d\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n", s.addr, len(batch)); err != nil {
		return 0, err
	}
	reader := bufio.NewReader(conn)
	req := &http.Request{Method: http.MethodPost}
	interim, err := http.ReadResponse(reader, req)
	if err != nil {
		return 0, err
	}
	if _, err := readBody(interim); err != nil {
		return 0, err
	}
	if interim.StatusCode != http.StatusContinue {
		return 0, fmt.Errorf("expected HTTP 100 before SIGKILL, got %s", interim.Status)
	}
	if _, err := conn.Write(batch[:sendBytes]); err != nil {
		return 0, err
	}
	if err := s.signal(syscall.SIGKILL); err != nil {
		return 0, err
	}
	res, err := http.ReadResponse(reader, req)
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if _, err := readBody(res); err != nil {
		return 0, err
	}
	return res.StatusCode, nil
}

func syncWrite(path string, data []byte, flags int) error {
	f, err := os.OpenFile(path, os.O_WRONLY|flags, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	return errors.Join(err, f.Close())
}
