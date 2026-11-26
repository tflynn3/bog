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
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const processTimeout = 5 * time.Second

type child struct {
	cmd    *exec.Cmd
	done   chan struct{}
	result error
	logs   limitedLog
}

type limitedLog struct{ buf bytes.Buffer }

func (l *limitedLog) Write(p []byte) (int, error) {
	_, err := l.buf.Write(p[:min(len(p), 32<<10-l.buf.Len())])
	return len(p), err
}

func spawn(binary, dir string, args ...string) (*child, error) {
	p := &child{cmd: exec.Command(binary, args...), done: make(chan struct{})}
	p.cmd.Dir = dir
	p.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	p.cmd.Stdout, p.cmd.Stderr = &p.logs, &p.logs
	p.cmd.WaitDelay = time.Second
	if err := p.cmd.Start(); err != nil {
		return nil, err
	}
	go func() {
		p.result = p.cmd.Wait()
		close(p.done)
	}()
	return p, nil
}

func (p *child) stop(sig syscall.Signal) error {
	select {
	case <-p.done:
		return fmt.Errorf("candidate exited before %s (%v): %s", sig, p.result, p.logs.buf.String())
	default:
	}
	err := p.cmd.Process.Signal(sig)
	if err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	timer := time.NewTimer(processTimeout)
	defer timer.Stop()
	select {
	case <-p.done:
		var exit *exec.ExitError
		if sig == syscall.SIGKILL && errors.As(p.result, &exit) &&
			exit.Sys().(syscall.WaitStatus).Signal() == sig {
			return nil
		}
		if p.result != nil {
			return fmt.Errorf("stop candidate: %w: %s", p.result, p.logs.buf.String())
		}
		if sig == syscall.SIGKILL {
			return errors.New("candidate exited before SIGKILL")
		}
		return nil
	case <-timer.C:
		return fmt.Errorf("candidate did not exit within %s of %s", processTimeout, sig)
	}
}

func (p *child) reap() error {
	select {
	case <-p.done:
		return nil
	default:
	}
	// Kill the process group in cleanup so an inherited output pipe cannot
	// hold the verifier open. WaitDelay also bounds copy-pipe drainage.
	err := syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		err = nil
	}
	timer := time.NewTimer(processTimeout)
	defer timer.Stop()
	select {
	case <-p.done:
		return err
	case <-timer.C:
		return errors.Join(err, errors.New("timed out reaping candidate"))
	}
}

type server struct {
	binary, dir, addr string
	client            *http.Client
	process           *child
}

func newServer(binary, dir string) (*server, error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return nil, err
	}
	client := &http.Client{
		Timeout:       processTimeout,
		Transport:     &http.Transport{DisableKeepAlives: true},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return &server{binary: binary, dir: dir, addr: addr, client: client}, nil
}

func (s *server) launch() error {
	p, err := spawn(s.binary, s.dir, "-addr", s.addr, "-data-dir", s.dir)
	if err == nil {
		s.process = p
	}
	return err
}

func (s *server) close() error {
	if s.process == nil {
		return nil
	}
	return s.process.reap()
}

func (s *server) start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.launch(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.process.done:
			return s.startupExit()
		default:
		}
		probe, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		_, _, err := s.request(probe, http.MethodGet, "/healthz", nil, http.StatusOK)
		cancel()
		if err == nil {
			select {
			case <-s.process.done:
				return s.startupExit()
			default:
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for /healthz (%w): %v", ctx.Err(), err)
		case <-s.process.done:
			return s.startupExit()
		case <-ticker.C:
		}
	}
}

func (s *server) startupExit() error {
	return fmt.Errorf("candidate exited before health (%v): %s", s.process.result, s.process.logs.buf.String())
}

func (s *server) request(ctx context.Context, method, path string, data []byte, status int) ([]byte, http.Header, error) {
	req, err := http.NewRequestWithContext(ctx, method, "http://"+s.addr+path, bytes.NewReader(data))
	if err != nil {
		return nil, nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	const maxBody = 4 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err := errors.Join(err, resp.Body.Close()); err != nil {
		return nil, nil, err
	}
	if len(body) > maxBody || resp.StatusCode != status {
		return nil, nil, fmt.Errorf("%s %s: HTTP %d, expected %d (response %.200q)", method, strings.SplitN(path, "?", 2)[0], resp.StatusCode, status, body)
	}
	return body, resp.Header, nil
}

func queryPath(cursor string, limit int) string {
	params := url.Values{"start": {epoch.Format(time.RFC3339)}, "end": {epoch.Add(time.Hour).Format(time.RFC3339)}, "limit": {strconv.Itoa(limit)}}
	if cursor == "" {
		params.Set("page", "1")
	} else {
		params.Set("cursor", cursor)
	}
	return "/query?" + params.Encode()
}

func (s *server) page(ctx context.Context, cursor string, limit int) ([]row, string, error) {
	body, hdr, err := s.request(ctx, http.MethodGet, queryPath(cursor, limit), nil, http.StatusOK)
	if err != nil {
		return nil, "", err
	}
	if len(body) != 0 && body[len(body)-1] != '\n' {
		return nil, "", errors.New("query ended with an unterminated NDJSON record")
	}
	var rows []row
	scan := bufio.NewScanner(bytes.NewReader(body))
	scan.Buffer(nil, 4<<20)
	for scan.Scan() {
		var r row
		if err := json.Unmarshal(scan.Bytes(), &r); err != nil {
			return nil, "", err
		}
		rows = append(rows, r)
	}
	next := hdr.Values("Bog-Next-Cursor")
	if len(rows) > limit || len(next) > 1 || (len(next) == 1 && (next[0] == "" || len(rows) == 0)) {
		return nil, "", errors.New("invalid query page size or continuation")
	}
	return rows, hdr.Get("Bog-Next-Cursor"), scan.Err()
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
	_, _, err := s.request(ctx, http.MethodGet, queryPath(cursor, 1), nil, http.StatusBadRequest)
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
		if seen[next] || next == cursor {
			return reconciliation{}, errors.New("query repeated a cursor")
		}
		seen[next] = true
		cursor = next
	}
	return reconciliation{}, errors.New("query exceeded the fixture row/page bound")
}

func (s *server) submit(ctx context.Context, rows []row, suffix string, status int) error {
	payload, err := encodeRows(rows)
	if err != nil {
		return err
	}
	body, _, err := s.request(ctx, http.MethodPost, "/ingest", append(payload, suffix...), status)
	if err == nil && status == http.StatusServiceUnavailable && strings.TrimSpace(string(body)) != "WAL unavailable" {
		err = fmt.Errorf("expected WAL allocation refusal, got %.200q", body)
	}
	return err
}
