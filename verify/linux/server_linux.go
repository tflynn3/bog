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
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

const processTimeout = 5 * time.Second

type child struct {
	cmd        *exec.Cmd
	done       chan struct{}
	result     error
	observeErr error
	logs       limitedLog

	mu       sync.Mutex
	cleaned  bool
	groupErr error
}

type limitedLog struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	truncated bool
}

func (l *limitedLog) snapshot() (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String(), l.truncated
}

func (l *limitedLog) text() string {
	log, _ := l.snapshot()
	return log
}

func (l *limitedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := min(len(p), 32<<10-l.buf.Len())
	l.truncated = l.truncated || n < len(p)
	_, err := l.buf.Write(p[:n])
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
	go p.wait()
	return p, nil
}

func (p *child) wait() {
	// Observe exit without reaping the leader. Its PID still identifies this
	// group while we kill descendants, even if they inherited output pipes.
	const pid = 1       // Linux waitid P_PID
	var info [16]uint64 // aligned, 128-byte Linux siginfo_t
	for {
		_, _, errno := syscall.Syscall6(syscall.SYS_WAITID, pid, uintptr(p.cmd.Process.Pid),
			uintptr(unsafe.Pointer(&info)), syscall.WEXITED|syscall.WNOWAIT, 0, 0)
		if errno == syscall.EINTR {
			continue
		}
		if errno != 0 {
			p.observeErr = fmt.Errorf("observe candidate exit: %w", errno)
		}
		break
	}
	p.cleanGroup() // reap returns the cached group signaling error.
	p.result = p.cmd.Wait()
	close(p.done)
}

func (p *child) signalGroup(sig syscall.Signal) error {
	err := syscall.Kill(-p.cmd.Process.Pid, sig)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("signal candidate group with %s: %w", sig, err)
	}
	return nil
}

func (p *child) cleanGroup() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.cleaned {
		p.groupErr = p.signalGroup(syscall.SIGKILL)
		p.cleaned = true
	}
	return p.groupErr
}

func (p *child) stop(sig syscall.Signal) (err error) {
	defer func() { err = errors.Join(err, p.reap()) }()
	p.mu.Lock()
	if p.cleaned {
		p.mu.Unlock()
		return fmt.Errorf("candidate exited before %s", sig)
	}
	err = p.signalGroup(sig)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	timer := time.NewTimer(processTimeout)
	defer timer.Stop()
	select {
	case <-p.done:
		var exit *exec.ExitError
		if sig == syscall.SIGKILL && errors.As(p.result, &exit) &&
			exit.ProcessState.Sys().(syscall.WaitStatus).Signal() == sig {
			return nil
		}
		if p.result != nil {
			return fmt.Errorf("stop candidate: %w: %s", p.result, p.logs.text())
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
	err := p.cleanGroup()
	timer := time.NewTimer(processTimeout)
	defer timer.Stop()
	select {
	case <-p.done:
		var exit *exec.ExitError
		if errors.As(p.result, &exit) {
			// A signal or nonzero exit is interpreted by the caller of stop
			// or by the startup probe. Always report other wait failures.
			return errors.Join(err, p.observeErr)
		}
		return errors.Join(err, p.observeErr, p.result)
	case <-timer.C:
		return errors.Join(err, errors.New("timed out reaping candidate"))
	}
}

type server struct {
	binary, dir, addr string
	client            *http.Client
	process           *child
	diagnostics       *caseDiagnostics
	current           *processEvidence
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

func (s *server) launch() error { return s.launchFor("direct") }

func (s *server) launchFor(probe string) error {
	if err := s.close(); err != nil {
		return fmt.Errorf("clean up previous candidate: %w", err)
	}
	if s.diagnostics != nil {
		s.current = &processEvidence{StartupProbe: probe, start: time.Now()}
		s.diagnostics.Candidates = append(s.diagnostics.Candidates, s.current)
	}
	p, err := spawn(s.binary, s.dir, "-addr", s.addr, "-data-dir", s.dir)
	if err == nil {
		s.process = p
	} else {
		s.process = nil
		s.recordStartupError(err)
	}
	return err
}

func (s *server) close() error {
	if s.process == nil {
		return nil
	}
	err := s.process.reap()
	if e := s.current; e != nil {
		p := s.process
		e.Log, e.LogTruncated = p.logs.snapshot()
		select {
		case <-p.done:
			e.Reaped = true
			code := p.cmd.ProcessState.ExitCode()
			if ws, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
				e.Signal = ws.Signal().String()
			} else if code >= 0 {
				e.ExitCode = &code
			}
			if rss := peakRSS(p); rss > 0 {
				e.PeakRSSKiB = &rss
			}
			if waitErr := errors.Join(p.result, p.observeErr); waitErr != nil {
				e.WaitError = diagnosticError(waitErr)
			}
		default:
		}
	}
	return err
}

func (s *server) start(ctx context.Context) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.launchFor("/healthz"); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			s.recordStartupError(err)
			err = errors.Join(err, s.close())
		}
	}()
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
			if err := ctx.Err(); err != nil {
				return err
			}
			select {
			case <-s.process.done:
				return s.startupExit()
			default:
				if s.current != nil {
					elapsed := time.Since(s.current.start)
					s.current.Health = &elapsed
				}
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

func (s *server) recordStartupError(err error) {
	if err != nil && s.current != nil {
		s.current.StartupError = diagnosticError(err)
	}
}

func (s *server) startupExit() error {
	return fmt.Errorf("candidate exited before health (%v): %s", s.process.result, s.process.logs.text())
}

func (s *server) request(ctx context.Context, method, path string, data []byte, status int) (_ []byte, _ http.Header, err error) {
	kind := "fresh_page"
	switch {
	case !strings.HasPrefix(path, "/query?"):
		kind = ""
	case status == http.StatusBadRequest:
		kind = "stale_cursor"
	case strings.Contains(path, "?cursor=") || strings.Contains(path, "&cursor="):
		kind = "continuation"
	}
	start := time.Now()
	q := queryResponse{Admission: "unknown", Phase: "headers"}
	if s.diagnostics != nil && kind != "" {
		q.Lifetime = len(s.diagnostics.Candidates)
		defer func() {
			q.Elapsed = time.Since(start)
			s.diagnostics.observeQuery(kind, q, err)
		}()
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://"+s.addr+path, bytes.NewReader(data))
	if err != nil {
		return nil, nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	q.observeHeaders(resp, time.Since(start))
	const maxBody = 4 << 20
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	q.Bytes = len(body)
	if readErr == nil && len(body) <= maxBody {
		elapsed := time.Since(start)
		q.Body = &elapsed
		q.observeRejection(body)
		q.Phase = "close"
	}
	if err := errors.Join(readErr, resp.Body.Close()); err != nil {
		return nil, nil, err
	}
	if len(body) > maxBody {
		return nil, nil, errors.New("response exceeds 4 MiB")
	}
	q.Phase = "validation"
	if resp.StatusCode != status {
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
