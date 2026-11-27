package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"
)

type linuxPausedResponse struct {
	table          string
	server, client string
	stop           func() bool
	closeOnce      func() error
}

func (p *linuxPausedResponse) abort() error {
	p.stop()
	return p.closeOnce()
}

func (p *linuxPausedResponse) pressure(ctx context.Context) (sample socketPressure, err error) {
	err = waitSlowObservation(ctx, 3*time.Second, func(context.Context) (bool, error) {
		data, err := os.ReadFile(p.table)
		if err != nil {
			return false, err
		}
		var found bool
		sample, found, err = readSocketPressure(string(data), p.server, p.client)
		return found, err
	})
	return sample, err
}

func slowDialControl(_, _ string, raw syscall.RawConn) error {
	var err error
	controlErr := raw.Control(func(fd uintptr) {
		// Set these before the handshake, so the advertised window and
		// segment size stay small on loopback. Linger zero sends a reset
		// when the benchmark cancels a request with unread body bytes.
		err = errors.Join(
			syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4096),
			syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_MAXSEG, 512),
			syscall.SetsockoptLinger(int(fd), syscall.SOL_SOCKET, syscall.SO_LINGER, &syscall.Linger{Onoff: 1}),
		)
	})
	return errors.Join(controlErr, err)
}

func openLinuxPaused(ctx context.Context, s service, pid int, path, phase string, want pageFraming) (_ pausedResponse, err error) {
	start := time.Now()
	attempt := queryAttempt{Admission: "unknown", Phase: "headers", Budget: 5 * time.Second}
	defer func() {
		attempt.Elapsed = time.Since(start)
		recordQuery(s.queryPhases, phase, attempt, err)
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.base+path, nil)
	if err != nil {
		return nil, err
	}
	req.Close = true
	dialer := net.Dialer{Timeout: attempt.Budget, Control: slowDialControl}
	connection, err := dialer.DialContext(ctx, "tcp4", req.URL.Host)
	if err != nil {
		return nil, err
	}
	p := &linuxPausedResponse{
		table:     fmt.Sprintf("/proc/%d/net/tcp", pid),
		server:    fmt.Sprintf("0100007F:%04X", connection.RemoteAddr().(*net.TCPAddr).Port),
		client:    fmt.Sprintf("0100007F:%04X", connection.LocalAddr().(*net.TCPAddr).Port),
		closeOnce: sync.OnceValue(connection.Close),
	}
	// abort retrieves the close result even when the context closed the socket first.
	p.stop = context.AfterFunc(ctx, func() { p.closeOnce() })
	defer func() {
		if err != nil {
			err = errors.Join(err, p.abort())
		}
	}()
	deadline := start.Add(attempt.Budget)
	if end, ok := ctx.Deadline(); ok {
		if end.Before(deadline) {
			deadline = end
		}
	}
	if err := connection.SetDeadline(deadline); err != nil {
		return nil, err
	}
	if err := req.Write(connection); err != nil {
		return nil, err
	}
	reader := bufio.NewReader(io.LimitReader(connection, 64<<10))
	res, err := http.ReadResponse(reader, req)
	if err != nil {
		return nil, err
	}
	attempt.observeHeaders(res, time.Since(start))
	if attempt.Admission != "ndjson_response" {
		body, readErr := io.ReadAll(io.LimitReader(res.Body, 4097))
		attempt.observeRejection(body)
		attempt.BodyBytes = len(body)
		return nil, errors.Join(fmt.Errorf("paused query HTTP %d (%s)", res.StatusCode, attempt.Admission), readErr)
	}
	// The connection owns the raw response body. Keep it unread after
	// headers and release it by resetting the connection, including on
	// error. Reading or draining it here would reopen the receive window.
	cursors := res.Header.Values("Bog-Next-Cursor")
	if res.ContentLength != int64(want.bytes) || len(cursors) != 1 || cursors[0] != want.next {
		return nil, errors.New("paused query length or continuation differs from the complete reference page")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return p, nil
}
