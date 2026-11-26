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
	"strconv"
	"strings"
	"syscall"
	"time"
)

type saturationEvidence struct {
	rejection
	ResponseStarted    bool `json:"response_started"`
	Backpressured      bool `json:"backpressured_before"`
	BackpressuredAfter bool `json:"backpressured_after"`
}

func holdContinuation(ctx context.Context, addr, path string, check func() (rejection, error)) (evidence saturationEvidence, err error) {
	startup, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	dialer := net.Dialer{Control: func(_, _ string, socket syscall.RawConn) error {
		var optionErr error
		controlErr := socket.Control(func(fd uintptr) {
			optionErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, receiveWindow)
		})
		return errors.Join(controlErr, optionErr)
	}}
	conn, err := dialer.DialContext(startup, "tcp4", addr)
	if err != nil {
		return evidence, err
	}
	var body io.ReadCloser
	defer func() {
		err = errors.Join(err, conn.Close())
		if body != nil {
			if closeErr := body.Close(); !errors.Is(closeErr, net.ErrClosed) {
				err = errors.Join(err, closeErr)
			}
		}
	}()
	deadline, _ := startup.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return evidence, err
	}
	req, err := http.NewRequestWithContext(startup, http.MethodGet, "http://"+addr+path, nil)
	if err != nil {
		return evidence, err
	}
	req.Close = true
	if err := req.Write(conn); err != nil {
		return evidence, err
	}
	res, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		return evidence, err
	}
	// Close the socket before closing the body so Close cannot drain it.
	body = res.Body
	if res.StatusCode != http.StatusOK || !strings.HasPrefix(res.Header.Get("Content-Type"), "application/x-ndjson") || res.ContentLength == 0 {
		return evidence, fmt.Errorf("slow continuation did not start an NDJSON response: HTTP %d", res.StatusCode)
	}
	evidence.ResponseStarted = true
	server := conn.RemoteAddr().(*net.TCPAddr).Port
	peer := conn.LocalAddr().(*net.TCPAddr).Port
	for {
		blocked, err := zeroWindow(server, peer)
		if err != nil {
			return evidence, err
		}
		if blocked {
			evidence.Backpressured = true
			break
		}
		select {
		case <-startup.Done():
			return evidence, fmt.Errorf("slow continuation never reached a zero receive window: %w", startup.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	evidence.rejection, err = check()
	blocked, socketErr := zeroWindow(server, peer)
	evidence.BackpressuredAfter = blocked
	if socketErr == nil && !blocked {
		socketErr = errors.New("continuation left zero-window backpressure during the checks")
	}
	return evidence, errors.Join(err, socketErr)
}

// Linux publishes the TCP persist timer as 04 while the peer advertises a zero
// window and the sender still has queued bytes. Match both ends of our socket.
func zeroWindow(server, peer int) (bool, error) {
	data, err := os.ReadFile("/proc/net/tcp")
	if err != nil {
		return false, err
	}
	return hasZeroWindow(string(data), server, peer)
}

func hasZeroWindow(table string, server, peer int) (bool, error) {
	local := fmt.Sprintf("0100007F:%04X", server)
	remote := fmt.Sprintf("0100007F:%04X", peer)
	for line := range strings.SplitSeq(table, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[1] != local || fields[2] != remote {
			continue
		}
		if len(fields) < 6 {
			return false, errors.New("incomplete TCP socket information")
		}
		queue, _, queueOK := strings.Cut(fields[4], ":")
		timer, _, timerOK := strings.Cut(fields[5], ":")
		tx, err := strconv.ParseUint(queue, 16, 32)
		if err != nil || !queueOK || !timerOK {
			return false, errors.New("invalid TCP socket information")
		}
		return fields[3] == "01" && timer == "04" && tx > 0, nil
	}
	return false, nil
}
