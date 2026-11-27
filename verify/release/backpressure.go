package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
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

// withBackpressuredContinuation owns the connection and response through use.
// An earlier I/O deadline allows admission to bound startup while keeping
// the socket open for its separate saturation checks.
func withBackpressuredContinuation(ctx context.Context, deadline time.Time, target string, use func(*net.TCPConn, *http.Response) error) (err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	req.Close = true
	dialer := net.Dialer{Deadline: deadline, Control: func(_, _ string, socket syscall.RawConn) error {
		var optionErr error
		controlErr := socket.Control(func(fd uintptr) {
			optionErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, receiveWindow)
		})
		return errors.Join(controlErr, optionErr)
	}}
	socket, err := dialer.DialContext(ctx, "tcp4", req.URL.Host)
	if err != nil {
		return err
	}
	conn := socket.(*net.TCPConn)

	// Raw HTTP reads and writes do not observe the request context.
	var closeErr error
	closed := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		closeErr = conn.Close()
		close(closed)
	})
	var res *http.Response
	defer func() {
		if stop() {
			closeErr = conn.Close()
		} else {
			<-closed
		}
		err = errors.Join(err, closeErr, ctx.Err())
		// Close the socket before the body, whose Close may drain unread bytes.
		if res != nil {
			if bodyErr := res.Body.Close(); !errors.Is(bodyErr, net.ErrClosed) {
				err = errors.Join(err, bodyErr)
			}
		}
	}()
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	if err := req.Write(conn); err != nil {
		return err
	}
	res, err = http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		return err
	}
	return use(conn, res)
}

func holdContinuation(ctx context.Context, addr, path string, check func() (rejection, error)) (evidence saturationEvidence, err error) {
	startup, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	deadline, _ := startup.Deadline()
	err = withBackpressuredContinuation(ctx, deadline, "http://"+addr+path, func(conn *net.TCPConn, res *http.Response) error {
		if res.StatusCode != http.StatusOK || !strings.HasPrefix(res.Header.Get("Content-Type"), "application/x-ndjson") || res.ContentLength == 0 {
			return fmt.Errorf("slow continuation did not start an NDJSON response: HTTP %d", res.StatusCode)
		}
		evidence.ResponseStarted = true
		server := conn.RemoteAddr().(*net.TCPAddr).Port
		peer := conn.LocalAddr().(*net.TCPAddr).Port
		for {
			blocked, err := zeroWindow(server, peer)
			if err != nil {
				return err
			}
			if blocked {
				evidence.Backpressured = true
				break
			}
			select {
			case <-startup.Done():
				return fmt.Errorf("slow continuation never reached a zero receive window: %w", startup.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
		rejected, checkErr := check()
		evidence.rejection = rejected
		blocked, socketErr := zeroWindow(server, peer)
		evidence.BackpressuredAfter = blocked
		if socketErr == nil && !blocked {
			socketErr = errors.New("continuation left zero-window backpressure during the checks")
		}
		return errors.Join(checkErr, socketErr)
	})
	return evidence, err
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
