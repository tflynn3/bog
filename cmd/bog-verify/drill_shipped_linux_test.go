package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"bog/internal/backup"
	"bog/internal/client"
	"bog/verify/listener"
)

// TestRestoreDrillShippedServer exercises the separately built server binary
// on a disposable copy of a verified reference image, including restart.
func TestRestoreDrillShippedServer(t *testing.T) {
	binary := os.Getenv("BOG_RESTORE_DRILL_BINARY")
	if binary == "" {
		t.Skip("set BOG_RESTORE_DRILL_BINARY to the shipped retained-restore server")
	}
	binary, err := filepath.Abs(binary)
	check(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)

	bundle, _, point := makeBundle(t)
	restored := filepath.Join(t.TempDir(), "restored")
	check(t, backup.Restore(ctx, bundle, restored))
	reserved, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	check(t, err)
	addr := reserved.Addr().(*net.TCPAddr)
	check(t, reserved.Close())
	opts, err := parseOptions("drill", compareArgs(bundle, "http://"+addr.String())[1:])
	check(t, err)
	current, err := startDrillCandidate(t, ctx, binary, restored, addr)
	check(t, err)

	server, err := client.New(opts.baseURL, nil)
	check(t, err)
	defer server.CloseIdleConnections()
	server.MaxPageBytes = opts.maxPageBytes
	boundary := restartFunc(func(ctx context.Context) error {
		if err := current.checkListener(addr.Port); err != nil {
			return err
		}
		if err := current.stop(); err != nil {
			return err
		}
		current, err = startDrillCandidate(t, ctx, binary, restored, addr)
		return err
	})
	var out bytes.Buffer
	status, err := drillBackup(ctx, opts, server, boundary, &out)
	if err != nil || status != exitOK {
		t.Fatalf("restore drill exit %d: %v\n%s", status, err, &out)
	}
	check(t, current.checkListener(addr.Port))
	check(t, current.stop())
	if strings.Count(out.String(), "rows=4") != 2 || strings.Count(out.String(), formatPoint(point)) != 2 {
		t.Fatalf("unexpected drill result:\n%s", &out)
	}
}

type drillCandidate struct {
	pid  int
	done chan struct{}
	err  error
	log  bytes.Buffer
	stop func() error
}

func startDrillCandidate(t *testing.T, ctx context.Context, binary, dir string, addr *net.TCPAddr) (*drillCandidate, error) {
	t.Helper()
	p := &drillCandidate{done: make(chan struct{})}
	cmd := exec.CommandContext(ctx, binary, "-data-dir", dir, "-addr", addr.String())
	cmd.WaitDelay = time.Second
	cmd.Stdout, cmd.Stderr = &p.log, &p.log
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start restored server: %w", err)
	}
	p.pid = cmd.Process.Pid
	go func() {
		p.err = cmd.Wait()
		close(p.done)
	}()
	p.stop = sync.OnceValue(func() error {
		signalErr := cmd.Process.Signal(syscall.SIGTERM)
		if errors.Is(signalErr, os.ErrProcessDone) {
			signalErr = nil
		}
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()
		select {
		case <-p.done:
		case <-timer.C:
			killErr := cmd.Process.Kill()
			if errors.Is(killErr, os.ErrProcessDone) {
				killErr = nil
			}
			signalErr = errors.Join(signalErr, errors.New("restored server shutdown timed out"), killErr)
			select {
			case <-p.done:
			case <-time.After(2 * time.Second):
				return errors.Join(signalErr, errors.New("restored server did not exit after kill"))
			}
		}
		if err := errors.Join(signalErr, p.err); err != nil {
			return fmt.Errorf("stop restored server: %w\n%s", err, &p.log)
		}
		return nil
	})
	t.Cleanup(func() { check(t, p.stop()) })

	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := p.waitReady(probeCtx, addr); err != nil {
		return nil, fmt.Errorf("wait for restored server: %w", err)
	}
	return p, nil
}

func (p *drillCandidate) checkListener(port int) error {
	owned, err := listener.Owns(p.pid, port)
	if err != nil {
		return err
	}
	if !owned {
		return fmt.Errorf("restored server PID %d does not own port %d", p.pid, port)
	}
	return nil
}

func (p *drillCandidate) waitReady(ctx context.Context, addr *net.TCPAddr) error {
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	probe := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		owned, err := listener.Owns(p.pid, addr.Port)
		if err != nil {
			return err
		}
		if owned {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr.String()+"/ready", nil)
			if err != nil {
				return err
			}
			resp, err := probe.Do(req)
			if err != nil {
				return err
			}
			_, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			if err := errors.Join(readErr, resp.Body.Close()); err != nil {
				return err
			}
			switch resp.StatusCode {
			case http.StatusOK:
				return p.checkListener(addr.Port)
			case http.StatusServiceUnavailable:
				// Retry a pending readiness check within the startup deadline.
			default:
				return fmt.Errorf("readiness returned HTTP %d", resp.StatusCode)
			}
		}
		select {
		case <-p.done:
			return fmt.Errorf("restored server exited during startup (%v)\n%s", p.err, &p.log)
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
