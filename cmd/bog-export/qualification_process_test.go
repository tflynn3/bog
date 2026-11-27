//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

type qualificationIssuer struct {
	base     string
	port     int
	cmd      *exec.Cmd
	done     chan struct{}
	waitErr  error // Both exit errors are published by closing done.
	groupErr error
	reaped   bool
	owns     func(int, int) (bool, error)
	log      bytes.Buffer
}

func startQualificationProcess(t *testing.T, ctx context.Context, cmd *exec.Cmd, addr *net.TCPAddr, owns func(int, int) (bool, error), handoff *os.File, observeExit func(int) error) *qualificationIssuer {
	t.Helper()
	p := &qualificationIssuer{base: "http://" + addr.String(), port: addr.Port, done: make(chan struct{}), cmd: cmd, owns: owns}
	// Lifetime is owned by cleanup. A canceled workload must still have
	// time to signal the issuer, drain it, and collect its exit status.
	p.cmd.Stdout, p.cmd.Stderr = &p.log, &p.log
	p.cmd.WaitDelay = time.Second
	startErr := p.cmd.Start()
	var closeErr error
	if handoff != nil {
		closeErr = handoff.Close()
	}
	if startErr != nil {
		t.Fatalf("start issuer: %v", errors.Join(startErr, closeErr))
	}
	go func() {
		if observeExit != nil {
			p.groupErr = observeExit(p.cmd.Process.Pid)
		}
		p.waitErr = p.cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(func() {
		if err := p.stop(syscall.SIGTERM); err != nil {
			t.Error(err)
		}
		if p.reaped && p.log.Len() != 0 {
			t.Logf("issuer stderr/stdout:\n%s", &p.log)
		}
	})

	if closeErr != nil {
		t.Fatalf("close inherited listener in parent: %v", closeErr)
	}
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := p.waitForHealth(probeCtx); err != nil {
		t.Fatalf("issuer startup: %v", err)
	}
	return p
}

func (p *qualificationIssuer) waitForHealth(ctx context.Context) error {
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	probe := &http.Client{Transport: transport, Timeout: time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		owned, err := p.ownsListener()
		if err != nil {
			return fmt.Errorf("check issuer listener: %w", err)
		}
		if owned {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+"/healthz", nil)
			if err != nil {
				return err
			}
			resp, err := probe.Do(req)
			if err != nil {
				return fmt.Errorf("health request: %w", err)
			}
			size, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, 4097))
			if err := errors.Join(readErr, resp.Body.Close()); err != nil {
				return err
			}
			if size > 4096 {
				return errors.New("health response exceeded 4096 bytes")
			}
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("health probe returned HTTP %d", resp.StatusCode)
			}
			owned, err := p.ownsListener()
			if err != nil {
				return err
			}
			if !owned {
				return errors.New("issuer lost its listener during health probe")
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.done:
			return fmt.Errorf("issuer exited before health probe (wait: %v)", p.waitErr)
		case <-ticker.C:
		}
	}
}

func (p *qualificationIssuer) stop(sig syscall.Signal) error {
	if p.reaped {
		return nil
	}
	signalErr := p.cmd.Process.Signal(sig)
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case <-p.done:
		p.reaped = true
		if sig == syscall.SIGKILL {
			var exit *exec.ExitError
			if errors.As(p.waitErr, &exit) && exit.ProcessState.Sys().(syscall.WaitStatus).Signal() == sig {
				return errors.Join(signalErr, p.groupErr)
			}
			return errors.Join(signalErr, p.groupErr, fmt.Errorf("issuer did not exit on SIGKILL (wait: %v)", p.waitErr))
		}
		// The fixture uses the default SIGTERM action. The shipped
		// issuer handles it and exits normally.
		if p.owns == nil {
			var exit *exec.ExitError
			if errors.As(p.waitErr, &exit) && exit.ProcessState.Sys().(syscall.WaitStatus).Signal() == sig {
				return errors.Join(signalErr, p.groupErr)
			}
		}
		return errors.Join(signalErr, p.groupErr, p.waitErr)
	case <-timer.C:
		killErr := p.cmd.Process.Kill()
		if errors.Is(killErr, os.ErrProcessDone) {
			killErr = nil
		}
		err := errors.Join(errors.New("issuer shutdown timed out"), signalErr, killErr)
		timer.Reset(3 * time.Second)
		select {
		case <-p.done:
			p.reaped = true
			return errors.Join(err, p.groupErr)
		case <-timer.C:
			return errors.Join(err, errors.New("timed out reaping issuer"))
		}
	}
}

func (p *qualificationIssuer) proxy(t *testing.T) http.Handler {
	t.Helper()
	target, err := url.Parse(p.base)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	transport := &http.Transport{}
	proxy.Transport = transport
	t.Cleanup(transport.CloseIdleConnections)
	return proxy
}

func (p *qualificationIssuer) ownsListener() (bool, error) {
	select {
	case <-p.done:
		return false, fmt.Errorf("issuer exited: %v", p.waitErr)
	default:
	}
	if p.owns == nil {
		return true, nil // The fixture inherits a private, already bound socket.
	}
	return p.owns(p.cmd.Process.Pid, p.port)
}

func (p *qualificationIssuer) requireListener() error {
	owned, err := p.ownsListener()
	if err != nil {
		return err
	}
	if !owned {
		return errors.New("issuer no longer owns its loopback listener")
	}
	return nil
}
