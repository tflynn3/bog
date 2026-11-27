package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

const (
	candidateStartupTimeout = 5 * time.Second
	candidateStopTimeout    = 3 * time.Second
	candidatePollInterval   = 20 * time.Millisecond
	// Bog handles SIGTERM for graceful shutdown. Use a distinct fatal
	// signal for this fixture so normal exit and SIGKILL can never pass
	// as the result of the verifier's stop request.
	candidateStopSignal = syscall.SIGHUP
)

// The observer reserves the leader PID through group cleanup. The mutex
// serializes group signals with observation and prevents signaling a reused
// PID once the observer proceeds to reap the leader.
type candidateProcess struct {
	process  *os.Process
	done     chan struct{}
	mu       sync.Mutex
	observed bool
	stopSent bool
	result   error
}

func startCandidate(ctx context.Context, binary string, args []string, cancelProbes context.CancelCauseFunc) (*candidateProcess, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := candidatePlatform(); err != nil {
		return nil, err
	}
	cmd := exec.Command(binary, args...)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start candidate: %w", err)
	}
	candidate := &candidateProcess{process: cmd.Process, done: make(chan struct{})}
	go candidate.observe(cmd, cancelProbes)
	return candidate, nil
}

func (p *candidateProcess) observe(cmd *exec.Cmd, cancelProbes context.CancelCauseFunc) {
	_, observeErr := candidateExited(p.process.Pid, false)
	p.mu.Lock()
	p.observed = true
	stopSent := p.stopSent
	// Kill any descendants even when the leader exited on its own.
	groupErr := killCandidateGroup(p.process.Pid)
	p.mu.Unlock()

	waitErr := cmd.Wait()
	p.result = errors.Join(candidateExitError(stopSent, waitErr), observeErr, groupErr)
	cancelProbes(p.result)
	close(p.done)
}

func candidateExitError(stopSent bool, waitErr error) error {
	var exit *exec.ExitError
	if errors.As(waitErr, &exit) {
		if status, ok := exit.Sys().(syscall.WaitStatus); ok && stopSent && status.Signaled() && status.Signal() == candidateStopSignal {
			return nil
		}
	}
	if waitErr == nil {
		return errors.New("candidate exited unexpectedly with status 0")
	}
	return fmt.Errorf("candidate exited unexpectedly: %w", waitErr)
}

func (p *candidateProcess) requestStop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.observed {
		return nil
	}
	// An exited leader may still have a PID and a process group. Check its
	// wait status directly rather than inferring liveness from kill(2).
	exited, err := candidateExited(p.process.Pid, true)
	if err != nil || exited {
		return err
	}
	if err := p.process.Signal(candidateStopSignal); err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			return nil // The observer classifies the actual wait status.
		}
		return fmt.Errorf("signal candidate for teardown: %w", err)
	}
	p.stopSent = true
	return nil
}

func killCandidateGroup(pid int) error {
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("kill candidate process group: %w", err)
	}
	return nil
}

func (p *candidateProcess) forceStop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.observed {
		return nil // The observer already attempted group cleanup.
	}
	return killCandidateGroup(p.process.Pid)
}

func (p *candidateProcess) stop(timeout time.Duration) error {
	stopErr := p.requestStop()
	if stopErr != nil {
		stopErr = errors.Join(stopErr, p.forceStop())
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.done:
		return errors.Join(p.result, stopErr)
	case <-timer.C:
	}
	// A forced kill guarantees neither an expected exit nor a successful
	// teardown. Still give the observer time to collect the wait status.
	stopErr = errors.Join(stopErr, errors.New("candidate did not exit within teardown deadline"), p.forceStop())
	timer.Reset(timeout)
	select {
	case <-p.done:
		return errors.Join(p.result, stopErr)
	case <-timer.C:
		return errors.Join(stopErr, errors.New("timed out reaping candidate"))
	}
}

// Check the kernel state even if the observer has yet to publish its result.
func (p *candidateProcess) checkRunning() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.observed {
		exited, err := candidateExited(p.process.Pid, true)
		if err != nil {
			return err
		}
		if !exited {
			return nil
		}
	}
	return errors.New("candidate exited before probe completion")
}

func (p *candidateProcess) requireListener(port int) error {
	listening, err := candidateOwnsListener(p.process.Pid, port)
	if err != nil {
		return err
	}
	if !listening {
		return errors.New("candidate no longer owns the release listener")
	}
	return p.checkRunning()
}

func (p *candidateProcess) awaitHealth(ctx context.Context, client *http.Client, base string, port int) error {
	startup, cancel := context.WithTimeout(ctx, candidateStartupTimeout)
	defer cancel()
	ticker := time.NewTicker(candidatePollInterval)
	defer ticker.Stop()
	for {
		if err := startup.Err(); err != nil {
			return fmt.Errorf("startup health: %w", context.Cause(startup))
		}
		owned, err := candidateOwnsListener(p.process.Pid, port)
		if err != nil {
			return fmt.Errorf("startup listener: %w", err)
		}
		if owned {
			if err := p.checkRunning(); err != nil {
				return err
			}
			res, err := request(startup, client, http.MethodGet, base+"/healthz", nil)
			if err != nil {
				return fmt.Errorf("startup health: %w", err)
			}
			if res.status != http.StatusOK {
				return fmt.Errorf("startup health returned HTTP %d", res.status)
			}
			return p.requireListener(port)
		}
		select {
		case <-startup.Done():
			return fmt.Errorf("startup health waiting for candidate listener: %w", context.Cause(startup))
		case <-ticker.C:
		}
	}
}

func withFixtureRoot(check func(string) error) (err error) {
	dir, err := os.MkdirTemp("", "bog-release-")
	if err != nil {
		return fmt.Errorf("create release fixture root: %w", err)
	}
	defer func() { err = addFixtureCleanup(err, dir, os.RemoveAll) }()
	return check(dir)
}

func addFixtureCleanup(primary error, dir string, remove func(string) error) error {
	if err := remove(dir); err != nil {
		return errors.Join(primary, fmt.Errorf("remove release fixture root %q: %w", dir, err))
	}
	return primary
}

func serve(ctx context.Context, binary string, client *http.Client, flags []string, check func(context.Context, string) error) error {
	return withFixtureRoot(func(dir string) error {
		return serveDirectory(ctx, binary, dir, client, flags, check)
	})
}

func serveDirectory(ctx context.Context, binary, dir string, client *http.Client, flags []string, check func(context.Context, string) error) (err error) {
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return fmt.Errorf("reserve release listener: %w", err)
	}
	addr := listener.Addr().(*net.TCPAddr)
	if err := listener.Close(); err != nil {
		return fmt.Errorf("release reserved listener: %w", err)
	}
	probes, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	p, err := startCandidate(probes, binary, append([]string{"-data-dir", dir, "-addr", addr.String()}, flags...), cancel)
	if err != nil {
		return err
	}
	defer func() {
		stopErr := p.stop(candidateStopTimeout)
		if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
			err = errors.Join(err, ctxErr)
		}
		err = errors.Join(err, stopErr)
	}()

	base := "http://" + addr.String()
	if err := p.awaitHealth(probes, client, base, addr.Port); err != nil {
		return err
	}
	if err := probes.Err(); err != nil {
		return err
	}
	probeErr := check(probes, base)
	// Keep the probe diagnostic first even when the listener or process
	// disappeared during the last request.
	return errors.Join(probeErr, p.requireListener(addr.Port))
}
