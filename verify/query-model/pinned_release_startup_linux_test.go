package querymodel_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"bog/verify/listener"
)

const (
	pinnedQueryStartupTimeout  = 5 * time.Second
	pinnedQueryStartupAttempts = 3
	pinnedQueryStopTimeout     = 3 * time.Second
)

var (
	errPinnedQueryPreProbeExit = errors.New("candidate exited before health probe")
	pinnedQueryLogPrefix       = regexp.MustCompile(`^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} `)
)

func withPinnedQueryCandidate(ctx context.Context, command func(string) *exec.Cmd, logf func(string, ...any), run func(*http.Client, string) error) (err error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	defer transport.CloseIdleConnections()
	raw := &http.Client{
		Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	startupCtx, cancel := context.WithTimeout(ctx, pinnedQueryStartupTimeout)
	candidate, err := startPinnedQuery(startupCtx, raw, command, logf)
	cancel()
	if err != nil {
		return err
	}
	defer func() {
		stopErr := candidate.stop()
		diagnostic, complete, logErr := candidate.closeLog()
		if diagnostic != "" {
			logf("candidate %s stderr (complete=%t):\n%s", candidate.addr, complete, diagnostic)
		}
		if stopErr != nil || logErr != nil {
			err = errors.Join(err, stopErr, logErr)
		}
	}()

	if err := ctx.Err(); err != nil {
		return err
	}
	err = run(raw, candidate.baseURL())
	owned, ownErr := candidate.ownsBind()
	if ownErr == nil && !owned {
		ownErr = errors.New("pinned replay candidate lost listener ownership")
	}
	if ownErr != nil || ctx.Err() != nil {
		return errors.Join(err, ownErr, ctx.Err())
	}
	return err
}

func startPinnedQuery(ctx context.Context, raw *http.Client, command func(string) *exec.Cmd, logf func(string, ...any)) (*pinnedQueryCandidate, error) {
	used := make(map[int]bool)
	var collisions error
	for attempt := 1; attempt <= pinnedQueryStartupAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(collisions, fmt.Errorf("candidate startup deadline: %w", err))
		}
		addr, err := reservePinnedQueryAddress(used)
		if err != nil {
			return nil, errors.Join(collisions, err)
		}
		used[addr.Port] = true
		p, err := launchPinnedQuery(ctx, command(addr.String()), addr)
		if err != nil {
			return nil, errors.Join(collisions, fmt.Errorf("start candidate on %s: %w", addr, err))
		}
		probeErr := p.awaitHealth(ctx, raw, p.ownsBind)
		if probeErr == nil {
			return p, nil
		}

		exited := errors.Is(probeErr, errPinnedQueryPreProbeExit)
		var stopErr error
		if !exited {
			stopErr = p.stop()
		}
		diagnostic, complete, logErr := p.closeLog()
		failure := fmt.Errorf("candidate startup attempt %d on %s: %w (stderr complete=%t):\n%s",
			attempt, addr, errors.Join(probeErr, stopErr, logErr), complete, diagnostic)
		if !exited {
			return nil, errors.Join(collisions, failure)
		}
		failure = errors.Join(failure, fmt.Errorf("candidate exit on %s (wait result: %v)", addr, p.waitErr))
		if logErr != nil || !complete || !p.confirmedBindCollision(diagnostic) {
			return nil, errors.Join(collisions, failure)
		}
		collisions = errors.Join(collisions, failure)
		if attempt < pinnedQueryStartupAttempts && ctx.Err() == nil {
			logf("retrying confirmed candidate bind collision: %v", failure)
		}
	}
	return nil, errors.Join(fmt.Errorf("candidate bind collisions exhausted %d startup attempts", pinnedQueryStartupAttempts), collisions, ctx.Err())
}

func reservePinnedQueryAddress(used map[int]bool) (addr *net.TCPAddr, err error) {
	// Hold any repeated ports until a fresh one has been allocated.
	var sockets []*net.TCPListener
	defer func() {
		for _, socket := range sockets {
			if closeErr := socket.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("release candidate port reservation: %w", closeErr))
			}
		}
	}()
	for {
		socket, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			return nil, fmt.Errorf("reserve candidate loopback port: %w", err)
		}
		sockets = append(sockets, socket)
		addr := socket.Addr().(*net.TCPAddr)
		if !used[addr.Port] {
			return addr, nil
		}
	}
}

type pinnedQueryCandidate struct {
	addr    *net.TCPAddr
	cmd     *exec.Cmd
	stderr  *os.File
	done    chan struct{}
	waitErr error // Published by closing done.
}

func launchPinnedQuery(ctx context.Context, cmd *exec.Cmd, addr *net.TCPAddr) (*pinnedQueryCandidate, error) {
	// A file lets Wait reap the child independently of diagnostic output.
	stderr, err := os.CreateTemp("", "query-replay-stderr-")
	if err != nil {
		return nil, fmt.Errorf("capture candidate stderr: %w", err)
	}
	if err := os.Remove(stderr.Name()); err != nil {
		return nil, errors.Join(fmt.Errorf("unlink candidate stderr capture: %w", err), stderr.Close())
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, stderr.Close())
	}
	cmd.Stdout, cmd.Stderr = os.Stderr, stderr
	if err := cmd.Start(); err != nil {
		return nil, errors.Join(err, stderr.Close())
	}
	p := &pinnedQueryCandidate{addr: addr, cmd: cmd, stderr: stderr, done: make(chan struct{})}
	go func() {
		p.waitErr = cmd.Wait()
		close(p.done)
	}()
	return p, nil
}

func (p *pinnedQueryCandidate) baseURL() string {
	return "http://" + p.addr.String()
}

func (p *pinnedQueryCandidate) ownsBind() (bool, error) {
	return listener.OwnsBind(p.cmd.Process.Pid, p.addr.Port, listener.BindLoopback)
}

func (p *pinnedQueryCandidate) awaitHealth(ctx context.Context, raw *http.Client, observeBind func() (bool, error)) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var collision error
	for {
		select {
		case <-p.done:
			return errors.Join(errPinnedQueryPreProbeExit, collision)
		default:
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(collision, fmt.Errorf("waiting for candidate listener: %w", err))
		}
		owned, err := observeBind()
		if err != nil {
			if !p.possibleBindCollision(err) {
				return err
			}
			// The port can become available while the child is still
			// initializing. Keep polling, without probing the foreign socket.
			collision, owned = err, false
		}
		if owned {
			status, _, err := pinnedRequest(ctx, raw, http.MethodGet, p.baseURL()+"/healthz")
			if err != nil {
				return fmt.Errorf("check candidate health: %w", err)
			}
			if status != http.StatusOK {
				return fmt.Errorf("candidate health returned HTTP %d", status)
			}
			owned, err := observeBind()
			if err != nil {
				return err
			}
			if !owned {
				return errors.New("candidate lost listener during startup")
			}
			return ctx.Err()
		}
		select {
		case <-p.done:
			return errors.Join(errPinnedQueryPreProbeExit, collision)
		case <-ctx.Done():
			return errors.Join(collision, fmt.Errorf("waiting for candidate listener: %w", ctx.Err()))
		case <-ticker.C:
		}
	}
}

func (p *pinnedQueryCandidate) possibleBindCollision(err error) bool {
	var pathErr *os.PathError
	// Linux can hide the fd directory or deny access as the child exits.
	// The caller still requires the reaped child's exact bind diagnostic.
	fdDir := filepath.Join("/proc", strconv.Itoa(p.cmd.Process.Pid), "fd")
	return strings.HasPrefix(err.Error(), fmt.Sprintf("foreign listener on IPv4 loopback port %d (", p.addr.Port)) ||
		((errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission)) &&
			errors.As(err, &pathErr) && pathErr.Path == fdDir)
}

func (p *pinnedQueryCandidate) confirmedBindCollision(diagnostic string) bool {
	var exit *exec.ExitError
	if !errors.As(p.waitErr, &exit) || exit.ExitCode() != 1 {
		return false
	}
	// Match the full cmd/bog bind diagnostic for the requested address.
	// In particular, a second line reporting WAL close failure forbids a retry.
	message := pinnedQueryLogPrefix.ReplaceAllString(strings.TrimSuffix(diagnostic, "\n"), "")
	want := fmt.Sprintf("listen on %q: listen tcp %s: bind: %s", p.addr, p.addr, syscall.EADDRINUSE)
	return message == want
}

func (p *pinnedQueryCandidate) closeLog() (diagnostic string, complete bool, err error) {
	const maxDiagnostic = 64 << 10
	data, err := io.ReadAll(io.NewSectionReader(p.stderr, 0, maxDiagnostic+1))
	err = errors.Join(err, p.stderr.Close())
	return string(data[:min(len(data), maxDiagnostic)]), len(data) <= maxDiagnostic, err
}

func (p *pinnedQueryCandidate) stop() error {
	select {
	case <-p.done:
		return fmt.Errorf("candidate exited before shutdown (wait result: %v)", p.waitErr)
	default:
	}
	signalErr := p.cmd.Process.Signal(syscall.SIGTERM)
	timer := time.NewTimer(pinnedQueryStopTimeout)
	defer timer.Stop()
	select {
	case <-p.done:
		if p.waitErr != nil {
			return errors.Join(signalErr, fmt.Errorf("candidate shutdown: %w", p.waitErr))
		}
		return signalErr
	case <-timer.C:
		killErr := p.cmd.Process.Kill()
		if errors.Is(killErr, os.ErrProcessDone) {
			killErr = nil
		}
		err := errors.Join(errors.New("candidate exceeded graceful shutdown deadline"), signalErr, killErr)
		timer.Reset(pinnedQueryStopTimeout)
		select {
		case <-p.done:
			return err
		case <-timer.C:
			return errors.Join(err, errors.New("timed out reaping candidate"))
		}
	}
}

func TestPinnedQueryCandidateStartup(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		modes                []string
		wantError            string
		wantServed           bool
		cancelAfterCollision bool
	}{
		{name: "collision then success", modes: []string{"collision", "serve"}, wantServed: true},
		{name: "unrelated exit with foreign listener", modes: []string{"startup-error"}, wantError: "open data directory"},
		{name: "collision with failed WAL close", modes: []string{"close-error"}, wantError: "close WAL: injected failure"},
		{name: "bind diagnostic for wrong address", modes: []string{"wrong-address"}, wantError: `listen on "127.0.0.1:0"`},
		{name: "retry exhaustion", modes: []string{"collision", "collision", "collision"}, wantError: "bind collisions exhausted 3 startup attempts"},
		{name: "canceled between attempts", modes: []string{"collision"}, wantError: "context canceled", cancelAfterCollision: true},
		{name: "unexpected health status", modes: []string{"bad-health"}, wantError: "candidate health returned HTTP 503"},
		{name: "ownership lost during health", modes: []string{"close-on-health"}, wantError: "candidate lost listener during startup"},
		{name: "ownership lost after health", modes: []string{"close-on-workload"}, wantError: "candidate lost listener ownership", wantServed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var commands []*exec.Cmd
			addresses := make(map[string]bool)
			command := func(addr string) *exec.Cmd {
				if len(commands) == len(tc.modes) {
					t.Fatalf("unexpected startup attempt on %s", addr)
				}
				assertPinnedQueryReaped(t, commands)
				if addresses[addr] {
					t.Fatalf("reused candidate address %s", addr)
				}
				addresses[addr] = true
				mode := tc.modes[len(commands)]
				switch mode {
				case "collision", "startup-error", "close-error", "wrong-address":
					serveForeignPinnedQueryListener(t, addr)
				}
				cmd := exec.Command(os.Args[0], "-test.run=^TestPinnedQueryCandidateChild$")
				cmd.Env = append(os.Environ(), "BOG_QUERY_STARTUP_TEST_MODE="+mode,
					"BOG_QUERY_STARTUP_TEST_ADDR="+addr, "GORACE=atexit_sleep_ms=0")
				commands = append(commands, cmd)
				return cmd
			}

			logf := func(format string, args ...any) {
				t.Logf(format, args...)
				if tc.cancelAfterCollision {
					cancel()
				}
			}
			served := 0
			err := withPinnedQueryCandidate(ctx, command, logf, func(raw *http.Client, base string) error {
				served++
				status, _, err := pinnedRequest(ctx, raw, http.MethodGet, base+"/workload")
				if err != nil {
					return err
				}
				if status != http.StatusNoContent {
					return fmt.Errorf("workload returned HTTP %d", status)
				}
				return nil
			})
			if tc.wantError == "" {
				if err != nil {
					t.Fatalf("startup or workload failed: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("got %v, want error containing %q", err, tc.wantError)
			}
			if got := served == 1; got != tc.wantServed || served > 1 {
				t.Errorf("served %d times, want served=%t", served, tc.wantServed)
			}
			if len(commands) != len(tc.modes) {
				t.Errorf("started %d commands, want %d", len(commands), len(tc.modes))
			}
			assertPinnedQueryReaped(t, commands)
		})
	}
}

func serveForeignPinnedQueryListener(t *testing.T, addr string) {
	t.Helper()
	l, err := net.Listen("tcp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("foreign listener on %s received %s %s", addr, r.Method, r.URL.Path)
		w.WriteHeader(http.StatusOK)
	})}
	served := make(chan error, 1)
	go func() { served <- server.Serve(l) }()
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
		if err := <-served; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve foreign listener: %v", err)
		}
	})
}

func assertPinnedQueryReaped(t *testing.T, commands []*exec.Cmd) {
	t.Helper()
	for _, cmd := range commands {
		if cmd.Process == nil {
			t.Fatal("candidate was never started")
		}
		if err := cmd.Process.Signal(syscall.Signal(0)); !errors.Is(err, os.ErrProcessDone) {
			t.Errorf("candidate PID %d was not reaped: %v", cmd.Process.Pid, err)
		}
	}
}

func TestPinnedQueryCandidateChild(t *testing.T) {
	mode := os.Getenv("BOG_QUERY_STARTUP_TEST_MODE")
	if mode == "" {
		return
	}
	addr := os.Getenv("BOG_QUERY_STARTUP_TEST_ADDR")
	switch mode {
	case "startup-error":
		log.Print("open data directory: injected startup failure")
		os.Exit(1)
	case "wrong-address":
		log.Printf(`listen on "127.0.0.1:0": listen tcp 127.0.0.1:0: bind: %s`, syscall.EADDRINUSE)
		os.Exit(1)
	}

	if mode == "wait-for-bind" {
		gate := os.NewFile(3, "candidate-bind-gate")
		if gate == nil {
			t.Fatal("missing bind gate")
		}
		var ready [1]byte
		_, readErr := io.ReadFull(gate, ready[:])
		if err := errors.Join(readErr, gate.Close()); err != nil {
			t.Fatal(err)
		}
	}
	l, err := net.Listen("tcp", addr)
	if mode == "collision" || mode == "close-error" {
		if !errors.Is(err, syscall.EADDRINUSE) {
			if err == nil {
				err = l.Close()
			}
			t.Fatalf("bind %s got %v, want EADDRINUSE", addr, err)
		}
		failure := fmt.Errorf("listen on %q: %w", addr, err)
		if mode == "close-error" {
			failure = errors.Join(failure, errors.New("close WAL: injected failure"))
		}
		log.Print(failure)
		os.Exit(1)
	}
	if err != nil {
		t.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(t.Context(), syscall.SIGTERM)
	defer stop()
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if (mode == "close-on-health" && r.URL.Path == "/healthz") ||
			(mode == "close-on-workload" && r.URL.Path == "/workload") {
			if err := l.Close(); err != nil {
				t.Errorf("close candidate listener: %v", err)
			}
		}
		switch {
		case r.URL.Path == "/workload":
			w.WriteHeader(http.StatusNoContent)
		case mode == "bad-health":
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusOK)
		}
	})}
	served := make(chan error, 1)
	go func() { served <- server.Serve(l) }()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Error(err)
	}
	if err := <-served; !errors.Is(err, http.ErrServerClosed) &&
		!((mode == "close-on-health" || mode == "close-on-workload") && errors.Is(err, net.ErrClosed)) {
		t.Errorf("serve candidate HTTP: %v", err)
	}
	if !t.Failed() {
		// Avoid emitting a child test runner's PASS in the release test event stream.
		cancel()
		stop()
		os.Exit(0)
	}
}
