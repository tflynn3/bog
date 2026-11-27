package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"bog/internal/client"
	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/verify/listener"
)

func TestCheckpointNativeIssuerQualification(t *testing.T) {
	binary := qualificationIssuerBinary(t)
	t.Run("live issuer", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		dir := t.TempDir()
		issuer := startQualificationIssuer(t, ctx, binary, dir, qualificationAddress(t))
		seedExportIssuer(t, ctx, issuer.base, exportSeed)
		for _, tc := range exportInterruptions {
			t.Run(tc.name, func(t *testing.T) {
				var afterInterrupt func(context.Context)
				// Commit backfill while the last capture is pinned. A
				// fresh walk afterwards must include the new tied row.
				if tc.wantAcknowledged == 2 {
					afterInterrupt = func(ctx context.Context) {
						seedExportIssuer(t, ctx, issuer.base, exportBackfill)
					}
				}
				qualifyExportInterruption(t, tc, issuer.proxy(t), afterInterrupt)
			})
		}
		fresh := newExportQualification(t, issuer.base)
		fresh.run(t, ctx, exitOK, "")
		assertSinkRows(t, fresh.output.Name(), exportEarly+exportBackfill+rowA+exportSecond+rowC)
	})
	t.Run("issuer restart", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		dir, addr := t.TempDir(), qualificationAddress(t)
		issuer := startQualificationIssuer(t, ctx, binary, dir, addr)
		seedExportIssuer(t, ctx, issuer.base, exportSeed)
		qualifyRejectedContinuation(t, ctx, issuer.base, issuer.proxy(t), func(string) {
			if err := issuer.stop(syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			restarted := startQualificationIssuer(t, ctx, binary, dir, addr)
			fresh := newExportQualification(t, restarted.base)
			fresh.run(t, ctx, exitOK, "")
			assertSinkRows(t, fresh.output.Name(), exportExpected)
		}, http.StatusBadRequest, client.ErrInvalidCursor)
	})
}

func TestCheckpointAuthenticatedCaptureExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	source := &exportExpirySource{rows: jsonRowsForExpiry(t)}
	api := httpapi.NewWithPageSource(source, nil, httpapi.DefaultQueryMaxResponseBytes,
		httpapi.DefaultQueryConcurrency, httpapi.DefaultQueryWorkBudget)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := api.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	issuer := httptest.NewServer(api)
	defer issuer.Close()
	qualifyRejectedContinuation(t, ctx, issuer.URL, api, func(token string) {
		source.expired.Store(true)
		// The issuer must still distinguish an unauthenticated cursor
		// from the token that it signed for the expired capture.
		macIndex := len(token) - 3
		replacement := "A"
		if token[macIndex] == 'A' {
			replacement = "B"
		}
		badToken := token[:macIndex] + replacement + token[macIndex+1:]
		checkQualificationRejection(t, ctx, issuer.URL, badToken, http.StatusBadRequest, client.ErrInvalidCursor)
	}, http.StatusGone, client.ErrCaptureExpired)
	if source.releases.Load() != 1 {
		t.Fatalf("expired capture releases = %d, want 1", source.releases.Load())
	}
}

func jsonRowsForExpiry(t *testing.T) []client.Row {
	t.Helper()
	var rows []client.Row
	for record := range strings.SplitSeq(strings.TrimSuffix(exportExpected, "\n"), "\n") {
		var row client.Row
		if err := json.Unmarshal([]byte(record), &row); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	return rows
}

type exportExpirySource struct {
	rows     []client.Row
	expired  atomic.Bool
	releases atomic.Int32
}

func (s *exportExpirySource) Page(_ context.Context, req httpapi.PageRequest, work *store.QueryWork) (httpapi.PageResult, error) {
	if req.Capture != "" && s.expired.Load() {
		return httpapi.PageResult{}, httpapi.ErrCaptureExpired
	}
	offset := 0
	if req.After != "" {
		var err error
		offset, err = strconv.Atoi(string(req.After))
		if err != nil || offset < 1 || offset >= len(s.rows) {
			return httpapi.PageResult{}, fmt.Errorf("unexpected fixture position %q", req.After)
		}
	}
	end := min(offset+req.Limit, len(s.rows))
	if err := work.Charge(uint64(end - offset)); err != nil {
		return httpapi.PageResult{}, err
	}
	result := httpapi.PageResult{Capture: "expiry-fixture", More: end < len(s.rows)}
	for i := offset; i < end; i++ {
		result.Rows = append(result.Rows, httpapi.PageRow{Row: s.rows[i], Position: httpapi.PagePosition(strconv.Itoa(i + 1))})
	}
	return result, nil
}

func (s *exportExpirySource) Release(capture string) error {
	if capture != "expiry-fixture" {
		return fmt.Errorf("unknown fixture capture %q", capture)
	}
	s.releases.Add(1)
	return nil
}

func qualificationIssuerBinary(t *testing.T) string {
	t.Helper()
	if binary := os.Getenv("BOG_RELEASE_BINARY"); binary != "" {
		abs, err := filepath.Abs(binary)
		if err != nil {
			t.Fatal(err)
		}
		return abs
	}
	// Native test runs can qualify their own checkout. Release runs
	// supply the already pinned issuer executable.
	binary := filepath.Join(t.TempDir(), "issuer")
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "../bog")
	build.Env = append(build.Environ(), "CGO_ENABLED=0")
	build.WaitDelay = time.Second
	if log, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build native issuer: %v\n%s", err, log)
	}
	return binary
}

func qualificationAddress(t *testing.T) *net.TCPAddr {
	t.Helper()
	socket, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	addr := socket.Addr().(*net.TCPAddr)
	if err := socket.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

type qualificationIssuer struct {
	base    string
	cmd     *exec.Cmd
	done    chan struct{}
	waitErr error // Published by closing done.
	reaped  bool
	log     bytes.Buffer
}

func startQualificationIssuer(t *testing.T, ctx context.Context, binary, dir string, addr *net.TCPAddr) *qualificationIssuer {
	t.Helper()
	p := &qualificationIssuer{base: "http://" + addr.String(), done: make(chan struct{})}
	// Lifetime is owned by cleanup. A canceled workload must still have
	// time to signal the issuer, drain it, and collect its exit status.
	p.cmd = exec.Command(binary, "-addr", addr.String(), "-data-dir", dir)
	p.cmd.Stdout, p.cmd.Stderr = &p.log, &p.log
	p.cmd.WaitDelay = time.Second
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("start issuer: %v", err)
	}
	go func() {
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

	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := p.waitForHealth(probeCtx, addr.Port); err != nil {
		t.Fatalf("issuer startup: %v", err)
	}
	return p
}

func (p *qualificationIssuer) waitForHealth(ctx context.Context, port int) error {
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	probe := &http.Client{Transport: transport, Timeout: time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return fmt.Errorf("issuer exited before listening (wait: %v)", p.waitErr)
		default:
		}
		owned, err := listener.OwnsBind(p.cmd.Process.Pid, port, listener.BindLoopback)
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
				return err
			}
			_, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			if err := errors.Join(readErr, resp.Body.Close()); err != nil {
				return err
			}
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("health probe returned HTTP %d", resp.StatusCode)
			}
			owned, err := listener.OwnsBind(p.cmd.Process.Pid, port, listener.BindLoopback)
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
			if errors.As(p.waitErr, &exit) && exit.Sys().(syscall.WaitStatus).Signal() == sig {
				return signalErr
			}
			return errors.Join(signalErr, fmt.Errorf("issuer did not exit on SIGKILL (wait: %v)", p.waitErr))
		}
		return errors.Join(signalErr, p.waitErr)
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
			return err
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
