//go:build linux

package main

import (
	"bufio"
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
	"syscall"
	"testing"
	"time"
)

type child struct {
	cmd    *exec.Cmd
	done   chan error
	logs   bytes.Buffer
	reaped bool
}

func startChild(t *testing.T, binary, dir, addr, cwd string) *child {
	t.Helper()
	p := &child{cmd: exec.Command(binary, "-data-dir", dir, "-addr", addr), done: make(chan error, 1)}
	p.cmd.Dir = cwd
	p.cmd.Stdout = &p.logs
	p.cmd.Stderr = &p.logs
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.done <- p.cmd.Wait() }()
	t.Cleanup(func() {
		if !p.reaped {
			if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Errorf("kill: %v", err)
			}
			select {
			case <-p.done:
			case <-time.After(5 * time.Second):
				t.Error("reap timed out")
			}
		}
	})
	return p
}

func (p *child) stop(t *testing.T) {
	t.Helper()
	if err := p.signalAndWait(t, syscall.SIGTERM, 10*time.Second); err != nil {
		t.Fatalf("SIGTERM: %v: %s", err, p.logs.String())
	}
}

func (p *child) signalAndWait(t *testing.T, signal syscall.Signal, timeout time.Duration) error {
	t.Helper()
	if err := p.cmd.Process.Signal(signal); err != nil {
		t.Fatalf("send %s: %v", signal, err)
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-p.done:
		p.reaped = true
		return err
	case <-timer.C:
		t.Fatalf("reap after %s timed out after %s", signal, timeout)
		return nil
	}
}

func TestLifecycle(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "bog")
	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelBuild()
	build := exec.CommandContext(buildCtx, "go", "build", "-o", binary, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	run := func(args ...string) (string, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, args...)
		out, err := cmd.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatalf("command timed out: %v", args)
		}
		return string(out), err
	}
	empty := func(dir string) {
		t.Helper()
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("%s modified: %v", dir, entries)
		}
	}
	dir := privateDataDir(t)
	other := t.TempDir()
	if out, err := run("-addr", "127.0.0.1:0", "-data-dir", dir, "-query-max-response-bytes=0"); err == nil || !strings.Contains(out, "query-max-response-bytes must be positive") {
		t.Fatalf("invalid response limit: %v %s", err, out)
	}
	empty(dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	cmd := exec.CommandContext(ctx, binary, "-addr", "127.0.0.1:0", "stray", "-data-dir", other)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	cancel()
	if err == nil || ctx.Err() == context.DeadlineExceeded || !strings.Contains(string(out), "unexpected argument") {
		t.Fatalf("positional before flag: %v %s", err, out)
	}
	empty(dir)
	empty(other)
	for _, tt := range []struct {
		name string
		args []string
	}{
		{"invalid concurrency", []string{"-addr", "127.0.0.1:0", "-data-dir", dir, "-query-concurrency", "0"}},
		{"leftover between flags", []string{"-addr", "127.0.0.1:0", "-data-dir", dir, "extra", "-data-dir", other}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if out, err := run(tt.args...); err == nil {
				t.Fatalf("expected failure: %q", out)
			}
			empty(dir)
			empty(other)
		})
	}
	unsafe := t.TempDir()
	if err := os.Chmod(unsafe, 0777); err != nil {
		t.Fatal(err)
	}
	if out, err := run("-addr", "127.0.0.1:0", "-data-dir", unsafe); err == nil || !strings.Contains(out, unsafe) || !strings.Contains(out, "open data directory") || !strings.Contains(out, "not group- or world-writable") {
		t.Fatalf("unsafe directory: %v %s", err, out)
	}
	if err := os.Chmod(unsafe, 0700); err != nil {
		t.Fatal(err)
	}
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	occupiedOpen := true
	t.Cleanup(func() {
		if occupiedOpen {
			if err := occupied.Close(); err != nil {
				t.Errorf("close occupied listener: %v", err)
			}
		}
	})
	addr := occupied.Addr().String()
	if out, err := run("-data-dir", dir, "-addr", addr); err == nil || !strings.Contains(out, "listen on") || !strings.Contains(out, addr) || !strings.Contains(out, "address already in use") {
		t.Fatalf("occupied listener: %v %s", err, out)
	}
	if err := occupied.Close(); err != nil {
		t.Fatal(err)
	}
	occupiedOpen = false
	start := func(path, address string) {
		t.Helper()
		p := startChild(t, binary, path, address, other)
		client := newServiceClient(address)
		client.waitReady(t, p)
		client.expect(t, http.MethodGet, "/healthz", "", http.StatusOK)
		p.stop(t)
	}
	start(dir, addr)
	if _, err := os.Stat(filepath.Join(dir, "bog.wal")); err != nil {
		t.Fatalf("selected directory WAL: %v", err)
	}
	empty(other)
	start(dir, addr)
	start(unsafe, addr)
	t.Run("acknowledged_batch_crash", func(t *testing.T) { testAcknowledgedBatchCrash(t, binary) })
	t.Run("drain", func(t *testing.T) { testShutdownWithInFlightIngest(t, binary, true) })
	t.Run("drain_deadline", func(t *testing.T) { testShutdownWithInFlightIngest(t, binary, false) })
	t.Run("ready_enospc", func(t *testing.T) { testReadyWithKernelENOSPC(t, binary, addr) })
}

func testShutdownWithInFlightIngest(t *testing.T, binary string, finishRequest bool) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	dir := privateDataDir(t)
	p := startChild(t, binary, dir, addr, "")
	cmd, done, logs := p.cmd, p.done, &p.logs
	var conn net.Conn
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err = net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			break
		}
		select {
		case err := <-done:
			p.reaped = true
			t.Fatalf("server exited: %v: %s", err, logs.String())
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("startup: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close connection: %v", err)
		}
	}()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	body := `{"ts":"2025-01-01T00:00:00Z","line":"drained"}` + "\n"
	if _, err := fmt.Fprintf(conn, "POST /ingest HTTP/1.1\r\nHost: %s\r\nContent-Length: %d\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n", addr, len(body)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	req := &http.Request{Method: "POST"}
	interim, err := http.ReadResponse(reader, req)
	if err != nil {
		t.Fatal(err)
	}
	if err := interim.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if interim.StatusCode != 100 {
		t.Fatalf("interim status: %s", interim.Status)
	}
	if !finishRequest {
		err := p.signalAndWait(t, syscall.SIGTERM, 4*time.Second)
		if err == nil || !strings.Contains(logs.String(), "WAL remains open until exit): context deadline exceeded") {
			t.Fatalf("stalled ingest shutdown: %v: %s", err, logs.String())
		}
		return
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	// Wait until shutdown has stopped accepting new connections before finishing the request.
	stopped := false
	for time.Now().Before(deadline) {
		c, e := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if e != nil {
			stopped = true
			break
		}
		if e := c.Close(); e != nil {
			t.Fatal(e)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !stopped {
		t.Fatal("listener remained open")
	}
	select {
	case err := <-done:
		p.reaped = true
		t.Fatalf("exited before request completed: %v", err)
	default:
	}
	if _, err := io.WriteString(conn, body); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(reader, req)
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("response body: %v %v", readErr, closeErr)
	}
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("ingest: %s", response.Status)
	}
	select {
	case err := <-done:
		p.reaped = true
		if err != nil {
			t.Fatalf("exit: %v: %s", err, logs.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown timed out")
	}
	restarted := startChild(t, binary, dir, addr, "")
	client := &http.Client{Timeout: 300 * time.Millisecond, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}
	deadline = time.Now().Add(10 * time.Second)
	var responseBody []byte
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://" + addr + "/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z")
		if err == nil {
			responseBody, err = io.ReadAll(resp.Body)
			closeErr := resp.Body.Close()
			if err != nil || closeErr != nil || resp.StatusCode != http.StatusOK {
				t.Fatalf("query: %v %v %s", err, closeErr, resp.Status)
			}
			break
		}
		select {
		case err := <-restarted.done:
			restarted.reaped = true
			t.Fatalf("restart: %v: %s", err, restarted.logs.String())
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !bytes.Contains(responseBody, []byte("drained")) {
		t.Fatalf("drained row missing after restart: %s", responseBody)
	}
	if err := restarted.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-restarted.done:
		restarted.reaped = true
		if err != nil {
			t.Fatalf("restart exit: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("restart exit timed out")
	}

}
