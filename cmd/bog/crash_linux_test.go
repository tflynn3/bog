package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os/exec"
	"slices"
	"syscall"
	"testing"
	"time"

	"bog/verify/listener"
)

// testAcknowledgedBatchCrash exercises the frozen bog/0.1/O1/J1 fixture
// through the built binary, including startup and shutdown after WAL replay.
func testAcknowledgedBatchCrash(t *testing.T, binary string) {
	t.Helper()
	dir := privateDataDir(t)
	probe, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().(*net.TCPAddr)
	if err := probe.Close(); err != nil {
		t.Fatalf("release loopback port: %v", err)
	}
	client := newServiceClient(addr.String())
	client.http.Timeout = 3 * time.Second
	client.http.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

	start := func() *child {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		p := startChild(t, binary, dir, addr.String(), "")
		waitForCrashTestHealth(t, ctx, client, p, addr.Port)
		return p
	}
	p := start()
	const (
		a        = `{"ts":"2025-01-01T05:30:01.123456789+05:30","line":" café \"quoted\"\npath\\tail "}` + "\n"
		boot     = `{"ts":"2025-01-01T00:00:00Z","line":"boot"}` + "\n"
		rejected = `{"ts":"2025-01-01T00:00:00Z","line":"rejected"}` + "\n"
		invalid  = `{"ts":"bad","line":"invalid"}` + "\n"
	)
	for _, batch := range []struct {
		body string
		code int
	}{
		{a, http.StatusNoContent},
		{rejected + invalid, http.StatusBadRequest},
		{boot + a, http.StatusNoContent},
	} {
		client.expect(t, http.MethodPost, "/ingest", batch.body, batch.code)
	}
	killErr := p.signalAndWait(t, syscall.SIGKILL, 5*time.Second)
	var exit *exec.ExitError
	if !errors.As(killErr, &exit) {
		t.Fatalf("expected SIGKILL termination, got %v: %s", killErr, p.logs.String())
	}
	if status, ok := exit.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("expected SIGKILL termination, got %v: %s", exit, p.logs.String())
	}

	restarted := start()
	headers, body := client.expectResponse(t, http.MethodGet,
		"/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&limit=10", "", http.StatusOK)
	if got := headers.Get("Content-Type"); got != "application/x-ndjson" {
		t.Fatalf("query Content-Type = %q, want application/x-ndjson", got)
	}
	type row struct {
		TS   string `json:"ts"`
		Line string `json:"line"`
	}
	duplicate := row{TS: "2025-01-01T00:00:01.123456789Z", Line: " café \"quoted\"\npath\\tail "}
	want := []row{{TS: "2025-01-01T00:00:00Z", Line: "boot"}, duplicate, duplicate}
	var got []row
	lines := bufio.NewScanner(bytes.NewReader(body))
	for lines.Scan() {
		var record row
		if err := json.Unmarshal(lines.Bytes(), &record); err != nil {
			t.Fatalf("decode query row %d: %v", len(got)+1, err)
		}
		got = append(got, record)
	}
	if err := lines.Err(); err != nil {
		t.Fatalf("scan NDJSON: %v", err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("query after SIGKILL = %#v, want %#v", got, want)
	}
	if err := restarted.signalAndWait(t, syscall.SIGTERM, 35*time.Second); err != nil {
		t.Fatalf("shutdown after restart: %v: %s", err, restarted.logs.String())
	}
}

func waitForCrashTestHealth(t *testing.T, ctx context.Context, client serviceClient, p *child, port int) {
	t.Helper()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, client.base+"/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	var last string
	for {
		select {
		case err := <-p.done:
			p.reaped = true
			t.Fatalf("server exited before /healthz succeeded: %v: %s", err, p.logs.String())
		case <-ctx.Done():
			t.Fatalf("/healthz did not return 200 within 10s: %s", last)
		default:
		}
		resp, err := client.http.Do(req)
		if err != nil {
			last = err.Error()
		} else {
			body := serviceResponse(t, resp)
			if resp.StatusCode == http.StatusOK {
				owns, err := listener.OwnsBind(p.cmd.Process.Pid, port, listener.BindLoopback)
				if err != nil || !owns {
					t.Fatalf("attribute /healthz response to child PID %d: owns=%t, %v", p.cmd.Process.Pid, owns, err)
				}
				return
			}
			last = resp.Status + " " + string(body)
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
		}
	}
}
