package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const journeyMemoryLimit = 256 << 20

// TestGrowingHistoryJourney runs bog/0.2/O1/J1 with an independently
// delegated cgroup v2 parent and the ordinary production executable.
func TestGrowingHistoryJourney(t *testing.T) {
	parent := os.Getenv("BOG_O1_CGROUP")
	if parent == "" {
		t.Skip("BOG_O1_CGROUP must name a delegated cgroup v2 parent")
	}
	binary := filepath.Join(t.TempDir(), "bog")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Bog: %v\n%s", err, out)
	}
	executable, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("binary_sha256=%x build=%v", sha256.Sum256(executable), info.Settings)

	ctx, cancel := context.WithTimeout(t.Context(), 110*time.Second)
	defer cancel()
	dir := privateDataDir(t)
	address := journeyAddress(t)
	base := "http://" + address
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	writer := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	readerTransport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer readerTransport.CloseIdleConnections()
	reader := &http.Client{Transport: readerTransport, Timeout: 2 * time.Second}

	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	padding := strings.Repeat("x", 10240)
	row := func(i int) journeyRow {
		tag := "needle"
		if i%4 == 1 {
			tag = "NEEDLE"
		}
		return journeyRow{start.Add(time.Duration(i) * time.Minute).Format(time.RFC3339), tag + "-" + strconv.Itoa(i) + "-" + padding}
	}
	query := func(k int) error {
		q := url.Values{"start": {row(k).TS}, "end": {row(k + 3).TS}, "contains": {"needle"}, "limit": {"10"}}
		headers, raw, err := journeyRequest(ctx, reader, http.MethodGet, base+"/query?"+q.Encode(), nil, http.StatusOK)
		if err != nil {
			return err
		}
		if headers.Get("Content-Type") != "application/x-ndjson" || !bytes.HasSuffix(raw, []byte("\n")) {
			return errors.New("query response was not complete NDJSON")
		}
		var rows []journeyRow
		for _, line := range bytes.Split(bytes.TrimSuffix(raw, []byte("\n")), []byte("\n")) {
			var got journeyRow
			if err := json.Unmarshal(line, &got); err != nil {
				return err
			}
			rows = append(rows, got)
		}
		if !slices.Equal(rows, []journeyRow{row(k), row(k + 2)}) {
			return fmt.Errorf("query k=%d returned %d rows with different order or values", k, len(rows))
		}
		return nil
	}
	for cycle := range 2 {
		t.Run(fmt.Sprintf("start_%d", cycle+1), func(t *testing.T) {
			p, group := startJourneyCandidate(t, parent, binary, dir, address)
			health, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			for {
				if _, _, err := journeyRequest(health, writer, http.MethodGet, base+"/healthz", nil, http.StatusOK); err == nil {
					break
				}
				select {
				case err := <-p.done:
					p.reaped = true
					t.Fatalf("startup: %v: %s", err, p.logs.String())
				case <-health.Done():
					t.Fatal(health.Err())
				case <-time.After(10 * time.Millisecond):
				}
			}
			if cycle == 0 {
				ingestCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
				defer cancel()
				timing := time.Now()
				middle := make(chan error, 1)
				for batch := range 128 {
					var body bytes.Buffer
					body.Grow(256 * (10240 + 70))
					enc := json.NewEncoder(&body)
					for i := batch * 256; i < (batch+1)*256; i++ {
						if err := enc.Encode(row(i)); err != nil {
							t.Fatal(err)
						}
					}
					if _, _, err := journeyRequest(ingestCtx, writer, http.MethodPost, base+"/ingest", &body, http.StatusNoContent); err != nil {
						t.Fatalf("batch %d: %v", batch+1, err)
					}
					if batch == 31 {
						go func() { middle <- query(0) }()
					}
				}
				if err := <-middle; err != nil {
					t.Fatal(err)
				}
				t.Logf("128 batches acknowledged in %s", time.Since(timing))
			}
			for _, k := range []int{0, 16384, 32764} {
				if err := query(k); err != nil {
					t.Fatal(err)
				}
			}
			stop := time.Now()
			if err := p.signalAndWait(t, syscall.SIGTERM, 4*time.Second); err != nil {
				t.Fatalf("graceful stop: %v: %s", err, p.logs.String())
			}
			rusage := p.cmd.ProcessState.SysUsage().(*syscall.Rusage)
			rss := rusage.Maxrss * 1024
			events, err := os.ReadFile(filepath.Join(group, "memory.events"))
			if err != nil {
				t.Fatal(err)
			}
			fields := strings.Fields(string(events))
			for _, counter := range []string{"oom", "oom_kill"} {
				index := slices.Index(fields, counter)
				if index < 0 || index+1 >= len(fields) || fields[index+1] != "0" {
					t.Fatalf("%s increased: %s", counter, events)
				}
			}
			if rss <= 0 || rss > journeyMemoryLimit {
				t.Fatalf("VmHWM = %d bytes, limit %d", rss, journeyMemoryLimit)
			}
			t.Logf("VmHWM=%d bytes, oom=0, oom_kill=0, SIGTERM=%s", rss, time.Since(stop))
		})
	}
}

type journeyRow struct {
	TS   string `json:"ts"`
	Line string `json:"line"`
}

func journeyRequest(ctx context.Context, client *http.Client, method, url string, body io.Reader, status int) (http.Header, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	b, err := io.ReadAll(resp.Body)
	if err := errors.Join(err, resp.Body.Close()); err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != status {
		return nil, nil, fmt.Errorf("%s %s: HTTP %d: %q", method, url, resp.StatusCode, b)
	}
	return resp.Header, b, nil
}

func journeyAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func startJourneyCandidate(t *testing.T, parent, binary, dir, address string) (*child, string) {
	t.Helper()
	group, err := os.MkdirTemp(parent, "journey-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Remove(group); err != nil {
			t.Error(err)
		}
	})
	for _, limit := range []struct{ name, value string }{
		{"memory.max", strconv.Itoa(journeyMemoryLimit)}, {"memory.swap.max", "0"},
	} {
		path := filepath.Join(group, limit.name)
		if err := os.WriteFile(path, []byte(limit.value), 0600); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(path)
		if err != nil || strings.TrimSpace(string(got)) != limit.value {
			t.Fatalf("read back %s: %q, %v", limit.name, got, err)
		}
	}
	// The shell parks until placement succeeds, then execs Bog in the same PID.
	p := &child{cmd: exec.Command("/bin/sh", "-c", `read -r gate && [ "$gate" = start ] && exec "$@"`,
		"journey", binary, "-data-dir", dir, "-addr", address), done: make(chan error, 1)}
	p.cmd.Stdout, p.cmd.Stderr = &p.logs, &p.logs
	in, err := p.cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.cmd.Start(); err != nil {
		t.Fatal(errors.Join(err, in.Close()))
	}
	go func() { p.done <- p.cmd.Wait() }()
	t.Cleanup(func() {
		if p.reaped {
			return
		}
		if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Error(err)
		}
		select {
		case <-p.done:
		case <-time.After(2 * time.Second):
			t.Error("reap after SIGKILL timed out")
		}
	})
	pid := strconv.Itoa(p.cmd.Process.Pid)
	err = os.WriteFile(filepath.Join(group, "cgroup.procs"), []byte(pid), 0600)
	if err == nil {
		raw, readErr := os.ReadFile(filepath.Join(group, "cgroup.procs"))
		err = readErr
		if err == nil && strings.TrimSpace(string(raw)) != pid {
			err = fmt.Errorf("cgroup members %q, expected only PID %s", raw, pid)
		}
	}
	if err == nil {
		_, err = io.WriteString(in, "start\n")
	}
	if err := errors.Join(err, in.Close()); err != nil {
		t.Fatal(err)
	}
	return p, group
}
