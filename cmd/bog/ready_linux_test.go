package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

type serviceClient struct {
	base string
	http *http.Client
}

func newServiceClient(addr string) serviceClient {
	return serviceClient{
		base: "http://" + addr,
		http: &http.Client{Timeout: time.Second, Transport: &http.Transport{DisableKeepAlives: true}},
	}
}

func serviceResponse(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	if err := errors.Join(err, resp.Body.Close()); err != nil {
		t.Fatalf("read response: %v", err)
	}
	return body
}

func (c serviceClient) expect(t *testing.T, method, path, input string, code int) []byte {
	t.Helper()
	req, err := http.NewRequest(method, c.base+path, strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	body := serviceResponse(t, resp)
	if resp.StatusCode != code {
		t.Fatalf("%s %s = %d %s, want %d", method, path, resp.StatusCode, body, code)
	}
	return body
}

func (c serviceClient) waitReady(t *testing.T, p *child) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		select {
		case err := <-p.done:
			p.reaped = true
			t.Fatalf("server exited: %v: %s", err, p.logs.String())
		default:
		}
		resp, err := c.http.Get(c.base + "/ready")
		if err == nil {
			body := serviceResponse(t, resp)
			switch resp.StatusCode {
			case http.StatusOK:
				assertServiceReadiness(t, body, true, "ready", "")
				return
			case http.StatusServiceUnavailable:
				last = string(body)
			default:
				t.Fatalf("GET /ready at startup = %d %s", resp.StatusCode, body)
			}
		} else {
			last = err.Error()
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("readiness timed out: %s", last)
}

func assertServiceReadiness(t *testing.T, body []byte, ready bool, reason, category string) {
	t.Helper()
	var got struct {
		Ready    bool   `json:"ready"`
		Reason   string `json:"reason"`
		Category string `json:"category"`
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&got); err != nil || got.Ready != ready || got.Reason != reason || got.Category != category {
		t.Fatalf("readiness = %s (%v), want ready=%t reason=%q category=%q", body, err, ready, reason, category)
	}
}

// BOG_READINESS_TEST_TMPFS must name an empty, disposable tmpfs of at most 8 MiB.
func testReadyWithKernelENOSPC(t *testing.T, binary, addr string) {
	mount := os.Getenv("BOG_READINESS_TEST_TMPFS")
	if mount == "" {
		t.Skip("requires a disposable tiny tmpfs")
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(mount, &fs); err != nil {
		t.Fatal(err)
	}
	const tmpfsMagic = 0x01021994
	if fs.Type != tmpfsMagic || fs.Bsize <= 0 || fs.Blocks < 4 ||
		fs.Blocks > (8<<20)/uint64(fs.Bsize) || fs.Bavail != fs.Blocks {
		t.Fatalf("want empty tmpfs of at most 8 MiB; got %+v", fs)
	}
	dir, err := os.MkdirTemp(mount, "bog-ready-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	p := startChild(t, binary, dir, addr, "")
	client := newServiceClient(addr)
	client.waitReady(t, p)

	if err := syscall.Statfs(dir, &fs); err != nil {
		t.Fatal(err)
	}
	filler := filepath.Join(dir, "fill")
	file, err := os.OpenFile(filler, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	err = syscall.Fallocate(int(file.Fd()), 0, 0, int64(fs.Bavail)*int64(fs.Bsize))
	if err := errors.Join(err, file.Close()); err != nil {
		t.Fatalf("fill tmpfs: %v", err)
	}
	const rejected = `{"ts":"2025-01-01T00:00:00Z","line":"failed write"}`
	const accepted = `{"ts":"2025-01-01T00:00:01Z","line":"durable write"}`
	client.expect(t, http.MethodPost, "/ingest", rejected, http.StatusServiceUnavailable)
	body := client.expect(t, http.MethodGet, "/ready", "", http.StatusServiceUnavailable)
	assertServiceReadiness(t, body, false, "wal_rollback", "enospc")
	client.expect(t, http.MethodGet, "/healthz", "", http.StatusOK)

	if err := os.Remove(filler); err != nil {
		t.Fatal(err)
	}
	body = client.expect(t, http.MethodGet, "/ready", "", http.StatusServiceUnavailable)
	assertServiceReadiness(t, body, false, "wal_rollback", "enospc")
	client.expect(t, http.MethodPost, "/ingest", accepted, http.StatusNoContent)
	body = client.expect(t, http.MethodGet, "/ready", "", http.StatusOK)
	assertServiceReadiness(t, body, true, "ready", "")
	p.stop(t)

	// A fresh process must replay the accepted write and omit the rolled-back batch.
	restarted := startChild(t, binary, dir, addr, "")
	client.waitReady(t, restarted)
	body = client.expect(t, http.MethodGet, "/query?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z", "", http.StatusOK)
	if bytes.Contains(body, []byte("failed write")) || !bytes.Contains(body, []byte("durable write")) {
		t.Fatalf("unexpected rows after replay: %s", body)
	}
	restarted.stop(t)
}
