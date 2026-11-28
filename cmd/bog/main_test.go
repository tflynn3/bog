//go:build linux

package main

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/internal/wal"
)

func privateDataDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestParseFlags(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		ok      bool
		wantErr string
	}{
		{"defaults", nil, true, ""},
		{"explicit", []string{"-data-dir", "data", "-addr", ":1234", "-query-max-response-bytes", "1", "-query-concurrency", "2", "-query-work-budget", "3"}, true, ""},
		{"unknown", []string{"-unknown"}, false, "flag provided but not defined"},
		{"bad number", []string{"-query-concurrency", "x"}, false, "invalid value"},
		{"zero response", []string{"-query-max-response-bytes", "0"}, false, "query-max-response-bytes must be positive"},
		{"negative concurrency", []string{"-query-concurrency", "-1"}, false, "query-concurrency must be positive"},
		{"zero budget", []string{"-query-work-budget", "0"}, false, "query-work-budget must be positive"},
		{"positional", []string{"stray"}, false, "unexpected argument"},
		{"positional before flag", []string{"stray", "-data-dir", "data"}, false, "unexpected argument"},
		{"terminator", []string{"--", "stray"}, false, "unexpected argument"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := parseFlags(tt.args, io.Discard)
			if (err == nil) != tt.ok {
				t.Fatalf("parse: %v", err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("error: %v, want %q", err, tt.wantErr)
			}
			if tt.name == "explicit" && c != (config{dir: "data", addr: ":1234", maxResponseBytes: 1, queryConcurrency: 2, queryWorkBudget: 3}) {
				t.Fatalf("explicit: %+v", c)
			}
			if tt.name == "defaults" && (c.dir != "." || c.addr != "127.0.0.1:8080" || c.maxResponseBytes != httpapi.DefaultQueryMaxResponseBytes || c.queryConcurrency != httpapi.DefaultQueryConcurrency || c.queryWorkBudget != defaultRetainedQueryWork) {
				t.Fatalf("defaults: %+v", c)
			}
		})
	}
	var out bytes.Buffer
	if _, err := parseFlags([]string{"-help"}, &out); err != flag.ErrHelp || out.Len() == 0 {
		t.Fatalf("help: %v, %q", err, out.String())
	}
}

func TestRunClosesWALOnListenFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			t.Error(err)
		}
	}()
	dir := privateDataDir(t)
	c, err := parseFlags([]string{"-data-dir", dir, "-addr", listener.Addr().String()}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if err := run(c); !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("listen error: %v", err)
	}
	w, err := wal.Open(dir, &store.Store{})
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}
}
