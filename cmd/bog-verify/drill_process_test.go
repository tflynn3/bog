package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"bog/internal/client"
)

func TestDrillSubprocess(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "bog-verify")
	buildCtx, cancelBuild := context.WithTimeout(t.Context(), time.Minute)
	defer cancelBuild()
	build := exec.CommandContext(buildCtx, "go", "build", "-o", binary, ".")
	build.WaitDelay = time.Second
	if log, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build bog-verify: %v\n%s", err, log)
	}
	bundle, rows, point := makeBundle(t)
	expected := []client.Row{rows[0], rows[2], rows[3], rows[4]}
	a, b := fixtureCursor('a'), fixtureCursor('b')
	for _, tc := range []struct {
		name, stderr, failure string
		status                int
	}{
		{name: "different pagination after restart"},
		{name: "startup failure with redirected stderr", stderr: "file", failure: "missing backup", status: exitVerification},
		{name: "startup failure with unwritable stderr", stderr: "read only", failure: "missing backup", status: exitVerification},
		{name: "startup failure with broken stderr pipe", stderr: "broken pipe", failure: "missing backup", status: exitVerification},
		{name: "parity failure with broken stderr pipe", stderr: "broken pipe", failure: "mismatch", status: exitMismatch},
		{name: "restart prompt with unwritable stderr", stderr: "read only", failure: "prompt", status: exitIncomplete},
		{name: "result with broken stdout pipe", failure: "stdout", status: exitIncomplete},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var restarted atomic.Bool
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				query := r.URL.Query()
				cursor := query.Get("cursor")
				if r.Method != http.MethodGet || r.URL.Path != "/query" ||
					(query.Get("page") == "1") != (cursor == "") {
					t.Errorf("unexpected drill request %s %s", r.Method, r.URL)
					http.Error(w, "unexpected query", http.StatusBadRequest)
					return
				}
				switch {
				case cursor == "" && tc.failure == "mismatch":
					writeDrillPage(t, w, drillPage{rows: []client.Row{rows[2]}})
				case cursor == "":
					writeDrillPage(t, w, drillPage{rows: expected[:2], next: a})
				case cursor == a && restarted.Load():
					writeDrillPage(t, w, drillPage{next: b})
				case cursor == a || (cursor == b && restarted.Load()):
					writeDrillPage(t, w, drillPage{rows: expected[2:]})
				default:
					t.Errorf("unexpected cursor %q", cursor)
					http.Error(w, "unexpected cursor", http.StatusBadRequest)
				}
			}))
			defer server.Close()

			ref := bundle
			if tc.failure == "missing backup" || tc.failure == "stdout" {
				ref = filepath.Join(t.TempDir(), "missing")
			}
			args := append(compareArgs(ref, server.URL), "-json")
			args[0] = "drill"
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, args...)
			cmd.WaitDelay = time.Second
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			prompt := make(chan struct{}, 1)
			cmd.Stderr = promptWriter(func(p []byte) (int, error) {
				n, err := stderr.Write(p)
				if bytes.HasPrefix(stderr.Bytes(), []byte("restart the restored server now.")) && bytes.Contains(stderr.Bytes(), []byte("\n")) {
					select {
					case prompt <- struct{}{}:
					default:
					}
				}
				return n, err
			})
			var redirectedPath string
			switch tc.stderr {
			case "file":
				redirectedPath = filepath.Join(t.TempDir(), "stderr")
				file, err := os.Create(redirectedPath)
				check(t, err)
				t.Cleanup(func() { check(t, file.Close()) })
				cmd.Stderr = file
			case "read only":
				file, err := os.Open(os.DevNull)
				check(t, err)
				t.Cleanup(func() { check(t, file.Close()) })
				cmd.Stderr = file
			case "broken pipe":
				cmd.Stderr = brokenPipe(t)
			}
			if tc.failure == "stdout" {
				cmd.Stdout = brokenPipe(t)
			}

			var observations drillObservations
			var opts options
			var probe *client.Client
			if tc.status == exitOK {
				var err error
				opts, err = parseOptions("drill", args[1:])
				check(t, err)
				probe, err = client.New(server.URL, nil)
				check(t, err)
				defer probe.CloseIdleConnections()
				observations.Before, err = observeDrillPhase(ctx, opts, probe, expected)
				check(t, err)
			}

			check(t, cmd.Start())
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			var waitErr error
			select {
			case <-prompt:
				restarted.Store(true)
				if err := cmd.Process.Signal(syscall.SIGUSR1); err != nil {
					cancel()
					t.Fatalf("acknowledge restart: %v (wait: %v)", err, <-done)
				}
				waitErr = <-done
			case waitErr = <-done:
			}
			status := exitOK
			if waitErr != nil {
				var failure *exec.ExitError
				if !errors.As(waitErr, &failure) || failure.ExitCode() < 0 {
					t.Fatalf("subprocess terminated without an exit code: %v", waitErr)
				}
				status = failure.ExitCode()
			}
			if status != tc.status {
				t.Fatalf("process exit %d, want %d, stdout %q, stderr %q", status, tc.status, &stdout, &stderr)
			}
			if tc.failure == "stdout" {
				if !strings.Contains(stderr.String(), "write result") {
					t.Fatalf("missing stdout failure diagnostic: %s", &stderr)
				}
				return
			}
			result := decodeDrillResult(t, stdout.Bytes())
			if result.ExitStatus != status || result.Parity != (status == exitOK) {
				t.Fatalf("process exit %d disagrees with %s", status, &stdout)
			}
			switch tc.failure {
			case "missing backup":
				if requests.Load() != 0 || result.RecoveryPoint != nil || result.BeforeRestart.Counts != nil ||
					result.BeforeRestart.Status != phaseFailed || result.AfterRestart.Status != phaseNotRun {
					t.Fatalf("unexpected startup observations: %s", &stdout)
				}
				requireProcessDiagnostic(t, result, phaseBeforeRestart, "backup_verification_failed")
				if redirectedPath != "" {
					data, err := os.ReadFile(redirectedPath)
					check(t, err)
					if !bytes.Contains(data, []byte(result.Diagnostic.Message)) {
						t.Fatalf("missing redirected diagnostic: %s", data)
					}
					checkDrillFixture(t, "startup-failure.json", stdout.Bytes(), false)
				}
			case "mismatch":
				requireProcessDiagnostic(t, result, phaseBeforeRestart, "mismatch")
			case "prompt":
				requireProcessDiagnostic(t, result, phaseRestartBoundary, "incomplete")
				if result.BeforeRestart.Status != phaseMatched || result.AfterRestart.Status != phaseNotRun || restarted.Load() {
					t.Fatalf("unexpected prompt failure observations: %s", &stdout)
				}
			default:
				if !restarted.Load() || result.Diagnostic != nil ||
					result.RecoveryPoint == nil || *result.RecoveryPoint != *resultPoint(point) {
					t.Fatalf("unexpected successful result: %s", &stdout)
				}
				var err error
				observations.After, err = observeDrillPhase(ctx, opts, probe, expected)
				check(t, err)
				data, err := json.Marshal(observations)
				check(t, err)
				checkDrillFixture(t, "observations.json", data, true)
				checkDrillFixture(t, "success.json", stdout.Bytes(), true)
			}
		})
	}
}

func brokenPipe(t *testing.T) *os.File {
	t.Helper()
	reader, writer, err := os.Pipe()
	check(t, err)
	t.Cleanup(func() { check(t, writer.Close()) })
	check(t, reader.Close())
	return writer
}

func requireProcessDiagnostic(t *testing.T, result drillResult, phase drillPhase, code string) {
	t.Helper()
	if result.Diagnostic == nil || result.Diagnostic.Phase != phase || result.Diagnostic.Code != code {
		t.Fatalf("diagnostic %+v, want phase %q and code %q", result.Diagnostic, phase, code)
	}
}

func checkDrillFixture(t *testing.T, name string, data []byte, exactMessage bool) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "restore-drill", name))
	if err != nil {
		t.Fatalf("read %s: %v (observed %s)", name, err, data)
	}
	decode := func(raw []byte) map[string]any {
		var fields map[string]any
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		check(t, dec.Decode(&fields))
		if err := dec.Decode(new(any)); err != io.EOF {
			t.Fatalf("expected end of fixture JSON, got %v", err)
		}
		if !exactMessage {
			if diagnostic, ok := fields["diagnostic"].(map[string]any); ok {
				delete(diagnostic, "message")
			}
		}
		return fields
	}
	if got, want := decode(data), decode(raw); !reflect.DeepEqual(got, want) {
		t.Fatalf("observation differs from %s\ngot  %s\nwant %s", name, data, raw)
	}
}
