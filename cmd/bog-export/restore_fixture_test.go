//go:build darwin || linux

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
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"bog/internal/client"
)

const recoveryFixtureRows = "fixture.ndjson"

func TestInterruptedExportRestoreFixture(t *testing.T) {
	for _, scenario := range exportRestoreCases {
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			dir := qualificationRestoreRoot(t)
			if err := os.WriteFile(filepath.Join(dir, recoveryFixtureRows), []byte(exportSeed), 0600); err != nil {
				t.Fatal(err)
			}
			scenario.run(t, ctx, func() *qualificationIssuer {
				return startRecoveryFixture(t, ctx, dir)
			})
		})
	}
}

func startRecoveryFixture(t *testing.T, ctx context.Context, dir string) *qualificationIssuer {
	t.Helper()
	socket, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	addr := socket.Addr().(*net.TCPAddr)
	file, fileErr := socket.File()
	if err := errors.Join(fileErr, socket.Close()); err != nil {
		if file != nil {
			err = errors.Join(err, file.Close())
		}
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestRecoveryIssuerHelper$", "--", dir)
	cmd.Env = append(cmd.Environ(), "BOG_EXPORT_RECOVERY_HELPER=1", "GORACE=atexit_sleep_ms=0")
	cmd.ExtraFiles = []*os.File{file}
	return startQualificationProcess(t, ctx, cmd, addr, nil, file, nil)
}

func TestRecoveryIssuerHelper(t *testing.T) {
	if os.Getenv("BOG_EXPORT_RECOVERY_HELPER") != "1" {
		return
	}
	if len(os.Args) != 4 {
		t.Fatal("fixture data directory required")
	}
	f := os.NewFile(3, "fixture listener")
	socket, socketErr := net.FileListener(f)
	if err := errors.Join(socketErr, f.Close()); err != nil {
		if socket != nil {
			err = errors.Join(err, socket.Close())
		}
		t.Fatal(err)
	}
	path := filepath.Join(os.Args[3], recoveryFixtureRows)
	mux := http.NewServeMux()
	var mu sync.Mutex
	type fixturePage struct{ body, next string }
	cursors := make(map[string]fixturePage)
	var serial byte
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /ingest", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		body, err := io.ReadAll(io.LimitReader(r.Body, 4097))
		if err != nil || len(body) > 4096 {
			http.Error(w, fmt.Sprintf("fixture ingest payload: %v", err), http.StatusBadRequest)
			return
		}
		if _, err := decodeRecoveryRows(bytes.NewReader(body)); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
		if err == nil {
			_, writeErr := file.Write(body)
			err = errors.Join(writeErr, file.Sync(), file.Close())
		}
		if err != nil {
			t.Error(err)
			http.Error(w, "fixture append failed", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /query", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		q := r.URL.Query()
		if token := q.Get("cursor"); token != "" {
			page, ok := cursors[token]
			if !ok {
				http.Error(w, "unknown fixture cursor", http.StatusBadRequest)
				return
			}
			servePage(t, w, page.next, page.body)
			return
		}
		from, startErr := time.Parse(time.RFC3339Nano, q.Get("start"))
		until, endErr := time.Parse(time.RFC3339Nano, q.Get("end"))
		limit, limitErr := strconv.Atoi(q.Get("limit"))
		if errors.Join(startErr, endErr, limitErr) != nil || limit != 2 || q.Get("page") != "1" {
			http.Error(w, "invalid fixture opening", http.StatusBadRequest)
			return
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Error(err)
			http.Error(w, "read fixture", http.StatusInternalServerError)
			return
		}
		rows, err := decodeRecoveryRows(bytes.NewReader(data))
		if err != nil {
			t.Error(err)
			http.Error(w, "decode fixture", http.StatusInternalServerError)
			return
		}
		rows = slices.DeleteFunc(rows, func(row client.Row) bool {
			return row.TS.Before(from) || !row.TS.Before(until) || !strings.Contains(row.Line, q.Get("contains"))
		})
		slices.SortStableFunc(rows, func(a, b client.Row) int { return a.TS.Compare(b.TS) })
		var pages []string
		for chunk := range slices.Chunk(rows, limit) {
			var body bytes.Buffer
			encoder := json.NewEncoder(&body)
			for _, row := range chunk {
				if err := encoder.Encode(row); err != nil {
					t.Error(err)
					http.Error(w, "encode fixture", http.StatusInternalServerError)
					return
				}
			}
			pages = append(pages, body.String())
		}
		if len(pages) == 0 {
			servePage(t, w, "", "")
			return
		}
		next := ""
		for i := len(pages) - 1; i > 0; i-- {
			serial++
			token := cursorFor(serial)
			cursors[token] = fixturePage{pages[i], next}
			next = token
		}
		servePage(t, w, next, pages[0])
	})
	t.Fatal(http.Serve(socket, mux))
}

type recoveryRoundTripper func(*http.Request) (*http.Response, error)

func (rt recoveryRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return rt(r) }

type observedIngestBody struct {
	events *[]string
	err    error
}

func (b observedIngestBody) Read([]byte) (int, error) {
	*b.events = append(*b.events, "read")
	return 0, errors.New("unexpected ingest body read")
}

func (b observedIngestBody) Close() error {
	*b.events = append(*b.events, "close")
	return b.err
}

func TestIngestAtCrashBoundary(t *testing.T) {
	t.Parallel()
	injected := errors.New("injected failure")
	for _, tc := range []struct {
		name                                string
		ownerErr, sendErr, killErr, bodyErr error
		status                              int
		wantKilled                          bool
		wantError                           string
		wantEvents                          []string
	}{
		{name: "204", status: 204, wantKilled: true, wantEvents: []string{"owner", "post", "kill", "close"}},
		{name: "non-204", status: 503, wantError: "HTTP 503", wantEvents: []string{"owner", "post", "close"}},
		{name: "owner check", ownerErr: injected, wantError: "pre-ingest listener", wantEvents: []string{"owner"}},
		{name: "request fails", sendErr: injected, wantError: "post-restore ingest", wantEvents: []string{"owner", "post"}},
		{name: "kill fails", status: 204, killErr: injected, wantError: "kill and reap", wantEvents: []string{"owner", "post", "kill", "close"}},
		{name: "close fails", status: 204, bodyErr: injected, wantKilled: true, wantError: "close ingest response", wantEvents: []string{"owner", "post", "kill", "close"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var events []string
			h := &http.Client{Transport: recoveryRoundTripper(func(req *http.Request) (*http.Response, error) {
				events = append(events, "post")
				data, err := io.ReadAll(req.Body)
				if err := errors.Join(err, req.Body.Close()); err != nil {
					t.Error(err)
				}
				if req.Method != http.MethodPost || req.URL.Path != "/ingest" ||
					req.Header.Get("Content-Type") != "application/x-ndjson" || string(data) != recoveryAppend {
					t.Errorf("unexpected ingest request %s %s, body %q", req.Method, req.URL, data)
				}
				if tc.sendErr != nil {
					return nil, tc.sendErr
				}
				return &http.Response{StatusCode: tc.status, Body: observedIngestBody{&events, tc.bodyErr}}, nil
			})}
			observation, err := ingestAtCrashBoundary(t.Context(), h, "http://127.0.0.1", recoveryAppend,
				func() error { events = append(events, "owner"); return tc.ownerErr },
				func() error { events = append(events, "kill"); return tc.killErr })
			if tc.wantError == "" && err != nil || tc.wantError != "" && (err == nil || !strings.Contains(err.Error(), tc.wantError)) {
				t.Errorf("ingest at crash boundary = %v, want %q", err, tc.wantError)
			}
			if observation.Status != tc.status || observation.Killed != tc.wantKilled || !slices.Equal(events, tc.wantEvents) {
				t.Errorf("ingest observation = %+v, events %v; want status %d, killed %t, events %v",
					observation, events, tc.status, tc.wantKilled, tc.wantEvents)
			}
		})
	}
}
