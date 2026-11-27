package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"bog/verify/listener"
)

const (
	capacityOptIn       = "BOG_CAPACITY_CONTROL"
	capacityScopeFilter = "capacity scope"
	capacityScopeSeed   = capacityScopeFilter + " seed"
	capacityScopeTail   = capacityScopeFilter + " continuation"
)

func TestReleaseCapacityControlScope(t *testing.T) {
	binary := os.Getenv("BOG_RELEASE_BINARY")
	if binary == "" {
		t.Skip("pinned release candidate required")
	}
	id, err := readIdentity(t.Context(), binary, os.Getenv("BOG_RELEASE_REVISION"), os.Getenv("BOG_RELEASE_FIXTURE"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("candidate revision=%s binary_sha256=%s fixture=%s", id.Revision, id.BinarySHA256, id.Fixture)

	for _, tc := range []struct {
		name    string
		bind    listener.Bind
		optIn   string
		visible bool
	}{
		{name: "loopback_without_opt_in", bind: listener.BindLoopback},
		{name: "loopback_with_opt_in_disabled", bind: listener.BindLoopback, optIn: "0"},
		{name: "wildcard_with_opt_in", bind: listener.BindWildcard, optIn: "1"},
		{name: "loopback_with_opt_in", bind: listener.BindLoopback, optIn: "1", visible: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Setenv registers restoration and prevents parallel execution. The
			// default case must remove even an opt-in inherited from the runner.
			t.Setenv(capacityOptIn, tc.optIn)
			if tc.optIn == "" {
				if err := os.Unsetenv(capacityOptIn); err != nil {
					t.Fatal(err)
				}
			}

			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			transport := &http.Transport{}
			client := &http.Client{
				Transport: transport,
				Timeout:   2 * time.Second,
				CheckRedirect: func(*http.Request, []*http.Request) error {
					return http.ErrUseLastResponse
				},
			}
			defer client.CloseIdleConnections()
			bind := candidateBind{
				ip: net.ParseIP(string(tc.bind)),
				verify: func(pid, port int) error {
					owned, err := listener.OwnsBind(pid, port, tc.bind)
					if err != nil {
						return err
					}
					if !owned {
						return fmt.Errorf("candidate PID %d has no IPv4 loopback listener on port %d", pid, port)
					}
					t.Logf("confirmed candidate pid=%d bind=%s:%d", pid, tc.bind, port)
					return nil
				},
			}
			err := withFixtureRoot(func(dir string) error {
				return serveDirectoryOn(ctx, binary, dir, client, nil, bind, (*candidateProcess).stop,
					func(ctx context.Context, base string) error {
						return probeCapacityControl(ctx, t, client, base, tc.visible)
					})
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func probeCapacityControl(ctx context.Context, t *testing.T, client *http.Client, base string, visible bool) error {
	t.Helper()
	// Commit a row so cutoff=1 ends a whole batch in every launch.
	if _, err := ingest(ctx, client, base, []row{{TS: fixtureStart, Line: capacityScopeSeed}}); err != nil {
		return fmt.Errorf("seed compaction cutoff: %w", err)
	}
	if !visible {
		if _, err := ingest(ctx, client, base, []row{{TS: fixtureStart, Line: capacityScopeTail}}); err != nil {
			return fmt.Errorf("seed continuation: %w", err)
		}
		return checkCapacityControlAbsent(ctx, t, client, base)
	}
	probeErr := checkCapacityControlPresent(ctx, t, client, base)
	return errors.Join(probeErr, checkCapacityHTTPStatus(ctx, t, client, base, http.MethodGet, "/healthz", http.StatusOK))
}

func checkCapacityControlPresent(ctx context.Context, t *testing.T, client *http.Client, base string) error {
	t.Helper()
	const cutoff = 1
	previous, err := readBackfillCompaction(ctx, client, base, http.MethodGet, "status", 0)
	if err != nil {
		return fmt.Errorf("read compaction predecessor: %w", err)
	}
	if previous.state != "complete" || previous.covered != 0 || previous.committed != cutoff {
		return fmt.Errorf("unexpected compaction predecessor after %d acknowledged row: %+v", cutoff, previous)
	}
	t.Logf("compaction predecessor: %+v", previous)
	published, err := publishBackfillGeneration(ctx, client, base, cutoff, &previous)
	if err != nil {
		return fmt.Errorf("publish compaction at cutoff %d: %w", cutoff, err)
	}
	t.Logf("compaction published: %+v", published)
	return nil
}

func checkCapacityControlAbsent(ctx context.Context, t *testing.T, client *http.Client, base string) error {
	t.Helper()
	var probeErr error
	for _, probe := range []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, "/_capacity/compaction?action=status&cutoff=1", http.StatusNotFound},
		{http.MethodPost, "/_capacity/compaction?action=start&cutoff=1", http.StatusNotFound},
	} {
		probeErr = errors.Join(probeErr, checkCapacityHTTPStatus(ctx, t, client, base, probe.method, probe.path, probe.want))
	}
	retirementErr := checkCapacityRetirementAbsent(ctx, t, client, base)
	healthErr := checkCapacityHTTPStatus(ctx, t, client, base, http.MethodGet, "/healthz", http.StatusOK)
	return errors.Join(probeErr, retirementErr, healthErr)
}

func checkCapacityRetirementAbsent(ctx context.Context, t *testing.T, client *http.Client, base string) error {
	t.Helper()
	const pageBytes = 4096
	pager, err := newReleasePager(base, client, pageBytes)
	if err != nil {
		return fmt.Errorf("create retirement scope pager: %w", err)
	}
	first, _, err := pager.fetch(ctx, capacityScopeFilter, 1, "")
	if err == nil {
		err = checkBudgetPage(first, []row{{TS: fixtureStart, Line: capacityScopeSeed}}, false, nil)
	}
	if err != nil {
		return fmt.Errorf("issue continuation for retirement: %w", err)
	}

	params := url.Values{"action": {"retire"}, "cursor": {first.NextCursor}}
	retireErr := checkCapacityHTTPStatus(ctx, t, client, base, http.MethodPost,
		"/_capacity/compaction?"+params.Encode(), http.StatusNotFound)
	last, _, resumeErr := pager.fetch(ctx, capacityScopeFilter, 1, first.NextCursor)
	if resumeErr == nil {
		resumeErr = checkBudgetPage(last, []row{{TS: fixtureStart, Line: capacityScopeTail}}, true, nil)
	}
	if resumeErr != nil {
		resumeErr = fmt.Errorf("resume continuation after retirement probe: %w", resumeErr)
	} else if retireErr == nil {
		t.Log("continuation returned the exact terminal row after HTTP 404 for retirement")
	}
	return errors.Join(retireErr, resumeErr)
}

func checkCapacityHTTPStatus(ctx context.Context, t *testing.T, client *http.Client, base, method, path string, want int) error {
	t.Helper()
	label, _, _ := strings.Cut(path, "&cursor=")
	res, err := request(ctx, client, method, base+path, nil)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, label, err)
	}
	t.Logf("%s %s returned HTTP %d", method, label, res.status)
	if res.status != want {
		return fmt.Errorf("%s %s returned HTTP %d, want %d: %.200s", method, label, res.status, want, res.body)
	}
	return nil
}

func TestProbeCapacityControlAbsent(t *testing.T) {
	t.Parallel()
	const token = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	for _, tc := range []struct {
		name                                  string
		status, start, retire, resume, health int
		missingCursor                         bool
		wantErr                               string
	}{
		{"hidden and healthy", 404, 404, 404, 200, 200, false, ""},
		{"status exposed", 200, 404, 404, 200, 200, false, "action=status"},
		{"start exposed", 404, 200, 404, 200, 200, false, "action=start"},
		{"start rejects method", 404, 405, 404, 200, 200, false, "action=start"},
		{"start redirects", 404, 302, 404, 200, 200, false, "action=start"},
		{"retirement exposed", 404, 404, 204, 200, 200, false, "action=retire"},
		{"retirement rejects method", 404, 404, 405, 200, 200, false, "action=retire"},
		{"retirement redirects", 404, 404, 302, 200, 200, false, "action=retire"},
		{"rejected retirement invalidates cursor", 404, 404, 404, 410, 200, false, "resume continuation after retirement probe"},
		{"query yields no cursor", 404, 404, 404, 200, 200, true, "missing continuation cursor"},
		{"health failed", 404, 404, 404, 200, 503, false, "/healthz"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var ingests atomic.Int32
			var retirementAttempted atomic.Bool
			handler := http.NewServeMux()
			handler.HandleFunc("POST /ingest", func(w http.ResponseWriter, r *http.Request) {
				const rowFormat = "{\"ts\":%q,\"line\":%q}\n"
				var want string
				switch ingests.Add(1) {
				case 1:
					want = fmt.Sprintf(rowFormat, fixtureStart, capacityScopeSeed)
				case 2:
					want = fmt.Sprintf(rowFormat, fixtureStart, capacityScopeTail)
				default:
					t.Error("unexpected extra ingest")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				body, err := io.ReadAll(r.Body)
				if err != nil || string(body) != want {
					t.Errorf("ingest body = %q, %v, want %q", body, err, want)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.WriteHeader(http.StatusNoContent)
			})
			handler.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.health)
			})
			handler.HandleFunc("/_capacity/compaction", func(w http.ResponseWriter, r *http.Request) {
				var status int
				switch r.Method + " " + r.URL.RawQuery {
				case "GET action=status&cutoff=1":
					status = tc.status
				case "POST action=start&cutoff=1":
					status = tc.start
				case "POST action=retire&cursor=" + token:
					if retirementAttempted.Swap(true) {
						t.Error("duplicate retirement request")
					}
					status = tc.retire
				default:
					t.Errorf("unexpected control request %s %q", r.Method, r.URL.RawQuery)
					status = http.StatusBadRequest
				}
				if status == http.StatusFound {
					w.Header().Set("Location", "/missing")
				}
				w.WriteHeader(status)
			})
			handler.HandleFunc("GET /query", func(w http.ResponseWriter, r *http.Request) {
				var line string
				switch r.URL.RequestURI() {
				case queryPath("", 1, capacityScopeFilter):
					if !tc.missingCursor {
						w.Header().Set(nextCursor, token)
					}
					line = capacityScopeSeed
				case queryPath(token, 1, capacityScopeFilter):
					if !retirementAttempted.Load() {
						t.Error("continuation resumed before retirement was attempted")
					}
					if tc.resume != http.StatusOK {
						http.Error(w, "query capture expired", tc.resume)
						return
					}
					line = capacityScopeTail
				default:
					t.Errorf("unexpected page request %q", r.URL.RequestURI())
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/x-ndjson")
				if _, err := fmt.Fprintf(w, "{\"ts\":%q,\"line\":%q}\n", fixtureStart, line); err != nil {
					t.Error(err)
				}
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			client := server.Client()
			client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
			err := probeCapacityControl(ctx, t, client, server.URL, false)
			if got := ingests.Load(); got != 2 {
				t.Errorf("ingests = %d, want 2", got)
			}
			if retirementAttempted.Load() == tc.missingCursor {
				t.Errorf("retirement attempted = %t, want %t", retirementAttempted.Load(), !tc.missingCursor)
			}
			if tc.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got %v, want an error containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestProbeCapacityControlPresent(t *testing.T) {
	t.Parallel()
	predecessor := backfillTestCompaction("complete", 7, 0, 1)
	running := backfillTestCompaction("running", 7, 0, 1)
	published := backfillTestCompaction("complete", 8, 1, 1)
	missing := lengthResponse(http.StatusNotFound, "", "missing")
	healthy := lengthResponse(http.StatusOK, "", "")

	for _, tc := range []struct {
		name    string
		replies []string
		health  string
		wantErr string
	}{
		{"running then complete", []string{backfillTestAck, predecessor, running, running, published}, healthy, ""},
		{"complete at start and status", []string{backfillTestAck, predecessor, published, published}, healthy, ""},
		{"seed refused", []string{lengthResponse(http.StatusServiceUnavailable, "", "")}, "", "ingest returned HTTP 503"},
		{"status route absent", []string{backfillTestAck, missing}, healthy, "status cutoff 0 returned HTTP 404"},
		{"start route absent", []string{backfillTestAck, predecessor, missing}, healthy, "start cutoff 1 returned HTTP 404"},
		{"poll route absent", []string{backfillTestAck, predecessor, running, missing}, healthy, "status cutoff 1 returned HTTP 404"},
		{"malformed predecessor", []string{backfillTestAck, lengthResponse(http.StatusOK, "", "{")}, healthy, "decode compaction status"},
		{"predecessor still running", []string{backfillTestAck, running}, healthy, "unexpected compaction predecessor"},
		{"predecessor already covers seed", []string{backfillTestAck, published}, healthy, "unexpected compaction predecessor"},
		{"predecessor missing seed", []string{backfillTestAck, backfillTestCompaction("complete", 7, 0, 0)}, healthy, "unexpected compaction predecessor"},
		{"predecessor has extra rows", []string{backfillTestAck, backfillTestCompaction("complete", 7, 0, 2)}, healthy, "unexpected compaction predecessor"},
		{"stale generation", []string{backfillTestAck, predecessor, running, backfillTestCompaction("complete", 7, 1, 1)}, healthy, "unexpected compaction state"},
		{"wrong coverage", []string{backfillTestAck, predecessor, running, backfillTestCompaction("complete", 8, 0, 1)}, healthy, "unexpected compaction state"},
		{"wrong committed rows", []string{backfillTestAck, predecessor, running, backfillTestCompaction("complete", 8, 1, 2)}, healthy, "unexpected compaction state"},
		{"missing publication counter", []string{backfillTestAck, predecessor, running, lengthResponse(http.StatusOK, "", `{"state":"complete","generation":8,"covered_rows":1}`)}, healthy, "invalid compaction status"},
		{"health failed after publication", []string{backfillTestAck, predecessor, running, published}, lengthResponse(http.StatusServiceUnavailable, "", ""), "GET /healthz returned HTTP 503"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requests := []backfillRequest{
				{path: "/ingest", batch: []row{{TS: fixtureStart, Line: capacityScopeSeed}}},
				{path: "/_capacity/compaction?action=status&cutoff=0"},
				{path: "/_capacity/compaction?action=start&cutoff=1"},
				{path: "/_capacity/compaction?action=status&cutoff=1"},
				{path: "/_capacity/compaction?action=status&cutoff=1"},
			}
			requests = requests[:len(tc.replies)]
			replies := tc.replies
			if tc.health != "" {
				requests = append(requests, backfillRequest{path: "/healthz"})
				replies = append(replies, tc.health)
			}
			client := &http.Client{Transport: scriptedBackfillTransport(t, requests, replies)}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			err := probeCapacityControl(ctx, t, client, "http://127.0.0.1", true)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got %v, want an error containing %q", err, tc.wantErr)
			}
		})
	}
}
