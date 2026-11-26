package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestWalk(t *testing.T) {
	a, b := entry(1, "a"), entry(1, "b")
	line := func(r row) string {
		data, err := payload([]row{r})
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	type page struct{ body, next string }
	for _, tc := range []struct {
		name  string
		pages []page
		pass  bool
	}{
		{"whole unknown batch", []page{{line(a), "1"}, {line(b), ""}}, true},
		{"absent unknown batch", []page{{"", ""}}, true},
		{"partial unknown batch", []page{{line(a), ""}}, false},
		{"duplicate", []page{{line(a), "1"}, {line(a), ""}}, false},
		{"tie reordered", []page{{line(b), "1"}, {line(a), ""}}, false},
		{"repeated cursor", []page{{line(a), "1"}, {line(b), "1"}}, false},
		{"empty continuation", []page{{"", "1"}}, false},
		{"empty terminal continuation", []page{{line(a), "1"}, {line(b), "2"}, {"", ""}}, false},
		{"unterminated row", []page{{strings.TrimSpace(line(a)), ""}}, false},
		{"multiple rows per page", []page{{line(a) + line(b), ""}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				i := int(calls.Add(1)) - 1
				q := r.URL.Query()
				if i >= len(tc.pages) || q.Get("limit") != "1" ||
					(i == 0 && q.Get("page") != "1") || (i > 0 && q.Get("cursor") != tc.pages[i-1].next) {
					t.Error("unexpected page request", r.URL)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				p := tc.pages[i]
				w.Header().Set("Bog-Next-Cursor", p.next)
				if _, err := io.WriteString(w, p.body); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			s := fixtureServer{addr: strings.TrimPrefix(server.URL, "http://")}
			if err := s.walk(t.Context(), "", nil, []row{a, b}); (err == nil) != tc.pass {
				t.Fatalf("walk returned %v, want success=%t", err, tc.pass)
			}
		})
	}
}

func TestPinRejectsUnverifiedCandidate(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "candidate")
	if err := os.WriteFile(source, []byte("not a Go executable"), 0700); err != nil {
		t.Fatal(err)
	}
	sum := fmt.Sprintf("%x", sha256.Sum256([]byte("not a Go executable")))
	for _, tc := range []struct{ name, path, digest, want string }{
		{"directory", dir, sum, "regular file"},
		{"missing", filepath.Join(dir, "missing"), sum, "no such file"},
		{"missing digest", source, "", "SHA-256"},
		{"digest mismatch", source, strings.Repeat("0", 64), "SHA-256"},
		{"missing metadata", source, sum, "build information"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pinBinary(tc.path, filepath.Join(t.TempDir(), "pinned"), strings.Repeat("a", 40), tc.digest)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("pin returned %v, want %q", err, tc.want)
			}
		})
	}
}

func TestVerifyBuild(t *testing.T) {
	revision := strings.Repeat("a", 40)
	for _, tc := range []struct{ name, key, value, want string }{
		{name: "clean pinned Linux build"},
		{"other revision", "vcs.revision", strings.Repeat("b", 40), "require clean Git build"},
		{"modified", "vcs.modified", "true", "require clean Git build"},
		{"missing clean flag", "vcs.modified", "", "require clean Git build"},
		{"missing VCS", "vcs", "", "require clean Git build"},
		{"other OS", "GOOS", "darwin", "candidate targets"},
		{"other architecture", "GOARCH", "unknown", "candidate targets"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings := map[string]string{
				"vcs": "git", "vcs.revision": revision, "vcs.modified": "false",
				"GOOS": "linux", "GOARCH": runtime.GOARCH,
			}
			if tc.key != "" {
				settings[tc.key] = tc.value
			}
			err := verifyBuild(settings, revision)
			if tc.want == "" && err == nil {
				return
			}
			if tc.want == "" || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("verifyBuild returned %v, want %q", err, tc.want)
			}
		})
	}
}

func TestRecoveryOracle(t *testing.T) {
	acknowledged := []row{entry(1, "acknowledged")}
	batch := []row{entry(2, "first"), entry(2, "second")}
	full := append([]row{acknowledged[0]}, batch...)
	for _, tc := range []struct {
		name     string
		status   int
		complete bool
		want     [][]row
	}{
		{"acknowledged", http.StatusNoContent, true, [][]row{full}},
		{"explicitly invalid", http.StatusBadRequest, true, [][]row{acknowledged}},
		{"explicitly unavailable", http.StatusServiceUnavailable, true, [][]row{acknowledged}},
		{"ambiguous", 0, true, [][]row{acknowledged, full}},
		{"incomplete", 0, false, [][]row{acknowledged}},
		{"incomplete but acknowledged", http.StatusNoContent, false, nil},
		{"unexpected", http.StatusInternalServerError, true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := recoveryOracle(acknowledged, batch, tc.status, tc.complete)
			if (err != nil) != (tc.want == nil) || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("oracle returned %v, %v, want %v", got, err, tc.want)
			}
		})
	}
}

func TestRunRequiresExpectedDigest(t *testing.T) {
	dir := t.TempDir()
	args := []string{"-binary", filepath.Join(dir, "candidate"), "-revision", strings.Repeat("a", 40),
		"-fixture", "test", "-report", filepath.Join(dir, "report.json")}
	for _, digestArgs := range [][]string{nil, {"-sha256", "invalid"}} {
		err := run(t.Context(), append(args, digestArgs...))
		if err == nil || !strings.Contains(err.Error(), "require -sha256") {
			t.Fatalf("run with digest flags %q returned %v", digestArgs, err)
		}
	}
}

func TestCanceledStartupReapsChild(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "never-healthy")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexec sleep 60\n"), 0700); err != nil {
		t.Fatal(err)
	}
	s := fixtureServer{binary: binary, dir: dir}
	t.Cleanup(func() {
		if err := s.stop(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if err := s.start(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("start: %v", err)
	}
	if s.process == nil {
		t.Fatal("child was never started")
	}
	if err := s.stop(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.process.done:
	default:
		t.Fatal("stop returned before the child was reaped")
	}
}

func TestCleanupDisk(t *testing.T) {
	for _, tc := range []struct {
		name      string
		makeMount bool
	}{
		{"unmounted fixture is removed", true},
		{"unverifiable mount is preserved", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "disk")
			mount := filepath.Join(root, "fs")
			create := root
			if tc.makeMount {
				create = mount
			}
			if err := os.MkdirAll(create, 0700); err != nil {
				t.Fatal(err)
			}
			err := cleanupDisk(root, mount)
			if (err == nil) != tc.makeMount {
				t.Fatalf("cleanup returned %v", err)
			}
			_, err = os.Stat(root)
			if tc.makeMount && !errors.Is(err, os.ErrNotExist) || !tc.makeMount && err != nil {
				t.Fatalf("unexpected fixture state after cleanup: %v", err)
			}
		})
	}
}

func TestReportFailureAggregation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	r := report{1, strings.Repeat("a", 40), strings.Repeat("b", 64), "test", make(map[string]outcome)}
	cause := errors.New("checksum accepted")
	if err := r.record("committed_corruption", cause); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if err := r.record("torn_suffix", nil); err != nil {
		t.Fatal(err)
	}
	r.Cases["recovery"] = summarize(r.Cases, "torn_suffix", "committed_corruption")
	if err := writeReport(path, r); err != nil {
		t.Fatal(err)
	}
	got := readTestReport(t, path)
	if got.SchemaVersion != 1 || got.Cases["torn_suffix"].Status != "pass" ||
		got.Cases["recovery"] != (outcome{Status: "fail", Detail: "committed_corruption: checksum accepted"}) {
		t.Fatalf("unexpected report: %+v", got)
	}
}

func readTestReport(t *testing.T, path string) report {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var r report
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestLinuxCandidate(t *testing.T) {
	binary, revision := os.Getenv("BOG_DURABILITY_BINARY"), os.Getenv("BOG_DURABILITY_REVISION")
	sum := os.Getenv("BOG_DURABILITY_SHA256")
	if binary == "" && revision == "" && sum == "" {
		t.Skip("set BOG_DURABILITY_BINARY, BOG_DURABILITY_REVISION and BOG_DURABILITY_SHA256 on a Linux host with loop mounts")
	}
	scratch := t.TempDir()
	t.Setenv("TMPDIR", scratch)
	output := filepath.Join(t.TempDir(), "report.json")
	args := []string{"-binary", binary, "-revision", revision, "-fixture", "linux-release-v1", "-report", output, "-sha256", sum}
	if err := run(t.Context(), args); err != nil {
		t.Fatal(err)
	}
	got := readTestReport(t, output)
	if got.SchemaVersion != 1 || got.Revision != revision || got.BinarySHA256 != sum || got.Fixture != "linux-release-v1" {
		t.Fatalf("unexpected identity: %+v", got)
	}
	for _, name := range []string{"sigkill_acknowledged", "torn_suffix", "committed_corruption", "tied_backfill_replay", "stale_cursor", "enospc_recovery", "sigkill", "recovery", "enospc"} {
		if got.Cases[name].Status != "pass" {
			t.Errorf("%s: %+v", name, got.Cases[name])
		}
	}
	if evidence := got.Cases["enospc"].Evidence; evidence == nil || *evidence != (enospcEvidence{"ENOSPC", true, true, true}) {
		t.Errorf("unexpected ENOSPC evidence: %+v", evidence)
	}
	args[3] = strings.Repeat("0", 40)
	if err := run(t.Context(), args); err == nil || !strings.Contains(err.Error(), "require clean Git build") {
		t.Fatalf("revision mismatch: %v", err)
	}
	proof, err := checkENOSPC(t.Context(), filepath.Join(scratch, "missing-binary"))
	if err == nil || !proof.DisposableLocalFilesystem || proof.ObservedENOSPC {
		t.Fatalf("expected startup failure after mounting the image, got %+v, %v", proof, err)
	}
	entries, err := os.ReadDir(scratch)
	if err != nil || len(entries) != 0 {
		t.Fatalf("fixture left temporary files or mounts: %v, %v", entries, err)
	}
}
