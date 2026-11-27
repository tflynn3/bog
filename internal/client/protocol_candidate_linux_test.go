package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"bog/verify/listener"
)

// Run on native Linux with BOG_RELEASE_BINARY and BOG_RELEASE_REVISION
// pointing to the pinned release executable and its full clean revision.
func TestPinnedClientProtocolQualification(t *testing.T) {
	binary, revision := os.Getenv("BOG_RELEASE_BINARY"), os.Getenv("BOG_RELEASE_REVISION")
	if binary == "" && revision == "" {
		t.Skip("pinned Linux release candidate required")
	}
	revisionBytes, revisionErr := hex.DecodeString(revision)
	if binary == "" || revisionErr != nil || len(revisionBytes) != 20 || revision != strings.ToLower(revision) {
		t.Fatal("set BOG_RELEASE_BINARY and a full lowercase BOG_RELEASE_REVISION together")
	}
	binary, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	settings := make(map[string]string, len(info.Settings))
	for _, entry := range info.Settings {
		settings[entry.Key] = entry.Value
	}
	if info.Path != "bog/cmd/bog" || settings["GOOS"] != "linux" || settings["GOARCH"] != runtime.GOARCH ||
		settings["vcs.revision"] != revision || settings["vcs.modified"] != "false" {
		t.Fatalf("candidate path=%s goos=%s goarch=%s revision=%s modified=%s, want bog/cmd/bog linux/%s clean %s",
			info.Path, settings["GOOS"], settings["GOARCH"], settings["vcs.revision"], settings["vcs.modified"], runtime.GOARCH, revision)
	}
	digest, err := clientCandidateDigest(binary)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("candidate revision=%s binary_sha256=%x", revision, digest)
	t.Cleanup(func() {
		after, err := clientCandidateDigest(binary)
		if err != nil || !bytes.Equal(digest, after) {
			t.Errorf("candidate digest after qualification = %x, %v, want %x", after, err, digest)
		}
	})

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	base, checkOwner := startClientCandidate(t, ctx, binary)
	seedClient := newTestExportClient(t, base)
	outcome, err := seedClient.Ingest(ctx, []byte(qualificationSeed))
	if err != nil || outcome != IngestCommitted {
		t.Fatalf("candidate seed ingest = %v, %v, want acknowledged batch", outcome, err)
	}
	qualifyAtomicExport(t, ctx, base)
	if err := checkOwner(); err != nil {
		t.Fatal(err)
	}
}

func clientCandidateDigest(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	_, err = io.Copy(h, f)
	return h.Sum(nil), errors.Join(err, f.Close())
}

func startClientCandidate(t *testing.T, ctx context.Context, binary string) (string, func() error) {
	t.Helper()
	data := t.TempDir()
	reservation, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	addr := reservation.Addr().(*net.TCPAddr)
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	base := "http://" + addr.String()
	cmd := exec.Command(binary, "-data-dir="+data, "-addr="+addr.String())
	cmd.Env = append(cmd.Environ(), "BOG_CAPACITY_CONTROL=")
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	cmd.WaitDelay = time.Second
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = cmd.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		// A complete run must shut down and reap the actual candidate.
		signalErr := cmd.Process.Signal(syscall.SIGTERM)
		if signalErr != nil {
			t.Errorf("signal candidate for shutdown: %v", signalErr)
		}
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()
		select {
		case <-done:
			if waitErr != nil {
				t.Errorf("candidate exit: %v\n%s", waitErr, &output)
			}
		case <-timer.C:
			if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Errorf("kill stalled candidate: %v", err)
			}
			timer.Reset(3 * time.Second)
			select {
			case <-done:
				t.Errorf("candidate shutdown timed out, reaped with %v\n%s", waitErr, &output)
			case <-timer.C:
				t.Error("candidate could not be reaped after kill")
			}
		}
	})

	owner := func() (bool, error) {
		select {
		case <-done:
			return false, fmt.Errorf("candidate exited: %v\n%s", waitErr, &output)
		default:
			return listener.OwnsBind(cmd.Process.Pid, addr.Port, listener.BindLoopback)
		}
	}
	checkOwner := func() error {
		owned, err := owner()
		if err != nil {
			return err
		}
		if !owned {
			return errors.New("candidate does not own its loopback listener")
		}
		return nil
	}
	probeCtx, stopProbe := context.WithTimeout(ctx, 5*time.Second)
	defer stopProbe()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		owned, err := owner()
		if err != nil {
			t.Fatal(err)
		}
		if owned {
			break
		}
		select {
		case <-probeCtx.Done():
			t.Fatalf("waiting for candidate listener: %v", probeCtx.Err())
		case <-done:
			t.Fatalf("candidate exited before health probe: %v\n%s", waitErr, &output)
		case <-ticker.C:
		}
	}

	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	probe := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, base+"/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := probe.Do(req)
	if err != nil {
		t.Fatalf("candidate health probe: %v", err)
	}
	_, err = readBounded(resp.Body, 4096)
	if err := errors.Join(err, resp.Body.Close()); err != nil {
		t.Fatalf("read candidate health response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("candidate health HTTP %d, want 200", resp.StatusCode)
	}
	if err := checkOwner(); err != nil {
		t.Fatal(err)
	}
	return base, checkOwner
}
