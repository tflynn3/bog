package querymodel_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"testing"
)

type ownershipCheckedTransport struct {
	http.RoundTripper
	candidate *pinnedQueryCandidate
	probes    int
}

func (t *ownershipCheckedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.probes++
	owned, err := t.candidate.ownsBind()
	if err != nil || !owned {
		return nil, fmt.Errorf("HTTP probe attempted before candidate ownership (owned=%t): %v", owned, err)
	}
	return t.RoundTripper.RoundTrip(req)
}

func TestPinnedQueryCandidateRecoversFromTransientForeignListener(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), pinnedQueryStartupTimeout)
	defer cancel()

	foreign, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	foreignOpen := true
	defer func() {
		if foreignOpen {
			if err := foreign.Close(); err != nil {
				t.Error(err)
			}
		}
	}()

	gate, release, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	releaseOpen := true
	defer func() {
		if releaseOpen {
			if err := release.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	addr := foreign.Addr().(*net.TCPAddr)
	cmd := exec.Command(os.Args[0], "-test.run=^TestPinnedQueryCandidateChild$")
	cmd.Env = append(os.Environ(), "BOG_QUERY_STARTUP_TEST_MODE=wait-for-bind",
		"BOG_QUERY_STARTUP_TEST_ADDR="+addr.String(), "GORACE=atexit_sleep_ms=0")
	cmd.ExtraFiles = []*os.File{gate}
	candidate, startErr := launchPinnedQuery(ctx, cmd, addr)
	gateErr := gate.Close()
	if startErr != nil {
		t.Fatal(errors.Join(startErr, gateErr))
	}
	defer func() {
		stopErr := candidate.stop()
		diagnostic, _, logErr := candidate.closeLog()
		if err := errors.Join(stopErr, logErr); err != nil {
			t.Errorf("candidate cleanup: %v\n%s", err, diagnostic)
		}
		assertPinnedQueryReaped(t, []*exec.Cmd{cmd})
	}()
	if gateErr != nil {
		t.Fatal(gateErr)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	defer transport.CloseIdleConnections()
	probes := &ownershipCheckedTransport{RoundTripper: transport, candidate: candidate}
	raw := &http.Client{Transport: probes}
	foreignSamples := 0
	observe := func() (bool, error) {
		owned, err := candidate.ownsBind()
		if err == nil || !candidate.possibleBindCollision(err) {
			return owned, err
		}
		foreignSamples++
		if probes.probes != 0 {
			return false, errors.New("health probe ran while the foreign listener still owned the address")
		}
		// Keep the listener for two polls to prove the monitor resumes
		// observation while the child remains alive and blocked on its bind.
		if foreignSamples == 2 {
			foreignOpen = false
			if closeErr := foreign.Close(); closeErr != nil {
				return false, closeErr
			}
			n, writeErr := release.Write([]byte{1})
			if writeErr == nil && n != 1 {
				writeErr = io.ErrShortWrite
			}
			releaseOpen = false
			if err := errors.Join(writeErr, release.Close()); err != nil {
				return false, err
			}
		}
		return false, err
	}
	if err := candidate.awaitHealth(ctx, raw, observe); err != nil {
		t.Fatal(err)
	}
	if foreignSamples != 2 || probes.probes != 1 {
		t.Fatalf("foreign observations=%d health probes=%d, want 2 and 1", foreignSamples, probes.probes)
	}
}
