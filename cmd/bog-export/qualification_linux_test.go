package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"bog/internal/client"
	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/verify/listener"
)

func TestCheckpointNativeIssuerQualification(t *testing.T) {
	binary := qualificationIssuerBinary(t)
	t.Run("live issuer", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		dir := t.TempDir()
		issuer := startQualificationIssuer(t, ctx, binary, dir, qualificationAddress(t))
		seedExportIssuer(t, ctx, issuer.base, exportSeed)
		for _, tc := range exportInterruptions {
			t.Run(tc.name, func(t *testing.T) {
				var afterInterrupt func(context.Context)
				// Commit backfill while the last capture is pinned. A
				// fresh walk afterwards must include the new tied row.
				if tc.wantAcknowledged == 2 {
					afterInterrupt = func(ctx context.Context) {
						seedExportIssuer(t, ctx, issuer.base, exportBackfill)
					}
				}
				qualifyExportInterruption(t, ctx, tc, issuer.proxy(t), afterInterrupt)
			})
		}
		fresh := newExportQualification(t, issuer.base)
		fresh.run(t, ctx, exitOK, "")
		assertSinkRows(t, fresh.output.Name(), exportEarly+exportBackfill+rowA+exportSecond+rowC)
	})
	t.Run("issuer restart", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		dir, addr := t.TempDir(), qualificationAddress(t)
		issuer := startQualificationIssuer(t, ctx, binary, dir, addr)
		seedExportIssuer(t, ctx, issuer.base, exportSeed)
		qualifyRejectedContinuation(t, ctx, issuer.base, issuer.proxy(t), func(string) {
			if err := issuer.stop(syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			restarted := startQualificationIssuer(t, ctx, binary, dir, addr)
			fresh := newExportQualification(t, restarted.base)
			fresh.run(t, ctx, exitOK, "")
			assertSinkRows(t, fresh.output.Name(), exportExpected)
		}, http.StatusBadRequest, client.ErrInvalidCursor)
	})
}

func TestCheckpointAuthenticatedCaptureExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	source := &exportExpirySource{rows: jsonRowsForExpiry(t)}
	api := httpapi.NewWithPageSource(source, nil, httpapi.DefaultQueryMaxResponseBytes,
		httpapi.DefaultQueryConcurrency, httpapi.DefaultQueryWorkBudget)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := api.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	issuer := httptest.NewServer(api)
	defer issuer.Close()
	qualifyRejectedContinuation(t, ctx, issuer.URL, api, func(token string) {
		source.expired.Store(true)
		// The issuer must still distinguish an unauthenticated cursor
		// from the token that it signed for the expired capture.
		macIndex := len(token) - 3
		replacement := "A"
		if token[macIndex] == 'A' {
			replacement = "B"
		}
		badToken := token[:macIndex] + replacement + token[macIndex+1:]
		checkQualificationRejection(t, ctx, issuer.URL, badToken, http.StatusBadRequest, client.ErrInvalidCursor)
	}, http.StatusGone, client.ErrCaptureExpired)
	if source.releases.Load() != 1 {
		t.Fatalf("expired capture releases = %d, want 1", source.releases.Load())
	}
}

func jsonRowsForExpiry(t *testing.T) []client.Row {
	t.Helper()
	var rows []client.Row
	for record := range strings.SplitSeq(strings.TrimSuffix(exportExpected, "\n"), "\n") {
		var row client.Row
		if err := json.Unmarshal([]byte(record), &row); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	return rows
}

type exportExpirySource struct {
	rows     []client.Row
	expired  atomic.Bool
	releases atomic.Int32
}

func (s *exportExpirySource) Page(_ context.Context, req httpapi.PageRequest, work *store.QueryWork) (httpapi.PageResult, error) {
	if req.Capture != "" && s.expired.Load() {
		return httpapi.PageResult{}, httpapi.ErrCaptureExpired
	}
	offset := 0
	if req.After != "" {
		var err error
		offset, err = strconv.Atoi(string(req.After))
		if err != nil || offset < 1 || offset >= len(s.rows) {
			return httpapi.PageResult{}, fmt.Errorf("unexpected fixture position %q", req.After)
		}
	}
	end := min(offset+req.Limit, len(s.rows))
	if err := work.Charge(uint64(end - offset)); err != nil {
		return httpapi.PageResult{}, err
	}
	result := httpapi.PageResult{Capture: "expiry-fixture", More: end < len(s.rows)}
	for i := offset; i < end; i++ {
		result.Rows = append(result.Rows, httpapi.PageRow{Row: s.rows[i], Position: httpapi.PagePosition(strconv.Itoa(i + 1))})
	}
	return result, nil
}

func (s *exportExpirySource) Release(capture string) error {
	if capture != "expiry-fixture" {
		return fmt.Errorf("unknown fixture capture %q", capture)
	}
	s.releases.Add(1)
	return nil
}

func qualificationIssuerBinary(t *testing.T) string {
	t.Helper()
	if binary := os.Getenv("BOG_RELEASE_BINARY"); binary != "" {
		abs, err := filepath.Abs(binary)
		if err != nil {
			t.Fatal(err)
		}
		return abs
	}
	// Native test runs can qualify their own checkout. Release runs
	// supply the already pinned issuer executable.
	binary := filepath.Join(t.TempDir(), "issuer")
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "../bog")
	build.Env = append(build.Environ(), "CGO_ENABLED=0")
	build.WaitDelay = time.Second
	if log, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build native issuer: %v\n%s", err, log)
	}
	return binary
}

func qualificationAddress(t *testing.T) *net.TCPAddr {
	t.Helper()
	socket, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	addr := socket.Addr().(*net.TCPAddr)
	if err := socket.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func startQualificationIssuer(t *testing.T, ctx context.Context, binary, dir string, addr *net.TCPAddr) *qualificationIssuer {
	t.Helper()
	cmd := exec.Command(binary, "-addr", addr.String(), "-data-dir", dir)
	cmd.Env = append(cmd.Environ(), "BOG_CAPACITY_CONTROL=")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return startQualificationProcess(t, ctx, cmd, addr, func(pid, port int) (bool, error) {
		return listener.OwnsBind(pid, port, listener.BindLoopback)
	}, nil, reapIssuerGroup)
}

// Observe exit without reaping the leader so its process group ID stays
// reserved until descendants have been signaled. Cmd.Wait reaps the leader.
func reapIssuerGroup(pid int) error {
	const pidType = 1 // Linux P_PID
	for {
		var info [16]uint64 // Linux siginfo_t, 128 bytes and naturally aligned.
		_, _, errno := syscall.Syscall6(syscall.SYS_WAITID, pidType, uintptr(pid),
			uintptr(unsafe.Pointer(&info)), syscall.WEXITED|syscall.WNOWAIT, 0, 0)
		if errno == syscall.EINTR {
			continue
		}
		if errno != 0 {
			return fmt.Errorf("observe issuer exit: %w", errno)
		}
		break
	}
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("clean up issuer process group: %w", err)
	}
	return nil
}
