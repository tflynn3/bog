package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	querymodel "bog/verify/query-model"
)

const shutdownPageCap = 16 << 20

func TestReleaseRetainedShutdown(t *testing.T) {
	if os.Getenv("BOG_RELEASE_RETAINED_SHUTDOWN") != "1" {
		t.Skip("set BOG_RELEASE_RETAINED_SHUTDOWN=1 to run against the pinned native candidate")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	binary := os.Getenv("BOG_RELEASE_BINARY")
	revision := os.Getenv("BOG_RELEASE_REVISION")
	fixture := os.Getenv("BOG_RELEASE_FIXTURE")
	digest := os.Getenv("BOG_RELEASE_BINARY_SHA256")
	if digest == "" {
		t.Fatal("BOG_RELEASE_BINARY_SHA256 is required")
	}
	checkIdentity := func() error {
		id, err := readIdentity(ctx, binary, revision, fixture)
		if err != nil {
			return err
		}
		if id.BinarySHA256 != digest {
			return fmt.Errorf("candidate SHA-256 = %s, want %s", id.BinarySHA256, digest)
		}
		return nil
	}
	if err := checkIdentity(); err != nil {
		t.Fatal(err)
	}
	t.Logf("candidate revision=%s binary_sha256=%s fixture=%s", revision, digest, fixture)
	t.Setenv(capacityOptIn, "1")
	client := &http.Client{
		Transport:     &http.Transport{DisableKeepAlives: true},
		Timeout:       5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	defer client.CloseIdleConnections()
	err := withFixtureRoot(func(dir string) error {
		return verifyRetainedShutdown(ctx, binary, dir, client, checkIdentity)
	})
	if err := errors.Join(err, checkIdentity()); err != nil {
		t.Fatal(err)
	}
	t.Log("retained continuation completed after SIGTERM with directory lock held during drain; exact restart walk passed")
}

func verifyRetainedShutdown(ctx context.Context, binary, dir string, client *http.Client, checkIdentity func() error) error {
	// A full continuation exceeds the socket send buffer. The first row,
	// tied distinct rows, and exact duplicates exercise the ordinal order.
	seed := make([]row, seedRows)
	seed[0] = row{fixtureStart, "opening row"}
	tie := fixtureTime(1).Format(time.RFC3339Nano)
	padding := strings.Repeat("x", payloadBytes)
	for i := 1; i < len(seed); i++ {
		seed[i] = row{tie, strconv.Itoa(i) + "-" + padding}
	}
	seed[3] = seed[1]
	backfill := []row{{tie, "successor tie"}, seed[1], {fixtureStart, "successor backfill"}}
	flags := []string{"-query-concurrency=1", "-query-max-response-bytes=" + strconv.Itoa(shutdownPageCap)}
	var ledger querymodel.Log
	err := superviseDirectory(ctx, binary, dir, client, flags, "", (*candidateProcess).gracefulStop,
		func(ctx context.Context, base string, p *candidateProcess, port int, owns listenerOwner) error {
			for offset := 0; offset < len(seed); offset += batchRows {
				if _, err := appendTailParityRows(ctx, client, base, &ledger, seed[offset:offset+batchRows]); err != nil {
					return fmt.Errorf("seed batch at %d: %w", offset, err)
				}
			}
			predecessor, err := publishReleaseGeneration(ctx, client, base, batchRows, ledger.HighWater(), nil)
			if err != nil {
				return fmt.Errorf("publish predecessor with committed suffix: %w", err)
			}
			pager, err := newReleasePager(base, client, shutdownPageCap)
			if err != nil {
				return err
			}
			filter := querymodel.Filter{Start: pager.query.Start, End: pager.query.End}
			first, _, err := fetchOraclePage(ctx, pager, querymodel.WireRequest{Filter: filter, Limit: 1, MaxPageBytes: shutdownPageCap})
			if err != nil {
				return fmt.Errorf("capture predecessor: %w", err)
			}
			if first.NextCursor == "" || len(first.Rows) != 1 || !first.Rows[0].Time.Equal(filter.Start) || first.Rows[0].Line != seed[0].Line {
				return errors.New("predecessor opening did not return its first row and cursor")
			}
			held, err := ledger.Match(ledger.HighWater(), filter)
			if err != nil {
				return err
			}
			want := make([]row, len(held)-1)
			for i, item := range held[1:] {
				want[i] = row{item.Time.Format(time.RFC3339Nano), item.Line}
			}
			return drainShutdownContinuation(ctx, base, first.NextCursor, want, func(ctx context.Context, peer int) error {
				if err := requireShutdownQuerySlot(ctx, client, base); err != nil {
					return err
				}
				if _, err := appendTailParityRows(ctx, client, base, &ledger, backfill); err != nil {
					return fmt.Errorf("acknowledge backfill: %w", err)
				}
				successor, err := publishReleaseGeneration(ctx, client, base, len(seed), ledger.HighWater(), &predecessor)
				if err != nil {
					return fmt.Errorf("replace predecessor while its continuation is blocked: %w", err)
				}
				pins, err := observeTailParityPins(ctx, client, base, successor)
				if err != nil {
					return fmt.Errorf("observe retired predecessor pin: %w", err)
				}
				if pins != 1 {
					return fmt.Errorf("retired predecessor pins = %d, want 1", pins)
				}
				if err := requireShutdownQuerySlot(ctx, client, base); err != nil {
					return err
				}
				if err := requireShutdownBackpressure(port, peer); err != nil {
					return err
				}
				if err := p.requireListenerOn(port, owns); err != nil {
					return err
				}
				if err := p.requestStop(syscall.SIGTERM); err != nil {
					return err
				}
				if err := awaitShutdownListenerClosed(ctx, strings.TrimPrefix(base, "http://")); err != nil {
					return err
				}
				if err := p.checkRunning(); err != nil {
					return fmt.Errorf("candidate exited before the retained response was released: %w", err)
				}
				if err := requireShutdownDirectoryLocked(dir); err != nil {
					return err
				}
				return errors.Join(requireShutdownBackpressure(port, peer), p.checkRunning())
			})
		})
	if err != nil {
		return fmt.Errorf("drain retained continuation: %w", err)
	}
	if err := checkIdentity(); err != nil {
		return err
	}
	err = serveDirectory(ctx, binary, dir, client, flags, func(ctx context.Context, base string) error {
		pager, err := newReleasePager(base, client, shutdownPageCap)
		if err != nil {
			return err
		}
		walk := querymodel.WireWalk{
			Filter:    querymodel.Filter{Start: pager.query.Start, End: pager.query.End},
			RowLimits: []int{17, 1, 29},
			Limits:    querymodel.WireLimits{MaxPages: ledger.HighWater(), MaxPageBytes: shutdownPageCap, Timeout: 12 * time.Second},
		}
		_, err = querymodel.VerifyWireWalk(ctx, &ledger, walk, parityFetch(pager), nil)
		return err
	})
	if err != nil {
		return fmt.Errorf("reopen the same directory and replay acknowledged rows: %w", err)
	}
	return nil
}

// Read only the response headers until the shutdown probe finishes. Then
// drain the same connection and check the complete predecessor response.
func drainShutdownContinuation(ctx context.Context, base, cursor string, want []row, check func(context.Context, int) error) (err error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	dialer := net.Dialer{Control: func(_, _ string, socket syscall.RawConn) error {
		var optionErr error
		controlErr := socket.Control(func(fd uintptr) {
			optionErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, receiveWindow)
		})
		return errors.Join(controlErr, optionErr)
	}}
	conn, err := dialer.DialContext(ctx, "tcp4", strings.TrimPrefix(base, "http://"))
	if err != nil {
		return err
	}
	var res *http.Response
	defer func() {
		// Close the socket first so error cleanup cannot block draining the body.
		err = errors.Join(err, conn.Close())
		if res != nil {
			if closeErr := res.Body.Close(); !errors.Is(closeErr, net.ErrClosed) {
				err = errors.Join(err, closeErr)
			}
		}
	}()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+queryPath(cursor, 1000, ""), nil)
	if err != nil {
		return err
	}
	req.Close = true
	if err := req.Write(conn); err != nil {
		return err
	}
	res, err = http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK || res.Header.Get("Content-Type") != "application/x-ndjson" ||
		res.Header.Get("Content-Length") == "" || res.ContentLength <= 0 || res.ContentLength > shutdownPageCap ||
		len(res.Header.Values(nextCursor)) != 0 || hasCursorTrailer(res) {
		return fmt.Errorf("invalid held response headers: HTTP %d, Content-Length %d", res.StatusCode, res.ContentLength)
	}
	server, peer := conn.RemoteAddr().(*net.TCPAddr).Port, conn.LocalAddr().(*net.TCPAddr).Port
	if err := awaitShutdownObservation(ctx, "held response to reach TCP backpressure", func(context.Context) (bool, error) {
		return zeroWindow(server, peer)
	}); err != nil {
		return err
	}
	if err := check(ctx, peer); err != nil {
		return err
	}
	if err := conn.(*net.TCPConn).SetReadBuffer(1 << 20); err != nil {
		return fmt.Errorf("expand receive buffer to drain retained response: %w", err)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("read retained response after SIGTERM: %w", err)
	}
	if int64(len(body)) != res.ContentLength || len(body) == 0 || body[len(body)-1] != '\n' || hasCursorTrailer(res) {
		return errors.New("retained response did not finish with exact NDJSON framing")
	}
	n, err := checkRows(response{status: res.StatusCode, contentType: res.Header.Get("Content-Type"), body: body}, want)
	if err != nil {
		return err
	}
	if n != len(want) {
		return fmt.Errorf("retained response delivered %d rows, want %d", n, len(want))
	}
	return nil
}

func requireShutdownQuerySlot(ctx context.Context, client *http.Client, base string) error {
	ctx, cancel := context.WithTimeout(ctx, queryDeadline)
	defer cancel()
	res, err := request(ctx, client, http.MethodGet, base+queryPath("", 1, ""), nil)
	if err != nil {
		return err
	}
	if res.status != http.StatusServiceUnavailable || !res.framed || res.cursorHeader ||
		!strings.HasPrefix(res.contentType, "text/plain") || string(res.body) != "query capacity exhausted\n" {
		return fmt.Errorf("held continuation did not occupy the sole query slot: HTTP %d %.100q", res.status, res.body)
	}
	return nil
}

func requireShutdownBackpressure(server, peer int) error {
	blocked, err := zeroWindow(server, peer)
	if err == nil && !blocked {
		return errors.New("held continuation left TCP backpressure before release")
	}
	return err
}

func awaitShutdownListenerClosed(ctx context.Context, addr string) error {
	var dialer net.Dialer
	return awaitShutdownObservation(ctx, "candidate to stop listening", func(ctx context.Context) (bool, error) {
		conn, err := dialer.DialContext(ctx, "tcp4", addr)
		if err != nil {
			if errors.Is(err, syscall.ECONNREFUSED) {
				return true, nil
			}
			return false, err
		}
		return false, conn.Close()
	})
}

func awaitShutdownObservation(ctx context.Context, condition string, check func(context.Context) (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, candidateStopTimeout)
	defer cancel()
	ticker := time.NewTicker(candidatePollInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		ready, err := check(ctx)
		if err != nil {
			return fmt.Errorf("wait for %s: %w", condition, err)
		}
		if ready {
			break
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("wait for %s: %w", condition, err)
	}
	return nil
}

func requireShutdownDirectoryLocked(dir string) (err error) {
	competing, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open competing directory descriptor: %w", err)
	}
	defer func() { err = errors.Join(err, competing.Close()) }()
	err = syscall.Flock(int(competing.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	switch {
	case errors.Is(err, syscall.EWOULDBLOCK):
		return nil
	case err != nil:
		return fmt.Errorf("probe directory lock during drain: %w", err)
	default:
		return errors.New("competing open acquired the data directory lock during drain")
	}
}

func TestShutdownDirectoryLockProbe(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	const unlocked = "competing open acquired the data directory lock during drain"
	if err := requireShutdownDirectoryLocked(dir); err == nil || err.Error() != unlocked {
		t.Fatalf("probe without owner = %v, want %q", err, unlocked)
	}
	owner, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := syscall.Flock(int(owner.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	if err := requireShutdownDirectoryLocked(dir); err != nil {
		t.Fatalf("probe with independent lock owner: %v", err)
	}
}
