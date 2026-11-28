package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"slices"
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
	t.Log("retained continuation drained under SIGTERM with the directory lock held. After reaping, verified predecessor reclamation and intact successor storage under the reacquired lock, released it, and passed the exact restart walk.")
}

func verifyRetainedShutdown(ctx context.Context, binary, dir string, client *http.Client, checkIdentity func() error) (err error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("pin shutdown fixture directory: %w", err)
	}
	defer func() { err = errors.Join(err, root.Close()) }()

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
	var storage shutdownStorageObservation
	err = superviseDirectory(ctx, binary, dir, client, flags, "", (*candidateProcess).gracefulStop,
		func(probes context.Context, base string, p *candidateProcess, port int, owns listenerOwner) error {
			for offset := 0; offset < len(seed); offset += batchRows {
				if _, err := appendTailParityRows(probes, client, base, &ledger, seed[offset:offset+batchRows]); err != nil {
					return fmt.Errorf("seed batch at %d: %w", offset, err)
				}
			}
			predecessor, err := publishReleaseGeneration(probes, client, base, batchRows, ledger.HighWater(), nil)
			if err != nil {
				return fmt.Errorf("publish predecessor with committed suffix: %w", err)
			}
			_, storage.predecessor, err = observeShutdownStorage(probes, root, predecessor)
			if err != nil {
				return fmt.Errorf("observe predecessor storage before capture: %w", err)
			}
			pager, err := newReleasePager(base, client, shutdownPageCap)
			if err != nil {
				return err
			}
			filter := querymodel.Filter{Start: pager.query.Start, End: pager.query.End}
			first, _, err := fetchOraclePage(probes, pager, querymodel.WireRequest{Filter: filter, Limit: 1, MaxPageBytes: shutdownPageCap})
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
			// Process exit cancels probes. The client can still have buffered
			// response bytes to read after a successful server exit.
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
				storage.authority, storage.successor, err = observeShutdownStorage(ctx, root, successor)
				if err != nil {
					return fmt.Errorf("observe successor storage during replacement: %w", err)
				}
				storage.retired, err = retiredTailParityFiles(root, storage.predecessor, storage.successor)
				if err != nil {
					return err
				}
				if err := requireShutdownRetirementHold(ctx, client, base, successor, p.process.Pid, storage.predecessor); err != nil {
					return err
				}
				storage.digests = make(map[string]string, len(storage.successor.files))
				for name := range storage.successor.files {
					digest, err := shutdownArtifactDigest(ctx, root, name)
					if err != nil {
						return fmt.Errorf("snapshot successor storage: %w", err)
					}
					storage.digests[name] = digest
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
				if err := requireShutdownDirectoryLocked(root); err != nil {
					return err
				}
				return errors.Join(requireShutdownBackpressure(port, peer), p.checkRunning())
			})
		})
	if err != nil {
		return fmt.Errorf("drain retained continuation: %w", err)
	}
	// superviseDirectory has accepted a clean exit and reaped the candidate.
	// Check the directory before recovery has an opportunity to reclaim files.
	if err := verifyReapedShutdownStorage(ctx, root, storage); err != nil {
		return fmt.Errorf("verify storage after clean shutdown: %w", err)
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

type shutdownStorageObservation struct {
	predecessor, successor tailParityStorage
	retired                []string
	authority              tailParityAuthority
	digests                map[string]string
}

func observeShutdownStorage(ctx context.Context, root *os.Root, published backfillCompaction) (tailParityAuthority, tailParityStorage, error) {
	authority, err := readTailParityAuthority(ctx, root)
	if err != nil {
		return tailParityAuthority{}, tailParityStorage{}, err
	}
	if authority.NextOrdinal != published.covered || authority.CommittedOrdinal != published.committed {
		return tailParityAuthority{}, tailParityStorage{}, fmt.Errorf("selector ordinals [%d, %d) disagree with published compaction [%d, %d)",
			authority.NextOrdinal, authority.CommittedOrdinal, published.covered, published.committed)
	}
	storage, err := observeTailParityStorage(ctx, root, authority.tailParityPoint)
	if err != nil {
		return tailParityAuthority{}, tailParityStorage{}, err
	}
	if storage.tail != authority.Tail {
		return tailParityAuthority{}, tailParityStorage{}, errors.New("selected tail changed during storage observation")
	}
	return authority, storage, nil
}

func requireShutdownRetirementHold(ctx context.Context, client *http.Client, base string, published backfillCompaction, pid int, predecessor tailParityStorage) error {
	pins, err := observeTailParityPins(ctx, client, base, published)
	if err != nil {
		return fmt.Errorf("observe retired predecessor pin: %w", err)
	}
	if pins != 1 {
		return fmt.Errorf("retired predecessor pins = %d, want 1", pins)
	}
	info := predecessor.files[predecessor.tail]
	if info == nil {
		return errors.New("missing predecessor tail identity")
	}
	_, fd, err := observeRetirementDescriptor(ctx, pid, []retirementArtifact{{name: predecessor.tail, info: info}})
	if err != nil {
		return fmt.Errorf("observe held predecessor descriptor: %w", err)
	}
	if fd == "" {
		return fmt.Errorf("candidate has no open descriptor for held predecessor tail %q", predecessor.tail)
	}
	return nil
}

func shutdownArtifactDigest(ctx context.Context, root *os.Root, name string) (string, error) {
	// Even the full seed payload fits within this cap. Each observation
	// closes its file before proceeding so the verifier holds no artifact pins.
	raw, err := readArtifact(ctx, root, name, shutdownPageCap)
	if err != nil {
		return "", err
	}
	return parityDigest(raw), nil
}

// The lock probe uses a fresh open of the pinned directory so it does not
// share the candidate's open file description. Run all checks before unlocking.
func verifyReapedShutdownStorage(ctx context.Context, root *os.Root, observed shutdownStorageObservation) (err error) {
	lock, err := root.Open(".")
	if err != nil {
		return fmt.Errorf("open directory to reacquire lock: %w", err)
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("reacquire directory lock after reaping: %w", err)
	}
	defer func() {
		if unlockErr := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); unlockErr != nil {
			err = errors.Join(err, fmt.Errorf("release reacquired directory lock: %w", unlockErr))
		}
	}()

	if len(observed.retired) == 0 {
		return errors.New("missing predecessor-exclusive artifact observations")
	}
	for _, name := range observed.retired {
		if observed.predecessor.files[name] == nil {
			return fmt.Errorf("missing predecessor identity for %q", name)
		}
	}
	// A process that has been reaped has no live descriptor table to sample.
	// Walk the pinned tree for original names, moved files, and hard links.
	if err := fs.WalkDir(root.FS(), ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if slices.Contains(observed.retired, path) {
			return fmt.Errorf("retired path %q still present", path)
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		for _, name := range observed.retired {
			if os.SameFile(observed.predecessor.files[name], info) {
				return fmt.Errorf("retired artifact %q still linked at %q", name, path)
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("inspect predecessor reclamation: %w", err)
	}
	authority, err := readTailParityAuthority(ctx, root)
	if err != nil {
		return fmt.Errorf("read successor selector after shutdown: %w", err)
	}
	if authority != observed.authority {
		return errors.New("successor selector changed during shutdown")
	}
	current, err := observeTailParityStorage(ctx, root, authority.tailParityPoint)
	if err != nil {
		return fmt.Errorf("observe successor storage after shutdown: %w", err)
	}
	for name, after := range current.files {
		before, digest := observed.successor.files[name], observed.digests[name]
		if before == nil || digest == "" {
			return fmt.Errorf("missing successor observation for %q", name)
		}
		if !os.SameFile(before, after) {
			return fmt.Errorf("successor artifact %q changed identity during shutdown", name)
		}
		got, err := shutdownArtifactDigest(ctx, root, name)
		if err != nil {
			return err
		}
		if got != digest {
			return fmt.Errorf("successor artifact %q changed contents during shutdown", name)
		}
	}
	return ctx.Err()
}

// Read only the response headers until the shutdown probe finishes. Then
// drain the same connection and check the complete predecessor response.
func drainShutdownContinuation(ctx context.Context, base, cursor string, want []row, check func(context.Context, int) error) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	return withBackpressuredContinuation(ctx, deadline, base+queryPath(cursor, 1000, ""), func(conn *net.TCPConn, res *http.Response) error {
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
		if err := conn.SetReadBuffer(1 << 20); err != nil {
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
	})
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

func requireShutdownDirectoryLocked(root *os.Root) (err error) {
	competing, err := root.Open(".")
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
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTailFixture(t, root) })
	const unlocked = "competing open acquired the data directory lock during drain"
	if err := requireShutdownDirectoryLocked(root); err == nil || err.Error() != unlocked {
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
	if err := requireShutdownDirectoryLocked(root); err != nil {
		t.Fatalf("probe with independent lock owner: %v", err)
	}
}

func TestShutdownRetirementHold(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, metrics, want string
		openTail            bool
	}{
		{name: "pin and descriptor", metrics: `{"retired_generation_pins":1}`, openTail: true},
		{name: "missing pin observation", metrics: `{}`, openTail: true, want: `missing or null "retired_generation_pins"`},
		{name: "zero pins", metrics: `{"retired_generation_pins":0}`, openTail: true, want: "retired predecessor pins = 0, want 1"},
		{name: "missing descriptor observation", metrics: `{"retired_generation_pins":1}`, want: `candidate has no open descriptor for held predecessor tail "old-tail"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root, err := os.OpenRoot(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { closeTailFixture(t, root) })
			old := retirementStorage(t, root, []string{"old-manifest", "old-tail"})
			if tc.openTail {
				f, err := root.Open(old.tail)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { closeTailFixture(t, f) })
			}
			status := `{"state":"complete","generation":7,"covered_rows":8,"committed_rows":10,"metrics":` + tc.metrics + "}"
			client := &http.Client{Transport: budgetRoundTripper(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodGet || req.URL.RequestURI() != "/_capacity/compaction?action=status&cutoff=8" {
					t.Fatalf("unexpected pin request: %s %s", req.Method, req.URL)
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(status))}, nil
			})}
			published := backfillCompaction{"complete", 7, 8, 10}
			err = requireShutdownRetirementHold(t.Context(), client, "http://127.0.0.1", published, os.Getpid(), old)
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("hold observation = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestObserveShutdownStorage(t *testing.T) {
	t.Parallel()
	_, root, _, authority, _ := newTailArtifactFixture(t)
	published := backfillCompaction{"complete", 2, authority.NextOrdinal, authority.CommittedOrdinal}
	wrongCutoff := published
	wrongCutoff.covered--
	got, storage, err := observeShutdownStorage(t.Context(), root, wrongCutoff)
	if err == nil || !strings.Contains(err.Error(), "disagree with published compaction") ||
		got != (tailParityAuthority{}) || storage.files != nil {
		t.Fatalf("observation with wrong cutoff = %+v, %+v, %v, want no storage and ordinal mismatch", got, storage, err)
	}
	if err := root.Remove(tailAuthorityName); err != nil {
		t.Fatal(err)
	}
	got, storage, err = observeShutdownStorage(t.Context(), root, published)
	if !errors.Is(err, os.ErrNotExist) || got != (tailParityAuthority{}) || storage.files != nil {
		t.Fatalf("observation without selector = %+v, %+v, %v, want no storage and ErrNotExist", got, storage, err)
	}
}

func TestReapedShutdownStorage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, keep, want                                   string
		move, change, replace, missing, noSnapshot, locked bool
	}{
		{name: "exclusive files reclaimed and shared file intact"},
		{name: "predecessor tail leaked", keep: "old-tail", want: `inspect predecessor reclamation: retired path "old-tail" still present`},
		{name: "predecessor segment leaked", keep: "old-block", want: `inspect predecessor reclamation: retired path "old-block" still present`},
		{name: "predecessor moved into staging", move: true, want: `inspect predecessor reclamation: retired artifact "old-tail" still linked at "stage/tail"`},
		{name: "successor contents changed", change: true, want: fmt.Sprintf("successor artifact %q changed contents during shutdown", tailTestSegment)},
		{name: "successor inode changed", replace: true, want: fmt.Sprintf("successor artifact %q changed identity during shutdown", tailTestSegment)},
		{name: "missing retired inventory", missing: true, want: "missing predecessor-exclusive artifact observations"},
		{name: "missing successor digest", noSnapshot: true, want: fmt.Sprintf("missing successor observation for %q", tailTestSegment)},
		{name: "directory lock still held", locked: true, want: "reacquire directory lock after reaping"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, root, _, authority, _ := newTailArtifactFixture(t)
			ctx := t.Context()
			old := retirementStorage(t, root, []string{"old-manifest", "old-tail", "old-block"})
			published := backfillCompaction{"complete", 2, authority.NextOrdinal, authority.CommittedOrdinal}
			selected, active, err := observeShutdownStorage(ctx, root, published)
			if err != nil {
				t.Fatal(err)
			}
			old.files[tailTestSegment] = active.files[tailTestSegment]
			retired, err := retiredTailParityFiles(root, old, active)
			if err != nil {
				t.Fatal(err)
			}
			if want := []string{"old-block", "old-manifest", "old-tail"}; !slices.Equal(retired, want) {
				t.Fatalf("predecessor-exclusive artifacts = %v, want %v", retired, want)
			}
			observation := shutdownStorageObservation{
				predecessor: old, successor: active, retired: retired, authority: selected,
				digests: make(map[string]string),
			}
			for name := range active.files {
				observation.digests[name], err = shutdownArtifactDigest(ctx, root, name)
				if err != nil {
					t.Fatal(err)
				}
			}
			if tc.move {
				if err := root.Mkdir("stage", 0700); err != nil {
					t.Fatal(err)
				}
				if err := root.Rename("old-tail", "stage/tail"); err != nil {
					t.Fatal(err)
				}
			}
			// Hold predecessor inodes so a filesystem cannot reuse a
			// retired identity for a replacement created by this fixture.
			for _, name := range retired {
				f, err := root.Open(name)
				if err != nil && tc.move && name == "old-tail" {
					f, err = root.Open("stage/tail")
				}
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { closeTailFixture(t, f) })
			}
			for _, name := range retired {
				if name != tc.keep && !(tc.move && name == "old-tail") {
					if err := root.Remove(name); err != nil {
						t.Fatal(err)
					}
				}
			}
			if tc.replace {
				if err := root.Rename(tailTestSegment, "original-block"); err != nil {
					t.Fatal(err)
				}
			}
			if tc.change || tc.replace {
				if err := root.WriteFile(tailTestSegment, []byte("replacement"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.missing {
				observation.retired = nil
			}
			if tc.noSnapshot {
				delete(observation.digests, tailTestSegment)
			}
			if tc.locked {
				owner, err := root.Open(".")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { closeTailFixture(t, owner) })
				if err := syscall.Flock(int(owner.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
					t.Fatal(err)
				}
			}
			err = verifyReapedShutdownStorage(ctx, root, observation)
			switch {
			case tc.locked:
				if !errors.Is(err, syscall.EWOULDBLOCK) || !strings.HasPrefix(err.Error(), tc.want) {
					t.Fatalf("lock probe = %v, want %q wrapping EWOULDBLOCK", err, tc.want)
				}
			case tc.want == "" && err != nil, tc.want != "" && (err == nil || err.Error() != tc.want):
				t.Fatalf("reaped storage verification = %v, want %q", err, tc.want)
			}
			if !tc.locked {
				if err := requireShutdownDirectoryLocked(root); err == nil || err.Error() != "competing open acquired the data directory lock during drain" {
					t.Fatalf("lock was not released after verification: %v", err)
				}
			}
		})
	}
}
