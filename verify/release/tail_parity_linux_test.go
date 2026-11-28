package main

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	bogclient "bog/internal/client"
	"bog/verify/capacityevidence"
	querymodel "bog/verify/query-model"
)

func tailOnlyParity(ctx context.Context, binary, root string) (proof tailParityProof, err error) {
	fixtures, err := tailParityFixtures()
	if err != nil {
		return proof, err
	}
	rows := parityRows()
	dir, _, err := makeRestoredFixture(ctx, root, rows)
	if err != nil {
		return proof, fmt.Errorf("prepare v1 tail-only restore: %w", err)
	}
	for _, name := range []string{"bundle", "source"} {
		if err := os.RemoveAll(filepath.Join(root, name)); err != nil {
			return proof, fmt.Errorf("remove initial %s: %w", name, err)
		}
	}
	input, err := readTailParityImage(ctx, dir, "BOGBAK01")
	if err != nil {
		return proof, err
	}
	firstGeneration := input.Generation
	var log querymodel.Log
	log.Append(rows)
	client := &http.Client{
		Transport:     &http.Transport{},
		Timeout:       5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	defer client.CloseIdleConnections()
	var archived [tailParityCycles]string
	for i, fixture := range fixtures {
		if i > 0 {
			fixture.input = proof.Cycles[i-1].Export
		}
		if err := checkTailParityPoint(input, fixture.input); err != nil {
			return proof, fmt.Errorf("cycle %d input: %w", i+1, err)
		}
		exported := filepath.Join(root, fmt.Sprintf("export-%d", i+1))
		restored := filepath.Join(root, fmt.Sprintf("restore-%d", i+1))
		archived[i] = filepath.Join(root, fmt.Sprintf("archived-%d", i+1))
		cycle, err := runTailParityCycle(ctx, binary, dir, exported, archived[i], restored, client, &log, fixture, input)
		proof.Cycles[i] = cycle
		if err != nil {
			return proof, fmt.Errorf("cycle %d: %w", i+1, err)
		}
		if err := validateTailParityCycle(cycle, fixture, firstGeneration); err != nil {
			return proof, fmt.Errorf("cycle %d: %w", i+1, err)
		}
		dir, input = restored, cycle.Export
	}
	for i, path := range archived {
		got, err := readTailParityImage(ctx, path, "BOGBAK02")
		if err != nil {
			return proof, fmt.Errorf("reverify archived export %d: %w", i+1, err)
		}
		if err := checkTailParityPoint(got, proof.Cycles[i].Export); err != nil {
			return proof, fmt.Errorf("archived export %d changed: %w", i+1, err)
		}
	}
	return proof, nil
}

func runTailParityCycle(ctx context.Context, binary, dir, exported, archived, restored string, client *http.Client,
	log *querymodel.Log, fixture tailCycleFixture, input tailParityPoint,
) (cycle tailParityCycle, err error) {
	cycle.Input = input
	flags := []string{fmt.Sprintf("-query-max-response-bytes=%d", restoredPageCap)}
	data, err := os.OpenRoot(dir)
	if err != nil {
		return cycle, err
	}
	defer func() { err = errors.Join(err, data.Close()) }()
	if err := requireTailOnlyDirectory(data); err != nil {
		return cycle, err
	}
	var successor tailParityStorage
	err = serveDirectoryWithCandidate(ctx, binary, dir, client, flags, "", (*candidateProcess).crashStop, func(ctx context.Context, base string, pid int) error {
		if _, err := requireTailParityStatus(ctx, client, base, input.NextOrdinal, input.CommittedOrdinal); err != nil {
			return fmt.Errorf("writable restore status: %w", err)
		}
		predecessor, err := observeTailParityStorage(ctx, data, input)
		if err != nil {
			return fmt.Errorf("observe selected predecessor: %w", err)
		}
		pager, err := newObservedOraclePager(base, client, restoredPageCap)
		if err != nil {
			return err
		}
		var published backfillCompaction
		held, terminal, err := verifyTailParityHeld(ctx, pager, log, func(ctx context.Context, _ string) error {
			for i, batch := range fixture.batches {
				status, appendErr := appendTailParityRows(ctx, client, base, log, batch)
				cycle.AppendStatuses[i] = status
				if appendErr != nil {
					return fmt.Errorf("append batch %d: %w", i+1, appendErr)
				}
			}
			previous, err := requireTailParityStatus(ctx, client, base, input.NextOrdinal, fixture.export.CommittedOrdinal)
			if err != nil {
				return err
			}
			published, err = publishReleaseGeneration(ctx, client, base, fixture.export.NextOrdinal, fixture.export.CommittedOrdinal, &previous)
			return err
		})
		if err != nil {
			return fmt.Errorf("held walk across append and compaction: %w", err)
		}
		successor, err = observeTailParityStorage(ctx, data, fixture.export)
		if err != nil {
			return fmt.Errorf("observe selected successor: %w", err)
		}
		if err := retireTailParityCapture(ctx, client, base, pager, terminal, published, data, pid, predecessor, successor); err != nil {
			return err
		}
		cycle.HeldRows = held.Rows
		cycle.FreshRows, err = walkTailParity(ctx, pager, log)
		if err != nil {
			return fmt.Errorf("fresh walk after compaction: %w", err)
		}
		return nil
	})
	if err != nil {
		return cycle, fmt.Errorf("serve writable tail and compact: %w", err)
	}

	tail, err := exportTailParityCycle(ctx, data, exported, fixture, &cycle)
	if err != nil {
		return cycle, err
	}
	defer func() { err = errors.Join(err, tail.file.Close()) }()
	if successor.manifest != "bog-checkpoint-"+cycle.Export.Generation ||
		successor.tail != tail.authority.Tail || !os.SameFile(successor.files[successor.tail], tail.info) {
		return cycle, errors.New("export selected different storage than the observed replacement")
	}
	if err := backup.Restore(ctx, exported, restored); err != nil {
		return cycle, fmt.Errorf("restore v2 export: %w", err)
	}
	got, err := readTailParityImage(ctx, restored, "BOGBAK02")
	if err != nil {
		return cycle, fmt.Errorf("verify restored export: %w", err)
	}
	if err := checkTailParityPoint(got, cycle.Export); err != nil {
		return cycle, fmt.Errorf("restore differs from export: %w", err)
	}
	// Move the verified bundle before either startup and keep its bytes
	// available for independent reverification after the next cycle.
	if err := os.Rename(exported, archived); err != nil {
		return cycle, fmt.Errorf("archive source export: %w", err)
	}
	restoredRoot, err := os.OpenRoot(restored)
	if err != nil {
		return cycle, err
	}
	if err := errors.Join(requireTailOnlyDirectory(restoredRoot), restoredRoot.Close()); err != nil {
		return cycle, err
	}

	// The restarted live directory must extend the exported tail at its
	// recovered ordinal. A separate oracle keeps this probe append out of
	// the history consumed by the next writable restore.
	var restartedLog querymodel.Log
	restartRows, err := log.Match(log.HighWater(), querymodel.Filter{Start: fixtureTime(0), End: fixtureTime(3)})
	if err != nil {
		return cycle, err
	}
	if len(restartRows) != log.HighWater() {
		return cycle, errors.New("restart oracle must cover every appended row")
	}
	// Match returns query order. Rebuild append order using the oracle's
	// append ordinals before adding another timestamp tie.
	slices.SortFunc(restartRows, func(a, b querymodel.Entry) int { return cmp.Compare(a.Ordinal, b.Ordinal) })
	for _, r := range restartRows {
		restartedLog.Append([]querymodel.Row{r.Row})
	}
	probe := []row{fixtureWireRow(1, fmt.Sprintf("restart after ordinal %d [match]", cycle.Export.CommittedOrdinal))}
	err = serveDirectoryWithStop(ctx, binary, dir, client, flags, (*candidateProcess).crashStop, func(ctx context.Context, base string) error {
		pager, err := newReleasePager(base, client, restoredPageCap)
		if err != nil {
			return err
		}
		p := cycle.Export
		if _, err := requireTailParityStatus(ctx, client, base, p.NextOrdinal, p.CommittedOrdinal); err != nil {
			return err
		}
		cycle.RestartRows, err = walkTailParity(ctx, pager, log)
		if err != nil {
			return err
		}
		cycle.RestartAppendStatus, err = appendTailParityRows(ctx, client, base, &restartedLog, probe)
		if err != nil {
			return err
		}
		status, err := requireTailParityStatus(ctx, client, base, p.NextOrdinal, p.CommittedOrdinal+len(probe))
		if err != nil {
			return err
		}
		cycle.RestartNextOrdinal = status.committed
		_, err = walkTailParity(ctx, pager, &restartedLog)
		if err != nil {
			return fmt.Errorf("walk after restarted append: %w", err)
		}
		return nil
	})
	if err != nil {
		return cycle, fmt.Errorf("restart advanced directory and append: %w", err)
	}
	probeFrame, err := fixtureFrame(probe)
	if err != nil {
		return cycle, err
	}
	if err := verifyParityRestartAppend(ctx, data, tail, probeFrame, len(probe)); err != nil {
		return cycle, fmt.Errorf("verify selected tail after restart append: %w", err)
	}
	err = serveDirectory(ctx, binary, restored, client, flags, func(ctx context.Context, base string) error {
		p := cycle.Export
		if _, err := requireTailParityStatus(ctx, client, base, p.NextOrdinal, p.CommittedOrdinal); err != nil {
			return err
		}
		pager, err := newReleasePager(base, client, restoredPageCap)
		if err != nil {
			return err
		}
		cycle.RestoredRows, err = walkTailParity(ctx, pager, log)
		return err
	})
	if err != nil {
		return cycle, fmt.Errorf("start separate v2 restore after source relocation: %w", err)
	}
	return cycle, nil
}

func verifyTailParityHeld(ctx context.Context, pager *releasePager, log *querymodel.Log, replace querymodel.AfterCapture) (querymodel.WireStats, querymodel.WireRequest, error) {
	var terminal querymodel.WireRequest
	var original []byte
	fetch := func(ctx context.Context, req querymodel.WireRequest) (querymodel.WirePage, error) {
		var body []byte
		page, _, err := fetchOraclePage(context.WithValue(ctx, oracleBodyKey{}, &body), pager, req)
		if err != nil {
			return page, err
		}
		if body == nil || int64(len(body)) != page.BodyBytes {
			return page, errors.New("missing complete tail parity response bytes")
		}
		page.EncodedBody = body
		return page, nil
	}
	walk := restoredWalk(pager, "", []int{1, 3})
	stats, err := querymodel.VerifyWireWalk(ctx, log, walk, func(ctx context.Context, req querymodel.WireRequest) (querymodel.WirePage, error) {
		page, err := fetch(ctx, req)
		if err == nil && page.NextCursor == "" {
			terminal, original = req, page.EncodedBody
		}
		return page, err
	}, replace)
	if err != nil {
		return stats, terminal, err
	}
	if terminal.Cursor == "" || original == nil {
		return stats, terminal, errors.New("held walk did not observe a terminal continuation with response bytes")
	}
	replay, err := fetch(ctx, terminal)
	if err != nil {
		return stats, terminal, fmt.Errorf("retry held terminal response: %w", err)
	}
	if replay.NextCursor != "" || !bytes.Equal(replay.EncodedBody, original) {
		return stats, terminal, errors.New("retried terminal response differs from the original encoded body or framing")
	}
	return stats, terminal, ctx.Err()
}

// Names come from the independent selector and its manifest. The live
// observation only reads the pinned directory and acquires no storage pins.
type tailParityStorage struct {
	manifest, tail string
	files          map[string]os.FileInfo
}

func observeTailParityStorage(ctx context.Context, root *os.Root, want tailParityPoint) (tailParityStorage, error) {
	a, err := readTailParityAuthority(ctx, root)
	if err != nil {
		return tailParityStorage{}, err
	}
	if err := checkTailParityPoint(a.tailParityPoint, want); err != nil {
		return tailParityStorage{}, err
	}
	storage := tailParityStorage{manifest: "bog-checkpoint-" + a.Generation, tail: a.Tail, files: make(map[string]os.FileInfo)}
	raw, err := readArtifact(ctx, root, storage.manifest, tailManifestLimit)
	if err != nil {
		return tailParityStorage{}, err
	}
	m, digest, err := checkpoint.DecodeManifest(raw)
	if err != nil {
		return tailParityStorage{}, err
	}
	if a.Generation != hex.EncodeToString(digest[:]) || m.CoveredOffset != a.CoveredOffset || m.NextOrdinal != uint64(a.NextOrdinal) {
		return tailParityStorage{}, errors.New("observed manifest disagrees with writable selector")
	}
	names := []string{storage.tail, storage.manifest}
	for _, segment := range m.Segments {
		names = append(names, segment.Name)
	}
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return tailParityStorage{}, err
		}
		info, err := root.Lstat(name)
		if err != nil {
			return tailParityStorage{}, fmt.Errorf("stat selected artifact %q: %w", name, err)
		}
		if !info.Mode().IsRegular() {
			return tailParityStorage{}, fmt.Errorf("selected artifact %q is not a regular file", name)
		}
		storage.files[name] = info
	}
	st, ok := storage.files[storage.tail].Sys().(*syscall.Stat_t)
	if !ok || uint64(st.Dev) != a.TailDevice || uint64(st.Ino) != a.TailInode {
		return tailParityStorage{}, errors.New("observed tail identity disagrees with writable selector")
	}
	return storage, nil
}

func retiredTailParityFiles(root *os.Root, old, active tailParityStorage) ([]string, error) {
	if old.manifest == active.manifest || old.tail == active.tail || os.SameFile(old.files[old.tail], active.files[active.tail]) {
		return nil, errors.New("replacement did not select a different checkpoint and physical tail")
	}
	var retired []string
	for name, before := range old.files {
		if after, shared := active.files[name]; shared {
			if !os.SameFile(before, after) {
				return nil, fmt.Errorf("shared artifact %q changed identity during replacement", name)
			}
			continue
		}
		info, err := root.Lstat(name)
		if err != nil {
			return nil, fmt.Errorf("stat retired artifact %q while capture is held: %w", name, err)
		}
		if !os.SameFile(before, info) {
			return nil, fmt.Errorf("retired artifact %q changed identity while capture is held", name)
		}
		retired = append(retired, name)
	}
	slices.Sort(retired)
	return retired, nil
}

func remainingTailParityArtifact(ctx context.Context, root *os.Root, pid int, retired []string, before map[string]os.FileInfo) (string, error) {
	var remaining string
	// Search the pinned tree for aliases, including artifacts moved into staging.
	err := fs.WalkDir(root.FS(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			if path != "." && errors.Is(walkErr, fs.ErrNotExist) {
				return nil // A staging directory can disappear during cleanup.
			}
			return walkErr
		}
		if slices.Contains(retired, path) {
			remaining = fmt.Sprintf("retired path %q still present", path)
			return fs.SkipAll
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		for _, name := range retired {
			if os.SameFile(before[name], info) {
				remaining = fmt.Sprintf("retired artifact %q still linked at %q", name, path)
				return fs.SkipAll
			}
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("inspect pinned data directory: %w", err)
	}
	if remaining != "" {
		return remaining, nil
	}

	artifacts := make([]retirementArtifact, 0, len(retired))
	for _, name := range retired {
		artifacts = append(artifacts, retirementArtifact{name: name, info: before[name]})
	}
	artifact, fd, err := observeRetirementDescriptor(ctx, pid, artifacts)
	if err != nil {
		return "", err
	}
	if fd != "" {
		return fmt.Sprintf("retired artifact %q still open in candidate descriptor %s", artifact, fd), nil
	}
	return "", nil
}

func observeTailParityPins(ctx context.Context, client *http.Client, base string, want backfillCompaction) (uint64, error) {
	got, err := readCompactionObservation(ctx, client, base, http.MethodGet, "status", want.covered, true)
	if err != nil {
		return 0, err
	}
	if got.backfillCompaction != want || got.state != "complete" {
		return 0, fmt.Errorf("published compaction changed during pin observation: got %+v, want %+v", got.backfillCompaction, want)
	}
	return got.retiredGenerationPins, nil
}

func retireTailParityCapture(ctx context.Context, client *http.Client, base string, pager *releasePager, terminal querymodel.WireRequest,
	published backfillCompaction, root *os.Root, pid int, predecessor, successor tailParityStorage,
) error {
	retired, err := retiredTailParityFiles(root, predecessor, successor)
	if err != nil {
		return err
	}
	pins, err := observeTailParityPins(ctx, client, base, published)
	if err != nil {
		return fmt.Errorf("observe held predecessor pins: %w", err)
	}
	if pins == 0 {
		return errors.New("expected a positive retired generation pin count before capture retirement")
	}
	params := url.Values{"action": {"retire"}, "cursor": {terminal.Cursor}}
	res, err := request(ctx, client, http.MethodPost, base+"/_capacity/compaction?"+params.Encode(), nil)
	if err != nil {
		return fmt.Errorf("retire held capture: %w", err)
	}
	if res.status != http.StatusNoContent || len(res.body) != 0 || res.cursorHeader {
		return fmt.Errorf("retire held capture: expected empty HTTP 204, got HTTP %d with %d body bytes", res.status, len(res.body))
	}

	drain, cancel := context.WithTimeout(ctx, capacityevidence.DrainBudget)
	err = waitTailParityObservation(drain, "retired generation pin drain", func(ctx context.Context) (bool, error) {
		pins, err := observeTailParityPins(ctx, client, base, published)
		return pins == 0, err
	})
	cancel()
	if err != nil {
		return err
	}
	reclaim, cancel := context.WithTimeout(ctx, capacityevidence.ReclamationBudget)
	defer cancel()
	var remaining string
	if err := waitTailParityObservation(reclaim, "retired artifact reclamation", func(ctx context.Context) (bool, error) {
		found, err := remainingTailParityArtifact(ctx, root, pid, retired, predecessor.files)
		if err != nil {
			return false, err
		}
		remaining = found
		return remaining == "", nil
	}); err != nil {
		if remaining != "" {
			return fmt.Errorf("%w (%s)", err, remaining)
		}
		return err
	}

	_, response, err := fetchOraclePage(ctx, pager, terminal)
	// The client returns the HTTP error directly after a fully read and
	// closed failure response. A joined error indicates an I/O failure.
	status, ok := err.(*bogclient.HTTPError)
	if !ok || !errors.Is(status, bogclient.ErrCaptureExpired) || response == nil ||
		len(response.Header.Values(nextCursor)) != 0 || hasCursorTrailer(response) {
		return fmt.Errorf("resuming retired capture: expected clean HTTP 410, got %v", err)
	}
	return ctx.Err()
}

func waitTailParityObservation(ctx context.Context, label string, observe func(context.Context) (bool, error)) error {
	ticker := time.NewTicker(candidatePollInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		done, err := observe(ctx)
		if err != nil {
			if canceled := ctx.Err(); canceled != nil && errors.Is(err, canceled) {
				break
			}
			return fmt.Errorf("observe %s: %w", label, err)
		}
		if done && ctx.Err() == nil {
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
		}
	}
	return fmt.Errorf("wait for %s: %w", label, ctx.Err())
}

func appendTailParityRows(ctx context.Context, client *http.Client, base string, log *querymodel.Log, batch []row) (int, error) {
	modelRows := make([]querymodel.Row, 0, len(batch))
	for _, r := range batch {
		ts, err := time.Parse(time.RFC3339Nano, r.TS)
		if err != nil {
			return 0, err
		}
		modelRows = append(modelRows, querymodel.Row{Time: ts, Line: r.Line})
	}
	status, err := ingest(ctx, client, base, batch)
	if err == nil {
		log.Append(modelRows)
	}
	return status, err
}

func walkTailParity(ctx context.Context, pager *releasePager, log *querymodel.Log) (int, error) {
	walk := restoredWalk(pager, "", []int{3, 1, 10})
	stats, err := querymodel.VerifyWireWalk(ctx, log, walk, parityFetch(pager), nil)
	return stats.Rows, err
}

func requireTailParityStatus(ctx context.Context, client *http.Client, base string, covered, committed int) (backfillCompaction, error) {
	s, err := readBackfillCompaction(ctx, client, base, http.MethodGet, "status", 0)
	if err == nil && (s.state != "complete" || s.covered != covered || s.committed != committed) {
		err = fmt.Errorf("compaction status %+v, want complete with covered=%d and committed=%d", s, covered, committed)
	}
	return s, err
}

func TestTailParityV1Fixture(t *testing.T) {
	dir, _, err := makeRestoredFixture(t.Context(), t.TempDir(), parityRows())
	if err != nil {
		t.Fatal(err)
	}
	point, err := readTailParityImage(t.Context(), dir, "BOGBAK01")
	if err != nil {
		t.Fatal(err)
	}
	fixtures, err := tailParityFixtures()
	if err != nil {
		t.Fatal(err)
	}
	if err := checkTailParityPoint(point, fixtures[0].input); err != nil {
		t.Fatal(err)
	}
}

func TestTailParityHeldTerminalReplay(t *testing.T) {
	t.Parallel()
	const token = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	const ndjson = "Content-Type: application/x-ndjson\r\n"
	first := fmt.Sprintf("{\"ts\":%q,\"line\":\"first\"}\n", fixtureStart)
	last := fmt.Sprintf("{\"ts\":%q,\"line\":\"last\"}\n", fixtureStart)
	for _, tc := range []struct{ name, replay, want string }{
		{"identical", lengthResponse(200, ndjson, last), ""},
		{"same rows different bytes", lengthResponse(200, ndjson, strings.Replace(last, ":\"last\"", ": \"last\"", 1)), "retried terminal response differs"},
		{"retry continues", lengthResponse(200, ndjson+nextCursor+": "+token+"\r\n", last), "retry held terminal response"},
		{"retry expired", lengthResponse(410, "", "query capture expired\n"), "retry held terminal response"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			script := scriptedBackfillTransport(t,
				[]backfillRequest{{path: "/query", limit: 1}, {path: "/query", limit: 3, cursor: token}, {path: "/query", limit: 3, cursor: token}},
				[]string{lengthResponse(200, ndjson+nextCursor+": "+token+"\r\n", first), lengthResponse(200, ndjson, last), tc.replay})
			calls := 0
			replaced := false
			raw := &http.Client{Transport: budgetRoundTripper(func(req *http.Request) (*http.Response, error) {
				calls++
				if replaced != (calls > 1) {
					t.Fatalf("query %d, replacement observed = %t", calls, replaced)
				}
				return script.RoundTrip(req)
			})}
			pager, err := newObservedOraclePager("http://127.0.0.1", raw, restoredPageCap)
			if err != nil {
				t.Fatal(err)
			}
			var log querymodel.Log
			log.Append([]querymodel.Row{{Time: fixtureTime(0), Line: "first"}, {Time: fixtureTime(0), Line: "last"}})
			stats, terminal, err := verifyTailParityHeld(t.Context(), pager, &log, func(_ context.Context, cursor string) error {
				if calls != 1 || cursor != token || replaced {
					t.Fatal("replacement did not follow the first continuation")
				}
				replaced = true
				log.Append([]querymodel.Row{{Time: fixtureTime(0), Line: "new row"}})
				return nil
			})
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("got %v, want %q", err, tc.want)
				}
			} else if err != nil || stats.Pages != 2 || stats.Rows != 2 || terminal.Limit != 3 || terminal.Cursor != token {
				t.Fatalf("held walk = %+v, terminal limit = %d, error = %v", stats, terminal.Limit, err)
			}
		})
	}
}

func TestTailParityRetirementAndReclamation(t *testing.T) {
	t.Parallel()
	const token = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	const statusPath = "/_capacity/compaction?action=status&cutoff=8"
	const retirePath = "/_capacity/compaction?action=retire&cursor=" + token
	const attempt = "bog-wal-stage-unrelated"
	generation := backfillCompaction{"complete", 7, 8, 10}
	status := func(metrics string) string {
		return lengthResponse(200, "", fmt.Sprintf(
			`{"state":"complete","generation":7,"covered_rows":8,"committed_rows":10,"metrics":%s}`, metrics))
	}
	const one = `{"retired_generation_pins":1}`
	const zero = `{"retired_generation_pins":0}`
	for _, tc := range []struct {
		name, held, drained, retired, resume, keep, want string
		early, stageTail, leakTail                       bool
		wantCalls                                        int
	}{
		{name: "positive then pending then zero", held: one, drained: zero, retired: backfillTestAck, resume: "query capture expired\n"},
		{name: "missing held metric", held: "{}", want: `missing or null "retired_generation_pins"`, wantCalls: 1},
		{name: "duplicate held metric", held: `{"retired_generation_pins":0,"retired_generation_pins":1}`, want: "duplicate", wantCalls: 1},
		{name: "zero before retirement", held: zero, want: "expected a positive retired generation pin count"},
		{name: "reclaimed before retirement", early: true, want: "while capture is held"},
		{name: "retirement refused", held: one, retired: lengthResponse(404, "", "hidden"), want: "expected empty HTTP 204"},
		{name: "missing final metric", held: one, retired: backfillTestAck, drained: "{}", want: `missing or null "retired_generation_pins"`, wantCalls: 4},
		{name: "null final metric", held: one, retired: backfillTestAck, drained: `{"retired_generation_pins":null}`, want: `missing or null "retired_generation_pins"`, wantCalls: 4},
		{name: "mis-cased final metric", held: one, retired: backfillTestAck, drained: `{"Retired_Generation_Pins":0}`, want: `missing or null "retired_generation_pins"`, wantCalls: 4},
		{name: "pin never drains", held: one, retired: backfillTestAck, drained: one, want: "wait for retired generation pin drain"},
		{name: "tail left behind", held: one, retired: backfillTestAck, drained: zero, keep: "old-tail", want: "wait for retired artifact reclamation"},
		{name: "segment left behind", held: one, retired: backfillTestAck, drained: zero, keep: "old-block", want: "wait for retired artifact reclamation"},
		{name: "tail moved into staging", held: one, retired: backfillTestAck, drained: zero, stageTail: true, want: `retired artifact "old-tail" still linked at "bog-wal-stage-fixture/tail"`},
		{name: "unlinked tail descriptor leaked", held: one, retired: backfillTestAck, drained: zero, leakTail: true, want: `retired artifact "old-tail" still open in candidate descriptor`},
		{name: "resume must be a clean 410", held: one, retired: backfillTestAck, drained: zero, resume: "other error\n", want: "expected clean HTTP 410"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			root, err := os.OpenRoot(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { closeTailFixture(t, root) })
			storage := func(manifest, tail, block string) tailParityStorage {
				files := make(map[string]os.FileInfo)
				for _, name := range []string{manifest, tail, block, "shared-block"} {
					path := filepath.Join(dir, name)
					if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
						writeFixture(t, path, []byte("owned storage"))
					} else if err != nil {
						t.Fatal(err)
					}
					info, err := root.Lstat(name)
					if err != nil {
						t.Fatal(err)
					}
					files[name] = info
				}
				return tailParityStorage{manifest, tail, files}
			}
			old := storage("old-manifest", "old-tail", "old-block")
			active := storage("new-manifest", "new-tail", "new-block")
			// Keep fixture inode numbers allocated outside the scanned tree.
			// Parallel tests in this process can otherwise open a reused inode
			// and be misidentified as holding a predecessor descriptor.
			inodes := t.TempDir()
			for _, name := range []string{old.manifest, old.tail, "old-block"} {
				if err := os.Link(filepath.Join(dir, name), filepath.Join(inodes, name)); err != nil {
					t.Fatal(err)
				}
			}
			writeFixture(t, filepath.Join(dir, attempt), nil)
			if tc.early {
				removeTailFixture(t, dir, old.tail)
			}
			calls := 0
			raw := &http.Client{Transport: budgetRoundTripper(func(req *http.Request) (*http.Response, error) {
				calls++
				path, method := statusPath, http.MethodGet
				var reply string
				switch calls {
				case 1:
					reply = status(tc.held)
				case 2:
					path, method, reply = retirePath, http.MethodPost, tc.retired
				case 3:
					reply = status(one)
				case 4:
					reply = status(tc.drained)
					if tc.drained == zero {
						if tc.leakTail {
							file, err := root.Open(old.tail)
							if err != nil {
								t.Fatal(err)
							}
							t.Cleanup(func() { closeTailFixture(t, file) })
						}
						if tc.stageTail {
							const stage = "bog-wal-stage-fixture"
							if err := root.Mkdir(stage, 0700); err != nil {
								t.Fatal(err)
							}
							if err := root.Rename(old.tail, stage+"/tail"); err != nil {
								t.Fatal(err)
							}
						}
						for _, name := range []string{old.manifest, old.tail, "old-block"} {
							if name == tc.keep || (tc.stageTail && name == old.tail) {
								continue
							}
							removeTailFixture(t, dir, name)
						}
					}
				default:
					if tc.drained == one {
						reply = status(one)
					} else if calls == 5 && tc.resume != "" {
						path = "/query?" + req.URL.Query().Encode()
						if req.URL.Query().Get("cursor") != token || req.URL.Query().Get("limit") != "3" {
							t.Fatal("retired resume changed the terminal request")
						}
						reply = lengthResponse(410, "", tc.resume)
					}
				}
				if reply == "" || req.Method != method || req.URL.RequestURI() != path {
					t.Fatalf("unexpected request %d: %s %s", calls, req.Method, req.URL.Path)
				}
				return http.ReadResponse(bufio.NewReader(strings.NewReader(reply)), req)
			})}
			const base = "http://127.0.0.1"
			pager, err := newReleasePager(base, raw, restoredPageCap)
			if err != nil {
				t.Fatal(err)
			}
			terminal := querymodel.WireRequest{Filter: restoredWalk(pager, "", []int{3}).Filter, Limit: 3, Cursor: token, MaxPageBytes: restoredPageCap}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			err = retireTailParityCapture(ctx, raw, base, pager, terminal, generation, root, os.Getpid(), old, active)
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("got %v, want %q", err, tc.want)
				}
			} else if err != nil || calls != 5 {
				t.Fatalf("retirement = %v, made %d requests, want 5", err, calls)
			}
			if tc.wantCalls != 0 && calls != tc.wantCalls {
				t.Fatalf("retirement = %v, made %d requests, want %d", err, calls, tc.wantCalls)
			}
			if tc.stageTail || tc.leakTail {
				if !errors.Is(err, context.DeadlineExceeded) || calls != 4 {
					t.Fatalf("reclamation = %v after %d requests, want deadline before resume", err, calls)
				}
				if _, err := root.Lstat(old.tail); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("original tail path must be absent, stat = %v", err)
				}
			}
			for _, name := range []string{attempt, "shared-block", active.manifest, active.tail, "new-block"} {
				if _, err := root.Lstat(name); err != nil {
					t.Errorf("active or unrelated file %q was disturbed: %v", name, err)
				}
			}
		})
	}
}

func TestObserveTailParityStorage(t *testing.T) {
	t.Parallel()
	dir, root, fixture, authority := newTailFileFixture(t)
	fixture.input.Generation = strings.Repeat("2", 64)
	authority.Generation = writeTailArtifactFixture(t, dir, fixture, fixture.input)
	writeTailSelector(t, dir, authority)
	got, err := observeTailParityStorage(t.Context(), root, authority.tailParityPoint)
	if err != nil {
		t.Fatal(err)
	}
	if got.manifest != "bog-checkpoint-"+authority.Generation || got.tail != authority.Tail ||
		len(got.files) != 3 || got.files[tailTestSegment] == nil || got.files[got.manifest] == nil || got.files[got.tail] == nil {
		t.Fatalf("unexpected protocol-owned storage: %+v", got)
	}
	authority.TailInode++
	writeTailSelector(t, dir, authority)
	if _, err := observeTailParityStorage(t.Context(), root, authority.tailParityPoint); err == nil ||
		!strings.Contains(err.Error(), "tail identity disagrees") {
		t.Fatalf("substituted tail error = %v", err)
	}
}
