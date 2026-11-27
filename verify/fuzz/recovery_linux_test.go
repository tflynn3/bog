package fuzz

import (
	"bytes"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
	"bog/internal/wal"
)

const (
	walFile           = "bog.wal"
	commitFile        = "bog.commit"
	walOrphan         = ".bog-commit-00000000000000000000000000000001"
	maxRecoverySuffix = 8 << 10
)

var recoveryEnd = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)

type recoveryFixture struct {
	disk        files
	firstEnd    int
	rows        []store.Row
	first, full replayView
}

func newRecoveryFixture(t testing.TB, dir string) recoveryFixture {
	t.Helper()
	batches := replayHistory(17)
	var ledger committedBatchLedger
	var s store.Store
	log, err := wal.Open(dir, &s)
	must(t, err)
	func() {
		defer func() { must(t, log.Close()) }()
		for _, rows := range batches[:2] {
			appendReplayBatch(t, log, &ledger, rows)
		}
	}()
	first, full := fixtureReplayView(t, &ledger, 1, 1), fixtureReplayView(t, &ledger, 2, 2)
	disk := readFiles(t, dir)
	if !bytes.Equal(disk[walFile], full.wal) {
		t.Fatal("acknowledged fixture WAL differs from the ledger frames")
	}
	return recoveryFixture{
		disk: disk, firstEnd: len(first.wal), rows: slices.Concat(batches[:2]...),
		first: first, full: full,
	}
}

func appendRows(t testing.TB, log *wal.Log, rows []store.Row) {
	t.Helper()
	var payload bytes.Buffer
	enc := json.NewEncoder(&payload)
	for _, row := range rows {
		must(t, enc.Encode(row))
	}
	must(t, log.Append(payload.Bytes(), rows))
}

func (f recoveryFixture) withSuffix(suffix []byte) []byte {
	b := slices.Clone(f.disk[walFile][:f.firstEnd])
	return append(b, suffix...)
}

func markerAt(marker []byte, end uint64) []byte {
	return changed(marker, func(b []byte) {
		le.PutUint64(b[8:16], end)
		le.PutUint64(b[16:24], ^end)
	})
}

type suffixCase struct {
	name  string
	data  []byte
	phase string
}

func recoverySuffixes(f recoveryFixture) []suffixCase {
	suffix := f.disk[walFile][f.firstEnd:]
	badRow := changed(suffix, func(b []byte) {
		// Damage the final row after an otherwise valid row in the same
		// frame, and keep both checksums valid so decoding sees the error.
		b[bytes.LastIndexByte(b, '}')] = '!'
		le.PutUint32(b[8:12], crc32.Checksum(b[frameHeader:], crcTable))
		fixFrameHeader(b)
	})
	return []suffixCase{
		{"short-header", suffix[:frameHeader-1], "incomplete committed frame header"},
		{"header-crc", flip(suffix, 12), "invalid WAL header checksum"},
		{"oversized-frame", frameLength(suffix, store.MaxBatch+1), "invalid WAL header"},
		{"short-payload", suffix[:len(suffix)-1], "incomplete committed frame payload"},
		{"payload-crc", flip(suffix, frameHeader), "invalid WAL checksum"},
		{"signed-invalid-row", badRow, "invalid WAL payload"},
	}
}

func runWALRecovery(t *testing.T) {
	dir := t.TempDir()
	f := newRecoveryFixture(t, dir)
	for _, tc := range recoverySuffixes(f) {
		t.Run(tc.name, func(t *testing.T) {
			input := f.withSuffix(tc.data)
			damaged := files{
				walFile: input, commitFile: markerAt(f.disk[commitFile], uint64(len(input))),
				walOrphan: []byte("abandoned marker"),
			}
			path := filepath.Join(t.TempDir(), "owner")
			writeFiles(t, path, damaged)
			assertWALRejected(t, path, damaged, tc.phase)
			// As an operator-directed repair, restore the preceding durable
			// frame boundary and verify that recovery can still use it.
			marker := markerAt(f.disk[commitFile], uint64(f.firstEnd))
			must(t, os.WriteFile(filepath.Join(path, commitFile), marker, 0600))
			reopenWAL(t, path, files{walFile: f.disk[walFile][:f.firstEnd], commitFile: marker}, f.rows[:1])
		})
	}
	for _, tc := range []struct {
		name   string
		marker []byte
		phase  string
	}{
		{"missing-marker", nil, "missing commit marker"},
		{"short-marker", f.disk[commitFile][:23], "invalid commit marker"},
		{"marker-complement", flip(f.disk[commitFile], 16), "invalid commit marker"},
		{"committed-past-eof", markerAt(f.disk[commitFile], ^uint64(0)), "invalid committed offset"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			damaged := files{walFile: f.disk[walFile], walOrphan: []byte("keep until validated")}
			if tc.marker != nil {
				damaged[commitFile] = tc.marker
			}
			path := filepath.Join(t.TempDir(), "owner")
			writeFiles(t, path, damaged)
			assertWALRejected(t, path, damaged, tc.phase)
		})
	}
	t.Run("active-owner", func(t *testing.T) {
		var live, blocked store.Store
		log, err := wal.Open(dir, &live)
		must(t, err)
		defer func() { must(t, log.Close()) }()
		other, err := wal.Open(dir, &blocked)
		if other != nil {
			must(t, other.Close())
		}
		if other != nil || !errors.Is(err, syscall.EWOULDBLOCK) {
			t.Fatalf("lock data directory: got log=%v error=%v, want EWOULDBLOCK", other != nil, err)
		}
		assertStoreRows(t, &blocked, nil)
		assertStoreRows(t, &live, f.rows)
		assertFiles(t, dir, f.disk)
	})
}

func assertWALRejected(t *testing.T, path string, disk files, phase string) {
	t.Helper()
	var s store.Store
	existing := []store.Row{{TS: start, Line: "preexisting query state"}}
	s.Publish(existing)
	log, err := wal.Open(path, &s)
	if log != nil {
		must(t, log.Close())
	}
	if log != nil || err == nil || phase != "" && !strings.Contains(err.Error(), phase) {
		t.Fatalf("recover committed WAL: got log=%v error=%v, want rejection in phase %q", log != nil, err, phase)
	}
	t.Logf("rejected committed WAL: %v", err)
	assertStoreRows(t, &s, existing)
	assertFiles(t, path, disk)
}

func assertStoreRows(t *testing.T, s *store.Store, rows []store.Row) {
	t.Helper()
	want := slices.Clone(rows)
	slices.SortStableFunc(want, func(a, b store.Row) int { return a.TS.Compare(b.TS) })
	got, count, next, ok := s.QueryPage(time.Time{}, recoveryEnd, "", len(want)+1, nil, nil)
	if !ok || count != len(want) || next != nil || !slices.Equal(got, want) {
		t.Fatalf("query rows %v count %d next %v ok %v, want %v", got, count, next, ok, want)
	}
}

func assertWALPoint(t *testing.T, log *wal.Log, want []byte, ordinal uint64) {
	t.Helper()
	c, err := log.Capture(t.Context())
	must(t, err)
	defer func() { must(t, c.Release()) }()
	if c.CommittedEnd() != int64(len(want)) || c.NextOrdinal() != ordinal {
		t.Fatalf("WAL capture end=%d ordinal=%d, want end=%d ordinal=%d", c.CommittedEnd(), c.NextOrdinal(), len(want), ordinal)
	}
	got := make([]byte, len(want)+1)
	n, err := c.ReadAt(got, 0)
	if !errors.Is(err, io.EOF) || n != len(want) || !bytes.Equal(got[:n], want) {
		t.Fatalf("read captured prefix with boundary probe: got %d bytes, error %v", n, err)
	}
}

func checkUncommitted(t *testing.T, f recoveryFixture, suffix []byte) {
	t.Helper()
	disk := maps.Clone(f.disk)
	disk[walFile] = append(slices.Clone(disk[walFile]), suffix...)
	disk[walOrphan] = []byte("abandoned marker")
	path := filepath.Join(t.TempDir(), "owner")
	writeFiles(t, path, disk)
	reopenWAL(t, path, f.disk, f.rows)
}

func reopenWAL(t *testing.T, path string, disk files, rows []store.Row) {
	t.Helper()
	for range 2 {
		func() {
			var s store.Store
			log, err := wal.Open(path, &s)
			must(t, err)
			defer func() { must(t, log.Close()) }()
			assertStoreRows(t, &s, rows)
			assertWALPoint(t, log, disk[walFile], uint64(len(rows)))
		}()
		assertFiles(t, path, disk)
	}
}

func FuzzWALRecovery(f *testing.F) {
	base := newRecoveryFixture(f, f.TempDir())
	f.Add(base.disk[walFile][base.firstEnd:])
	f.Add([]byte{})
	for _, tc := range recoverySuffixes(base) {
		f.Add(tc.data)
	}
	f.Fuzz(func(t *testing.T, suffix []byte) {
		if len(suffix) > maxRecoverySuffix {
			t.Skip()
		}
		checkUncommitted(t, base, suffix)
		rows, valid := replaySuffix(t, base, suffix)
		input := base.withSuffix(suffix)
		disk := files{walFile: input, commitFile: markerAt(base.disk[commitFile], uint64(len(input)))}
		dir := filepath.Join(t.TempDir(), "owner")
		writeFiles(t, dir, disk)
		if !valid {
			assertWALRejected(t, dir, disk, "")
			return
		}
		var s store.Store
		log, err := wal.Open(dir, &s)
		must(t, err)
		defer func() { must(t, log.Close()) }()
		want := append(slices.Clone(base.rows[:1]), rows...)
		assertStoreRows(t, &s, want)
		assertWALPoint(t, log, input, uint64(len(want)))
		assertFiles(t, dir, disk)
	})
}

func replaySuffix(t *testing.T, f recoveryFixture, suffix []byte) ([]store.Row, bool) {
	t.Helper()
	input := f.withSuffix(suffix)
	b := checkpoint.NewBudget(checkpoint.Limits{checkpoint.ReplayTailBytes: maxRecoverySuffix})
	tail, err := b.ReplayCapturedTail(t.Context(), bytes.NewReader(input), int64(f.firstEnd), int64(len(input)), 1)
	if errors.Is(err, checkpoint.ErrIntegrity) {
		if tail != nil || b.Usage().InUse[checkpoint.ReplayTailBytes] != 0 {
			t.Fatal("invalid suffix retained replay state")
		}
		return nil, false
	}
	must(t, err)
	var rows []store.Row
	for _, batch := range tail.Batches {
		rows = append(rows, batch.Rows...)
		batch.Reservation.Release()
	}
	if tail.NextOrdinal != 1+uint64(len(rows)) || b.Usage().InUse[checkpoint.ReplayTailBytes] != 0 {
		t.Fatal("inconsistent replay ordinal or reservation")
	}
	return rows, true
}
