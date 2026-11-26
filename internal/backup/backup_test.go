package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

const batch = `{"ts":"2026-01-01T00:00:00.000000001Z","line":"needle second"}`
const tied = `{"ts":"2026-01-01T00:00:00Z","line":"needle duplicate"}`
const low = `{"ts":"0001-01-01T00:00:00Z","line":"needle low"}`
const high = `{"ts":"9999-12-31T23:59:59.999999998Z","line":"needle high"}`

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func write(t *testing.T, dir, name string, data []byte) {
	t.Helper()
	check(t, os.WriteFile(filepath.Join(dir, name), data, 0600))
}

func read(t *testing.T, dir, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	check(t, err)
	return b
}

func add(log *wal.Log, payload string) error {
	rows, err := store.Decode([]byte(payload))
	if err != nil {
		return err
	}
	return log.Append([]byte(payload), rows)
}

func checkpointAt(t *testing.T, dir string) {
	t.Helper()
	l, err := checkpoint.OpenLegacy(dir)
	check(t, err)
	_, err = l.Publish()
	check(t, errors.Join(err, l.Close()))
}

func fixture(t *testing.T, payload string) string {
	t.Helper()
	dir := t.TempDir()
	log, err := wal.Open(dir, &store.Store{})
	check(t, err)
	check(t, errors.Join(add(log, payload), log.Close()))
	return dir
}

func TestConcurrentCheckpointBackup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	initial := batch + "\n" + tied + "\n" + low + "\n" + high
	source := fixture(t, initial)
	checkpointAt(t, source)
	prefix := read(t, source, walName)
	marker := read(t, source, markerName)
	const rounds = 12
	backfill := tied + "\n" + low
	type generation struct {
		manifest []byte
		name     string
		block    []byte
	}
	var generations []generation
	for range rounds {
		dir := t.TempDir()
		write(t, dir, walName, prefix)
		write(t, dir, markerName, marker)
		log, err := wal.Open(dir, &store.Store{})
		check(t, err)
		check(t, errors.Join(add(log, backfill), log.Close()))
		checkpointAt(t, dir)
		b := read(t, dir, manifestName)
		m, err := decodeManifest(b)
		check(t, err)
		name := m.Segments[0].Name
		generations = append(generations, generation{b, name, read(t, dir, name)})
	}

	// The base publisher is offline. Reuse its real manifests and blocks,
	// installing them over the live WAL with the same sync/rename protocol.
	log, err := wal.Open(source, &store.Store{})
	check(t, err)
	defer func() { check(t, log.Close()) }()
	initialManifest, err := decodeManifest(read(t, source, manifestName))
	check(t, err)
	old := initialManifest.Segments[0].Name
	check(t, add(log, backfill))
	payloads := []string{initial, backfill}
	started := make(chan struct{})
	done := make(chan error, 2)
	p := t.TempDir()
	for i := range rounds {
		payloads = append(payloads, tied+"\n"+fmt.Sprintf(`{"ts":"2026-01-01T00:00:00Z","line":"needle %02d"}`, i)+"\n"+tied)
	}
	go func() {
		<-started
		for _, payload := range payloads[2:] {
			if err := add(log, payload); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	go func() {
		<-started
		dir, err := openDirectory(source)
		if err != nil {
			done <- err
			return
		}
		for _, g := range generations {
			err = errors.Join(dir.write(g.name, g.block), dir.fd.Sync())
			if err == nil {
				err = dir.write(".next-checkpoint", g.manifest)
			}
			if err == nil {
				err = dir.root.Rename(".next-checkpoint", manifestName)
			}
			if err == nil {
				err = dir.fd.Sync()
			}
			if err == nil {
				err = dir.root.Remove(old)
			}
			if err != nil {
				break
			}
			old = g.name
		}
		done <- errors.Join(err, dir.Close())
	}()
	close(started)
	workers := 2
	wait := func() {
		for workers > 0 {
			workers--
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-ctx.Done():
				t.Error(ctx.Err())
			}
		}
	}
	defer wait()

	for i := 0; i <= rounds; i++ {
		if i == rounds {
			wait()
		}
		exported := filepath.Join(p, fmt.Sprintf("export-%d", i))
		got, err := Backup(ctx, source, exported)
		if err == errChanged && i < rounds {
			continue // A continuously retiring publisher can require a retry.
		}
		check(t, err)
		assertRestored(t, exported, filepath.Join(p, fmt.Sprintf("restore-%d", i)), payloads, got)
		if i == rounds && (got.Rows != uint64(6+3*rounds) || got.CoveredOffset != int64(len(prefix)+16+len(backfill))) {
			t.Fatalf("final snapshot missed a completed append or replacement: %+v", got)
		}
	}
}

func assertRestored(t *testing.T, exported, restored string, payloads []string, point Snapshot) {
	t.Helper()
	verified, err := Verify(t.Context(), exported)
	check(t, err)
	check(t, os.Mkdir(restored, 0700))
	got, err := Restore(t.Context(), exported, restored)
	check(t, err)
	if got != point || verified != point {
		t.Fatalf("snapshot changed: export %+v, verify %+v, restore %+v", point, verified, got)
	}
	var input []store.Row
	end := int64(len(walMagic))
	for _, payload := range payloads {
		next := end + int64(16+len(payload))
		if next > point.CommittedOffset {
			break
		}
		rows, err := store.Decode([]byte(payload))
		check(t, err)
		input = append(input, rows...)
		end = next
	}
	first := int64(len(walMagic) + 16 + len(payloads[0]))
	second := first + int64(16+len(payloads[1]))
	if end != point.CommittedOffset || uint64(len(input)) != point.Rows ||
		(point.CoveredOffset != first && point.CoveredOffset != second) {
		t.Fatalf("unexpected recovery point %+v", point)
	}
	restoredRows := make([]store.Row, len(input))
	func() {
		l, err := checkpoint.OpenLegacy(restored)
		check(t, err)
		defer func() { check(t, l.Close()) }()
		published, err := l.OpenPublished()
		check(t, err)
		check(t, published.Scan(func(row checkpoint.BlockRow) error {
			restoredRows[row.Ordinal] = row.Row
			return nil
		}))
		check(t, l.ScanUncovered(published, func(first uint64, rows []store.Row) error {
			copy(restoredRows[first:], rows)
			return nil
		}))
	}()
	if !slices.Equal(restoredRows, input) {
		t.Fatalf("restored checkpoint and tail differ from append log")
	}

	s := &store.Store{}
	log, err := wal.Open(restored, s)
	check(t, err)
	defer func() { check(t, log.Close()) }()
	slices.SortStableFunc(input, func(a, b store.Row) int { return a.TS.Compare(b.TS) })
	for _, q := range []struct {
		start, end string
		literal    string
	}{
		{"0001-01-01T00:00:00Z", "9999-12-31T23:59:59.999999999Z", ""},
		{"2026-01-01T00:00:00Z", "2026-01-01T00:00:00.000000001Z", "needle"},
		{"2026-01-01T00:00:00Z", "2026-01-01T00:00:00.000000001Z", "Needle"},
	} {
		start, err := store.ParseTime(q.start)
		check(t, err)
		end, err := store.ParseTime(q.end)
		check(t, err)
		var want, results []store.Row
		for _, row := range input {
			if !row.TS.Before(start) && row.TS.Before(end) && strings.Contains(row.Line, q.literal) {
				want = append(want, row)
			}
		}
		var snapshot *int
		var after *store.Position
		for page := 0; page <= len(input); page++ {
			rows, count, cursor, err := s.QueryPageWithWork(start, end, q.literal, 2, snapshot, after, nil)
			check(t, err)
			if count != len(input) {
				t.Fatalf("snapshot count = %d, want %d", count, len(input))
			}
			results = append(results, rows...)
			if cursor == nil {
				break
			}
			snapshot, after = &count, cursor
		}
		if !slices.Equal(results, want) {
			t.Fatalf("paged query returned %v, want %v", results, want)
		}
	}
}

func TestCorruptBackupAndRetry(t *testing.T) {
	source := fixture(t, batch)
	checkpointAt(t, source)
	m, err := decodeManifest(read(t, source, manifestName))
	check(t, err)
	for _, tc := range []struct {
		name, file string
		damage     func([]byte) []byte
	}{
		{"interrupted", indexName, nil},
		{"incomplete index", indexName, func(b []byte) []byte { return b[:7] }},
		{"missing segment", m.Segments[0].Name, nil},
		{"segment checksum", m.Segments[0].Name, flipLast},
		{"manifest checksum", manifestName, flipLast},
		{"committed corruption", walName, flipLast},
		{"unexpected tail", walName, func(b []byte) []byte { return append(b, 0) }},
		{"short WAL", walName, func(b []byte) []byte { return b[:len(b)-1] }},
		{"boundary inside frame", markerName, func([]byte) []byte { return makeMarker(25) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := t.TempDir()
			exported, restored := filepath.Join(base, "backup"), filepath.Join(base, "restore")
			_, err := Backup(t.Context(), source, exported)
			check(t, err)
			original := read(t, exported, tc.file)
			if tc.damage == nil {
				check(t, os.Remove(filepath.Join(exported, tc.file)))
			} else {
				write(t, exported, tc.file, tc.damage(slices.Clone(original)))
			}
			if _, err := Verify(t.Context(), exported); err == nil {
				t.Fatal("verified damaged backup")
			}
			check(t, os.Mkdir(restored, 0700))
			if _, err := Restore(t.Context(), exported, restored); err == nil {
				t.Fatal("restored damaged backup")
			}
			entries, err := os.ReadDir(base)
			check(t, err)
			if len(entries) != 2 {
				t.Fatalf("failed restore left a staging directory: %v", entries)
			}
			write(t, exported, tc.file, original)
			_, err = Restore(t.Context(), exported, restored)
			check(t, err)
			if _, err := Restore(t.Context(), exported, restored); err == nil {
				t.Fatal("overwrote nonempty destination")
			}
		})
	}
}

func flipLast(b []byte) []byte {
	b[len(b)-1] ^= 1
	return b
}

func TestLegacyAndMaximumFrame(t *testing.T) {
	source := t.TempDir()
	write(t, source, walName, []byte(walMagic))
	p := t.TempDir()
	got, err := Backup(t.Context(), source, filepath.Join(p, "empty"))
	check(t, err)
	if got.Rows != 0 || got.CommittedOffset != 8 || got.CoveredOffset != 8 {
		t.Fatalf("unexpected bootstrap snapshot %+v", got)
	}

	payload := strings.Replace(batch, "needle second", strings.Repeat("a", store.MaxBatch-len(batch)+len("needle second")), 1)
	source = fixture(t, payload)
	checkpointAt(t, source)
	committed := read(t, source, walName)
	write(t, source, walName, append(slices.Clone(committed), []byte("torn WAL suffix")...))
	got, err = Backup(t.Context(), source, filepath.Join(p, "maximum"))
	check(t, err)
	if got.Rows != 1 || got.CommittedOffset != int64(len(committed)) || !slices.Equal(read(t, filepath.Join(p, "maximum"), walName), committed) {
		t.Fatalf("unexpected maximum-frame snapshot %+v", got)
	}
	_, err = Restore(t.Context(), filepath.Join(p, "maximum"), filepath.Join(p, "result"))
	check(t, err)
}

func TestPublicationIOFailure(t *testing.T) {
	for _, failRename := range []bool{true, false} {
		calls := 0
		err := installDirectory(func() error {
			calls++
			if failRename {
				return syscall.EIO
			}
			return nil
		}, func() error {
			calls++
			return syscall.EIO
		})
		if !errors.Is(err, ErrPublicationUncertain) || !errors.Is(err, syscall.EIO) || (calls == 1) != failRename {
			t.Fatalf("uncertain publication error = %v, calls = %d", err, calls)
		}
	}
	dir := t.TempDir()
	destination := filepath.Join(dir, "output")
	err := publish(t.Context(), destination, func(stage *directory) error {
		return stage.output(walName, func(io.Writer) error { return syscall.ENOSPC })
	})
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("staged write error = %v", err)
	}
	entries, err := os.ReadDir(dir)
	check(t, err)
	if len(entries) != 0 {
		t.Fatalf("failed write left %v", entries)
	}
}

func TestManifestGrowthAndMissingLiveSegment(t *testing.T) {
	source := fixture(t, strings.Repeat(tied+"\n", checkpoint.MaxSegments))
	walBytes := read(t, source, walName)
	m := manifestV1{
		Version: 1, SourceSHA256: checksum(walBytes),
		CoveredOffset: int64(len(walBytes)), NextOrdinal: checkpoint.MaxSegments,
	}
	rows, err := store.Decode([]byte(tied))
	check(t, err)
	for i := range checkpoint.MaxSegments {
		b, err := checkpoint.EncodeBlock([]checkpoint.BlockRow{{Row: rows[0], Ordinal: uint64(i)}})
		check(t, err)
		name := fmt.Sprintf("%s%032x", segmentPrefix, i)
		write(t, source, name, b)
		m.Segments = append(m.Segments, segmentV1{name, int64(len(b)), checksum(b)})
	}
	payload, err := json.Marshal(m)
	check(t, err)
	manifest, err := json.Marshal(manifestEnvelope{payload, checksum(payload)})
	check(t, err)
	write(t, source, manifestName, manifest)
	// An abandoned segment is outside the authoritative manifest.
	orphan := fmt.Sprintf("%s%032x", segmentPrefix, checkpoint.MaxSegments)
	write(t, source, orphan, []byte("abandoned"))
	parent := t.TempDir()
	dst := filepath.Join(parent, "backup")
	point, err := Backup(t.Context(), source, dst)
	check(t, err)
	got, err := Verify(t.Context(), dst)
	check(t, err)
	if got != point || got.Rows != checkpoint.MaxSegments {
		t.Fatalf("unexpected recovery point %+v", got)
	}
	if _, err := os.Lstat(filepath.Join(dst, orphan)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan present or unexpected error: %v", err)
	}
	check(t, os.Remove(filepath.Join(source, m.Segments[0].Name)))
	missing := filepath.Join(parent, "missing")
	if _, err := Backup(t.Context(), source, missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing referenced segment: %v", err)
	}
	if _, err := os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("output published after missing input: %v", err)
	}
}
