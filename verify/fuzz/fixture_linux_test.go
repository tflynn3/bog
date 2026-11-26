// Package fuzz verifies the public backup and retained-history storage boundaries.
package fuzz

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
)

const (
	metadataFile = "bog.backup"
	manifestFile = "bog.checkpoint"
	tailFile     = "bog.wal.tail"
	walMagic     = "BOGWAL01"

	frameHeader = 16
	blockHeader = 44
	rowHeader   = 24

	// Include one byte past the documented manifest limit in the corpus.
	maxArtifactSize = (256 << 10) + 1
	maxFixtureFiles = 8
)

var (
	le       = binary.LittleEndian
	crcTable = crc32.MakeTable(crc32.Castagnoli)
	start    = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end      = start.Add(time.Hour)
)

type files map[string][]byte

type fixture struct {
	image         files
	segment       string
	point         backup.Point
	covered, rows []backup.Row
}

type capture struct {
	point backup.Point
	files files
	wal   []byte
}

func (c capture) Point() backup.Point { return c.point }

func (c capture) OpenManifest() (io.ReadCloser, error) {
	return c.OpenSegment(manifestFile)
}

func (c capture) OpenSegment(name string) (io.ReadCloser, error) {
	data, ok := c.files[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (c capture) OpenWAL() (backup.WALReader, error) {
	return memoryWAL{bytes.NewReader(c.wal)}, nil
}

type memoryWAL struct{ *bytes.Reader }

func (memoryWAL) Close() error { return nil }

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func frame(t testing.TB, rows []backup.Row) []byte {
	t.Helper()
	var payload bytes.Buffer
	encoder := json.NewEncoder(&payload)
	for _, row := range rows {
		must(t, encoder.Encode(row.Row))
	}
	b := make([]byte, frameHeader)
	n := uint32(payload.Len())
	le.PutUint32(b[0:4], n)
	le.PutUint32(b[4:8], ^n)
	le.PutUint32(b[8:12], crc32.Checksum(payload.Bytes(), crcTable))
	le.PutUint32(b[12:16], crc32.Checksum(b[:12], crcTable))
	return append(b, payload.Bytes()...)
}

func newFixture(t testing.TB) fixture {
	t.Helper()
	rows := []backup.Row{
		{Row: store.Row{TS: start.Add(time.Second), Line: "first checkpoint run, later timestamp"}, Ordinal: 0},
		{Row: store.Row{TS: start, Line: "duplicate checkpoint row"}, Ordinal: 1},
		{Row: store.Row{TS: start, Line: "duplicate checkpoint row"}, Ordinal: 2},
		{Row: store.Row{TS: start, Line: "second checkpoint run, tied timestamp"}, Ordinal: 3},
		{Row: store.Row{TS: start, Line: "tail row, tied timestamp"}, Ordinal: 4},
		{Row: store.Row{TS: start.Add(time.Nanosecond), Line: "tail backfill"}, Ordinal: 5},
		{Row: store.Row{TS: start.Add(2 * time.Second), Line: "last tail frame"}, Ordinal: 6},
	}
	wal := append([]byte(walMagic), frame(t, rows[:3])...)
	firstEnd := len(wal)
	wal = append(wal, frame(t, rows[3:4])...)
	coveredEnd := len(wal)
	wal = append(wal, frame(t, rows[4:6])...)
	wal = append(wal, frame(t, rows[6:])...)

	directory := t.TempDir()
	must(t, os.Chmod(directory, 0700))
	published := publishCheckpoint(t, directory, wal, firstEnd, coveredEnd)
	var catalog history.Catalog
	generation, err := catalog.Replace(t.Context(), directory)
	t.Cleanup(func() { must(t, catalog.Close()) })
	must(t, err)
	covered := ordered(rows[:4])
	assertHistoryRows(t, catalog.AcquireCurrent, generation, covered)

	point := backup.Point{
		Generation:    [sha256.Size]byte(generation.ID),
		CoveredOffset: int64(coveredEnd), NextOrdinal: 4, CommittedEnd: int64(len(wal)),
	}
	if generation.Boundary != (history.Boundary{
		SourceSHA256: sha256.Sum256(wal[:coveredEnd]), CoveredOffset: point.CoveredOffset, NextOrdinal: point.NextOrdinal,
	}) {
		t.Fatalf("published generation has unexpected WAL boundary %+v", generation.Boundary)
	}
	source := files{manifestFile: readArtifact(t, filepath.Join(directory, manifestFile))}
	for _, segment := range published.Segments {
		source[segment.Name] = readArtifact(t, filepath.Join(directory, segment.Name))
	}
	bundle := filepath.Join(t.TempDir(), "export")
	must(t, backup.Export(t.Context(), capture{point, source, wal}, bundle))
	got, err := backup.Verify(t.Context(), bundle)
	must(t, err)
	if got != point {
		t.Fatalf("verified point %+v, want %+v", got, point)
	}
	image := readFiles(t, bundle)
	if !bytes.Equal(image[tailFile], wal[coveredEnd:]) {
		t.Fatal("exported tail differs from the captured frames")
	}

	restored := filepath.Join(t.TempDir(), "restored")
	must(t, backup.Restore(t.Context(), bundle, restored))
	want := ordered(rows)
	assertRestored(t, restored, point, want)
	return fixture{image, published.Segments[0].Name, point, covered, want}
}

func publishCheckpoint(t testing.TB, path string, wal []byte, firstEnd, coveredEnd int) *checkpoint.Manifest {
	t.Helper()
	root, err := os.OpenRoot(path)
	must(t, err)
	defer func() { must(t, root.Close()) }()
	dir, err := root.Open(".")
	must(t, err)
	defer func() { must(t, dir.Close()) }()
	previous, err := checkpoint.PublishCaptured(t.Context(), root, dir, bytes.NewReader(wal), int64(firstEnd), nil, 3, digest(wal[:firstEnd]))
	must(t, err)
	m, err := checkpoint.PublishCaptured(t.Context(), root, dir, bytes.NewReader(wal), int64(coveredEnd), previous, 4, digest(wal[:coveredEnd]))
	must(t, err)
	if len(m.Segments) != 2 {
		t.Fatalf("fixture has %d checkpoint segments, want 2", len(m.Segments))
	}
	return m
}

func ordered(rows []backup.Row) []backup.Row {
	rows = slices.Clone(rows)
	slices.SortStableFunc(rows, func(a, b backup.Row) int { return a.TS.Compare(b.TS) })
	return rows
}

func assertRestored(t testing.TB, path string, point backup.Point, want []backup.Row) {
	t.Helper()
	r, err := backup.OpenRestored(t.Context(), path)
	must(t, err)
	defer func() { must(t, r.Close()) }()
	if r.Point() != point {
		t.Fatalf("restored point %+v, want %+v", r.Point(), point)
	}
	if want == nil {
		return
	}
	q := backup.Query{Start: start, End: end}
	var got []backup.Row
	for range len(want) + 1 {
		page, next, err := r.QueryPage(t.Context(), q, 2)
		must(t, err)
		got = append(got, page...)
		if next == nil {
			if !slices.Equal(got, want) {
				t.Fatalf("restored walk %+v, want %+v", got, want)
			}
			return
		}
		if len(page) == 0 || *next != page[len(page)-1].Position() || len(got) >= len(want) {
			t.Fatalf("unexpected restored continuation %+v after %d rows", next, len(got))
		}
		q.After = next
	}
	t.Fatal("restored walk did not terminate")
}

func assertHistoryRows(t testing.TB, acquire func() (*history.Reader, error), generation history.Generation, want []backup.Row) {
	t.Helper()
	reader, err := acquire()
	must(t, err)
	defer func() { must(t, reader.Close()) }()
	checkHistoryRows(t, reader, generation, want)
}

func checkHistoryRows(t testing.TB, reader *history.Reader, generation history.Generation, want []backup.Row) {
	t.Helper()
	if got := reader.Generation(); got != generation {
		t.Fatalf("reader generation %+v, want %+v", got, generation)
	}
	var got []backup.Row
	err := reader.Scan(t.Context(), history.Query{Start: start, End: end}, func(row checkpoint.BlockRow) error {
		if len(got) == len(want) {
			return errors.New("history scan exceeded fixture row count")
		}
		got = append(got, backup.Row{Row: row.Row, Ordinal: row.Ordinal})
		return nil
	})
	must(t, err)
	if !slices.Equal(got, want) {
		t.Fatalf("history rows %+v, want %+v", got, want)
	}
}

func readArtifact(t testing.TB, path string) []byte {
	t.Helper()
	f, err := os.Open(path)
	must(t, err)
	b, err := io.ReadAll(io.LimitReader(f, maxArtifactSize+1))
	must(t, errors.Join(err, f.Close()))
	if len(b) > maxArtifactSize {
		t.Fatalf("artifact %s exceeds harness limit", path)
	}
	return b
}

func readFiles(t testing.TB, dir string) files {
	t.Helper()
	d, err := os.Open(dir)
	must(t, err)
	entries, err := d.ReadDir(maxFixtureFiles + 1)
	if err == io.EOF {
		err = nil
	}
	must(t, errors.Join(err, d.Close()))
	if len(entries) > maxFixtureFiles {
		t.Fatalf("%s exceeds fixture file count", dir)
	}
	result := make(files, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		must(t, err)
		if !info.Mode().IsRegular() {
			t.Fatalf("unexpected non-regular fixture file %s", entry.Name())
		}
		result[entry.Name()] = readArtifact(t, filepath.Join(dir, entry.Name()))
	}
	return result
}

func writeFiles(t testing.TB, dir string, content files) {
	t.Helper()
	if len(content) > maxFixtureFiles {
		t.Fatal("too many fixture files")
	}
	must(t, os.Mkdir(dir, 0700))
	for name, b := range content {
		if len(b) > maxArtifactSize || filepath.Base(name) != name || !filepath.IsLocal(name) {
			t.Fatalf("invalid fixture file %q with size %d", name, len(b))
		}
		must(t, os.WriteFile(filepath.Join(dir, name), b, 0600))
	}
}

func assertFiles(t testing.TB, dir string, want files) {
	t.Helper()
	if got := readFiles(t, dir); !maps.EqualFunc(got, want, bytes.Equal) {
		t.Fatalf("file contents changed in %s (got %v, want %v)", dir, fileNames(got), fileNames(want))
	}
}

func fileNames(content files) []string {
	return slices.Sorted(maps.Keys(content))
}

func historyFiles(content files) files {
	result := make(files)
	for name, b := range content {
		if name == manifestFile || strings.HasPrefix(name, "bog-segment-") {
			result[name] = b
		}
	}
	return result
}

func assertChildren(t testing.TB, dir string, want ...string) {
	t.Helper()
	d, err := os.Open(dir)
	must(t, err)
	got, err := d.Readdirnames(len(want) + 1)
	if err == io.EOF {
		err = nil
	}
	must(t, errors.Join(err, d.Close()))
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("entries in %s: got %v, want %v", dir, got, want)
	}
}
