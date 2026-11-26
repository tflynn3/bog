package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"bog/internal/store"
)

type memoryWAL struct{ *bytes.Reader }

func (memoryWAL) Close() error { return nil }

type fixture struct {
	point    Point
	manifest []byte
	segments map[string][]byte
	order    []string
	wal      []byte
	rows     []store.Row
}

func (f *fixture) Point() Point { return f.point }
func (f *fixture) OpenManifest() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(f.manifest)), nil
}
func (f *fixture) OpenSegment(name string) (io.ReadCloser, error) {
	b, ok := f.segments[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}
func (f *fixture) OpenWAL() (WALReader, error) { return memoryWAL{bytes.NewReader(f.wal)}, nil }

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func digest(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func timeAt(t testing.TB, s string) time.Time {
	t.Helper()
	p, err := time.Parse(time.RFC3339Nano, s)
	must(t, err)
	return p
}

func encodeFrame(payload []byte) []byte {
	frame := make([]byte, 16, 16+len(payload))
	n := uint32(len(payload))
	little.PutUint32(frame[:4], n)
	little.PutUint32(frame[4:8], ^n)
	little.PutUint32(frame[8:12], crc32.Checksum(payload, crcTable))
	little.PutUint32(frame[12:16], crc32.Checksum(frame[:12], crcTable))
	return append(frame, payload...)
}

func frameRows(t testing.TB, rows []store.Row) []byte {
	t.Helper()
	var payload bytes.Buffer
	e := json.NewEncoder(&payload)
	for _, row := range rows {
		must(t, e.Encode(row))
	}
	return encodeFrame(payload.Bytes())
}

// Build a fixture using the documented checkpoint layout independently of
// the backup decoder. The runs deliberately interleave append ordinal ranges.
func fixtureBlock(rows []store.Row, ordinals ...int) []byte {
	putTime := func(b []byte, tm time.Time) {
		little.PutUint64(b, uint64(tm.Unix()))
		little.PutUint32(b[8:], uint32(tm.Nanosecond()))
	}
	b := make([]byte, 44)
	for _, i := range ordinals {
		var h [24]byte
		putTime(h[:], rows[i].TS)
		little.PutUint64(h[12:], uint64(i))
		little.PutUint32(h[20:], uint32(len(rows[i].Line)))
		b = append(b, h[:]...)
		b = append(b, rows[i].Line...)
	}
	little.PutUint32(b[:4], 1)
	little.PutUint32(b[4:8], uint32(len(ordinals)))
	little.PutUint32(b[8:12], uint32(len(b)-44))
	putTime(b[12:24], rows[ordinals[0]].TS)
	putTime(b[24:36], rows[ordinals[len(ordinals)-1]].TS)
	little.PutUint32(b[36:40], crc32.Checksum(b[44:], crcTable))
	little.PutUint32(b[40:44], crc32.Checksum(b[:40], crcTable))
	return b
}

func manifestEnvelope(t testing.TB, m manifest) ([]byte, [32]byte) {
	t.Helper()
	raw, err := json.Marshal(m)
	must(t, err)
	b, err := json.Marshal(struct {
		Payload json.RawMessage `json:"payload"`
		SHA256  string          `json:"sha256"`
	}{raw, digest(raw)})
	must(t, err)
	return b, sha256.Sum256(raw)
}

func newFixture(t testing.TB) *fixture {
	t.Helper()
	a := timeAt(t, "2026-04-01T00:00:00Z")
	b := a.Add(time.Nanosecond)
	rows := []store.Row{
		{TS: b, Line: "Case"},
		{TS: a, Line: "duplicate [x]"},
		{TS: a, Line: "duplicate [x]"},
		{TS: timeAt(t, "0001-01-01T00:00:00Z"), Line: "first [x]"},
		{TS: b, Line: strings.Repeat("long", 1300)},
		{TS: timeAt(t, "9999-12-31T23:59:59.999999998Z"), Line: "last [x]"},
		{TS: a, Line: "duplicate [x]"},
		{TS: a.Add(-time.Nanosecond), Line: "backfill [x]"},
		{TS: b, Line: "case"},
	}
	wal := append([]byte("BOGWAL01"), frameRows(t, rows[:3])...)
	wal = append(wal, frameRows(t, rows[3:6])...)
	m := manifest{Version: 1, SourceSHA256: digest(wal), CoveredOffset: int64(len(wal)), NextOrdinal: 6}
	f := &fixture{segments: map[string][]byte{}, rows: rows}
	for j, ordinals := range [][]int{{3, 1, 4}, {2, 0, 5}} {
		name := fmt.Sprintf("bog-segment-%032x", j+1)
		block := fixtureBlock(rows, ordinals...)
		f.segments[name] = block
		f.order = append(f.order, name)
		m.Segments = append(m.Segments, segment{name, int64(len(block)), digest(block)})
	}
	var id [32]byte
	f.manifest, id = manifestEnvelope(t, m)
	wal = append(wal, frameRows(t, rows[6:8])...)
	wal = append(wal, frameRows(t, rows[8:])...)
	f.point = Point{id, m.CoveredOffset, m.NextOrdinal, int64(len(wal))}
	f.wal = append(wal, frameRows(t, []store.Row{{TS: a, Line: "uncaptured"}})...)
	return f
}

func oracle(rows []store.Row, q Query) []Row {
	var result []Row
	for ordinal, row := range rows {
		if !row.TS.Before(q.Start) && row.TS.Before(q.End) && strings.Contains(row.Line, q.Contains) {
			result = append(result, Row{row, uint64(ordinal)})
		}
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].TS.Before(result[j].TS) })
	return result
}

func checkWalk(t *testing.T, r *Reader, rows []store.Row) {
	t.Helper()
	tie := rows[1].TS
	for _, q := range []Query{
		{Start: minTime, End: maxTime},
		{Start: minTime, End: maxTime, Contains: "[x]"},
		{Start: minTime, End: maxTime, Contains: "Case"},
		{Start: minTime, End: maxTime, Contains: "missing"},
		{Start: tie, End: tie.Add(time.Nanosecond)},
		{Start: tie, End: tie},
	} {
		want := oracle(rows, q)
		for _, limit := range []int{1, 2, 3, MaxPageSize} {
			var got []Row
			query := q
			for pages := 0; ; pages++ {
				if pages > len(rows) {
					t.Fatal("continuation did not advance")
				}
				page, next, err := r.QueryPage(context.Background(), query, limit)
				must(t, err)
				got = append(got, page...)
				if next == nil {
					break
				}
				if len(page) != limit || *next != page[len(page)-1].Position() {
					t.Fatalf("invalid continuation: %v", next)
				}
				query.After = next
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("query %+v, limit %d: got ordinals %v, want %v", q, limit, ordinalList(got), ordinalList(want))
			}
		}
	}
}

func ordinalList(rows []Row) []uint64 {
	values := make([]uint64, len(rows))
	for i, row := range rows {
		values[i] = row.Ordinal
	}
	return values
}

func TestExportRestoreAndOrderedQueryWalk(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	root := t.TempDir()
	bundle, target := filepath.Join(root, "bundle"), filepath.Join(root, "restored")
	must(t, Export(ctx, f, bundle))
	p, err := Verify(ctx, bundle)
	must(t, err)
	if p != f.point {
		t.Fatalf("got recovery point %+v, want %+v", p, f.point)
	}
	tail, err := os.ReadFile(filepath.Join(bundle, tailName))
	must(t, err)
	if !bytes.Equal(tail, f.wal[p.CoveredOffset:p.CommittedEnd]) {
		t.Fatal("bundle does not contain the exact captured tail")
	}
	must(t, os.Mkdir(target, 0700))
	must(t, Restore(ctx, bundle, target))
	must(t, os.RemoveAll(bundle))

	r, err := OpenRestored(ctx, target)
	must(t, err)
	t.Cleanup(func() { must(t, r.Close()) })
	if r.Point() != p {
		t.Fatal("recovery point changed after restore")
	}
	var firsts []uint64
	var replayed []store.Row
	must(t, r.ReplayTail(ctx, func(first uint64, rows []store.Row) error {
		firsts = append(firsts, first)
		replayed = append(replayed, rows...)
		return nil
	}))
	if !reflect.DeepEqual(firsts, []uint64{6, 8}) || !reflect.DeepEqual(replayed, f.rows[6:]) {
		t.Fatalf("incorrect WAL replay ordinals: %v", firsts)
	}
	checkWalk(t, r, f.rows)
}

func rewrite(t *testing.T, path string, edit func([]byte) []byte) {
	t.Helper()
	b, err := os.ReadFile(path)
	must(t, err)
	must(t, os.WriteFile(path, edit(b), 0600))
}

func rehashTail(t *testing.T, bundle string, tail []byte) {
	t.Helper()
	must(t, os.WriteFile(filepath.Join(bundle, tailName), tail, 0600))
	rewrite(t, filepath.Join(bundle, metadataName), func(b []byte) []byte {
		m, err := decodeMetadata(b)
		must(t, err)
		m.CommittedEnd = m.CoveredOffset + int64(len(tail))
		m.tailSum = sha256.Sum256(tail)
		return m.encode()
	})
}

func TestBadBundlesAndRestoreRetry(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	tests := []struct {
		name string
		edit func(*testing.T, string)
	}{
		{"missing metadata", func(t *testing.T, b string) { must(t, os.Remove(filepath.Join(b, metadataName))) }},
		{"missing segment with an unrelated replacement", func(t *testing.T, b string) {
			must(t, os.Rename(filepath.Join(b, f.order[0]), filepath.Join(b, "orphan")))
		}},
		{"symlink segment", func(t *testing.T, b string) {
			p := filepath.Join(b, f.order[0])
			must(t, os.Remove(p))
			must(t, os.Symlink(f.order[1], p))
		}},
		{"extra file", func(t *testing.T, b string) { must(t, os.WriteFile(filepath.Join(b, "orphan"), nil, 0600)) }},
		{"metadata checksum", func(t *testing.T, b string) {
			rewrite(t, filepath.Join(b, metadataName), func(p []byte) []byte { p[56] ^= 1; return p })
		}},
		{"manifest identity", func(t *testing.T, b string) {
			rewrite(t, filepath.Join(b, metadataName), func(p []byte) []byte {
				m, err := decodeMetadata(p)
				must(t, err)
				m.Generation[0] ^= 1
				return m.encode()
			})
		}},
		{"wrong ordinal", func(t *testing.T, b string) {
			rewrite(t, filepath.Join(b, metadataName), func(p []byte) []byte {
				m, err := decodeMetadata(p)
				must(t, err)
				m.NextOrdinal++
				return m.encode()
			})
		}},
		{"segment checksum", func(t *testing.T, b string) {
			rewrite(t, filepath.Join(b, f.order[0]), func(p []byte) []byte { p[len(p)-1] ^= 1; return p })
		}},
		{"segment duplicate ordinal with valid checksums", func(t *testing.T, b string) {
			p := filepath.Join(b, f.order[1])
			block, err := os.ReadFile(p)
			must(t, err)
			little.PutUint64(block[56:64], 1)
			little.PutUint32(block[36:40], crc32.Checksum(block[44:], crcTable))
			little.PutUint32(block[40:44], crc32.Checksum(block[:40], crcTable))
			must(t, os.WriteFile(p, block, 0600))
			m, err := readManifest(f.manifest, f.point)
			must(t, err)
			m.Segments[1].SHA256 = digest(block)
			manifest, id := manifestEnvelope(t, m)
			must(t, os.WriteFile(filepath.Join(b, manifestName), manifest, 0600))
			rewrite(t, filepath.Join(b, metadataName), func(p []byte) []byte {
				meta, err := decodeMetadata(p)
				must(t, err)
				meta.Generation = id
				return meta.encode()
			})
		}},
		{"tail header CRC", func(t *testing.T, b string) {
			p := bytes.Clone(f.wal[f.point.CoveredOffset:f.point.CommittedEnd])
			p[12] ^= 1
			rehashTail(t, b, p)
		}},
		{"tail payload CRC", func(t *testing.T, b string) {
			p := bytes.Clone(f.wal[f.point.CoveredOffset:f.point.CommittedEnd])
			p[16] ^= 1
			rehashTail(t, b, p)
		}},
		{"tail digest", func(t *testing.T, b string) {
			p := append(frameRows(t, []store.Row{{TS: f.rows[6].TS, Line: "Duplicate [x]"}, f.rows[7]}), frameRows(t, f.rows[8:])...)
			must(t, os.WriteFile(filepath.Join(b, tailName), p, 0600))
		}},
		{"incomplete frame with valid outer checksum", func(t *testing.T, b string) {
			rehashTail(t, b, f.wal[f.point.CoveredOffset:f.point.CommittedEnd-1])
		}},
		{"invalid batch in valid frame", func(t *testing.T, b string) { rehashTail(t, b, encodeFrame([]byte("invalid"))) }},
		{"extra tail bytes", func(t *testing.T, b string) {
			must(t, os.WriteFile(filepath.Join(b, tailName), f.wal[f.point.CoveredOffset:], 0600))
		}},
	}
	good := filepath.Join(t.TempDir(), "good")
	must(t, Export(ctx, f, good))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			bad, target := filepath.Join(root, "bad"), filepath.Join(root, "restore")
			must(t, Restore(ctx, good, bad))
			must(t, os.Mkdir(target, 0700))
			tt.edit(t, bad)
			if _, err := Verify(ctx, bad); !errors.Is(err, ErrIntegrity) {
				t.Fatalf("verification error = %v", err)
			}
			if err := Restore(ctx, bad, target); !errors.Is(err, ErrIntegrity) {
				t.Fatalf("restore error = %v", err)
			}
			entries, err := os.ReadDir(target)
			must(t, err)
			if len(entries) != 0 {
				t.Fatal("failed restore modified its destination")
			}
			backupChildren(t, root, stagingGuard, "bad", "restore")
			must(t, Restore(ctx, good, target))
			p, err := Verify(ctx, target)
			must(t, err)
			if p != f.point {
				t.Fatal("retry installed the wrong generation")
			}
		})
	}
}

type wiredSnapshot struct {
	*fixture
	openSegment func(string) (io.ReadCloser, error)
	openWAL     func() (WALReader, error)
}

func (s wiredSnapshot) OpenSegment(name string) (io.ReadCloser, error) { return s.openSegment(name) }
func (s wiredSnapshot) OpenWAL() (WALReader, error)                    { return s.openWAL() }

type boundedWAL struct {
	io.ReaderAt
	end int64
}

func (w boundedWAL) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off > w.end || int64(len(p)) > w.end-off {
		return 0, errors.New("read beyond pinned committed WAL")
	}
	return w.ReaderAt.ReadAt(p, off)
}

func (boundedWAL) Close() error { return nil }

func TestConcurrentExportsKeepCapturedGenerationAndEnd(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	root := t.TempDir()
	active := filepath.Join(root, "active")
	must(t, os.Mkdir(active, 0700))
	must(t, os.WriteFile(filepath.Join(active, manifestName), f.manifest, 0600))
	walPath := filepath.Join(active, "bog.wal")
	must(t, os.WriteFile(walPath, f.wal[:f.point.CommittedEnd], 0600))
	wal, err := os.Open(walPath)
	must(t, err)
	t.Cleanup(func() { must(t, wal.Close()) })
	pins := make(map[string]*os.File)
	for name, data := range f.segments {
		path := filepath.Join(active, name)
		must(t, os.WriteFile(path, data, 0600))
		file, err := os.Open(path)
		must(t, err)
		t.Cleanup(func() { must(t, file.Close()) })
		pins[name] = file
	}

	const workers = 4
	reached := make(chan struct{}, workers)
	proceed := make(chan struct{})
	var release sync.Once
	s := wiredSnapshot{
		fixture: f,
		openSegment: func(name string) (io.ReadCloser, error) {
			if name == f.order[0] {
				reached <- struct{}{}
				<-proceed
			}
			return io.NopCloser(io.NewSectionReader(pins[name], 0, int64(len(f.segments[name])))), nil
		},
		openWAL: func() (WALReader, error) { return boundedWAL{wal, f.point.CommittedEnd}, nil },
	}
	paths := make([]string, workers)
	results := make(chan error, workers)
	var group sync.WaitGroup
	defer group.Wait()
	defer release.Do(func() { close(proceed) })
	for i := range paths {
		paths[i] = filepath.Join(root, fmt.Sprintf("bundle-%d", i))
		group.Go(func() { results <- Export(ctx, s, paths[i]) })
	}
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	for range workers {
		select {
		case <-reached:
		case err := <-results:
			t.Fatalf("export returned before reaching its pinned segment: %v", err)
		case <-timeout.C:
			t.Fatal("exports did not reach the pinned generation")
		}
	}
	writer, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0)
	must(t, err)
	_, err = io.Copy(writer, bytes.NewReader(f.wal[f.point.CommittedEnd:]))
	must(t, errors.Join(err, writer.Sync(), writer.Close()))
	replacement := []byte("replacement generation")
	must(t, os.WriteFile(filepath.Join(active, "successor"), replacement, 0600))
	must(t, os.Rename(filepath.Join(active, "successor"), filepath.Join(active, manifestName)))
	for _, name := range f.order {
		must(t, os.Remove(filepath.Join(active, name)))
	}
	release.Do(func() { close(proceed) })
	group.Wait()
	for range workers {
		must(t, <-results)
	}
	for _, path := range paths {
		r, err := OpenRestored(ctx, path)
		must(t, err)
		t.Cleanup(func() { must(t, r.Close()) })
		if r.Point() != f.point {
			t.Fatal("export changed generation")
		}
		checkWalk(t, r, f.rows)
	}
}

type closeErrorReader struct {
	io.Reader
	err error
}

func (r closeErrorReader) Close() error { return r.err }

func TestFailedExportLeavesDestinationRetryable(t *testing.T) {
	f := newFixture(t)
	ioFailure := errors.New("injected close failure")
	tests := []struct {
		name     string
		point    func(*Point)
		wal      func([]byte) []byte
		closeErr error
		want     error
	}{
		{name: "segment close", closeErr: ioFailure, want: ioFailure},
		{name: "wrong WAL identity", wal: func(b []byte) []byte {
			batch := append([]store.Row(nil), f.rows[:3]...)
			batch[2].Line = "Duplicate [x]"
			copy(b[len(walMagic):], frameRows(t, batch))
			return b
		}, want: ErrIntegrity},
		{name: "end inside frame", point: func(p *Point) { p.CommittedEnd-- }, want: ErrIntegrity},
		{name: "short captured WAL", wal: func(b []byte) []byte { return b[:f.point.CommittedEnd-1] }, want: io.ErrUnexpectedEOF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := *f
			if tt.point != nil {
				tt.point(&input.point)
			}
			if tt.wal != nil {
				input.wal = tt.wal(bytes.Clone(input.wal))
			}
			s := wiredSnapshot{fixture: &input, openSegment: input.OpenSegment, openWAL: input.OpenWAL}
			if tt.closeErr != nil {
				s.openSegment = func(name string) (io.ReadCloser, error) {
					return closeErrorReader{bytes.NewReader(f.segments[name]), tt.closeErr}, nil
				}
			}
			root := t.TempDir()
			bundle := filepath.Join(root, "bundle")
			if err := Export(context.Background(), s, bundle); !errors.Is(err, tt.want) {
				t.Fatalf("export error = %v, want %v", err, tt.want)
			}
			backupChildren(t, root, stagingGuard)
			must(t, Export(context.Background(), f, bundle))
		})
	}
}

func TestExportCancellationAndDestinationCollision(t *testing.T) {
	f := newFixture(t)
	root := t.TempDir()
	dest := filepath.Join(root, "bundle")
	must(t, os.Mkdir(dest, 0700))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := wiredSnapshot{fixture: f, openWAL: f.OpenWAL, openSegment: func(name string) (io.ReadCloser, error) {
		cancel()
		return f.OpenSegment(name)
	}}
	if err := Export(ctx, s, dest); !errors.Is(err, context.Canceled) {
		t.Fatalf("export error = %v", err)
	}
	entries, err := os.ReadDir(dest)
	must(t, err)
	if len(entries) != 0 {
		t.Fatal("canceled export modified destination")
	}
	must(t, Export(context.Background(), f, dest))
	if err := Export(context.Background(), f, dest); err == nil {
		t.Fatal("overwrote existing bundle")
	}
	if err := Restore(context.Background(), dest, dest); err == nil {
		t.Fatal("overwrote existing restore")
	}
	if p, err := Verify(context.Background(), dest); err != nil || p != f.point {
		t.Fatalf("existing bundle changed: %v, %v", p, err)
	}
}

func TestEmptyCheckpointAndLargestFrame(t *testing.T) {
	for _, populated := range []bool{false, true} {
		t.Run(fmt.Sprint(populated), func(t *testing.T) {
			ctx := context.Background()
			wal := []byte("BOGWAL01")
			b, id := manifestEnvelope(t, manifest{Version: 1, SourceSHA256: digest(wal), CoveredOffset: 8})
			f := &fixture{manifest: b, wal: wal, point: Point{id, 8, 0, 8}}
			if populated {
				const head = "{\"ts\":\"2026-01-01T00:00:00Z\",\"line\":\""
				const foot = "\"}\n"
				payload := []byte(head + strings.Repeat("x", store.MaxBatch-len(head)-len(foot)) + foot)
				f.wal = append(f.wal, encodeFrame(payload)...)
				f.point.CommittedEnd = int64(len(f.wal))
			}
			dir := filepath.Join(t.TempDir(), "backup")
			must(t, Export(ctx, f, dir))
			r, err := OpenRestored(ctx, dir)
			must(t, err)
			t.Cleanup(func() { must(t, r.Close()) })
			rows, next, err := r.QueryPage(ctx, Query{Start: minTime, End: maxTime}, 1)
			must(t, err)
			if next != nil || (populated && (len(rows) != 1 || rows[0].Ordinal != 0 || len(rows[0].Line) < store.MaxBatch-100)) || (!populated && len(rows) != 0) {
				t.Fatalf("wrong result: %v rows, next %v", len(rows), next)
			}
		})
	}
}

func TestLargeTailCandidateMemory(t *testing.T) {
	ctx := context.Background()
	fixture := newFixture(t)
	dir := t.TempDir()
	must(t, os.WriteFile(filepath.Join(dir, manifestName), fixture.manifest, 0600))
	for name, block := range fixture.segments {
		must(t, os.WriteFile(filepath.Join(dir, name), block, 0600))
	}

	tailTime := fixture.rows[0].TS.Add(time.Second)
	tail, err := os.Create(filepath.Join(dir, tailName))
	must(t, err)
	hash := sha256.New()
	w := io.MultiWriter(tail, hash)
	point := fixture.point
	point.CommittedEnd = point.CoveredOffset
	const frameCount = 16
	{
		line := "[x]" + strings.Repeat("x", store.MaxBatch-100)
		frame := frameRows(t, []store.Row{{TS: tailTime, Line: line}})
		for range frameCount {
			_, err := w.Write(frame)
			must(t, err)
			point.CommittedEnd += int64(len(frame))
		}
	}
	must(t, tail.Close())
	m := metadata{Point: point, tailSum: [32]byte(hash.Sum(nil))}
	must(t, os.WriteFile(filepath.Join(dir, metadataName), m.encode(), 0600))
	r, err := OpenRestored(ctx, dir)
	must(t, err)
	t.Cleanup(func() { must(t, r.Close()) })

	q := Query{Start: minTime, End: tailTime.Add(time.Second), Contains: "[x]"}
	f, err := openTail(r.root, m)
	must(t, err)
	t.Cleanup(func() { must(t, f.Close()) })
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	candidates, err := r.tailPage(ctx, f, q, MaxPageSize+1)
	must(t, err)
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(candidates)
	if len(candidates) != frameCount {
		t.Fatalf("got %d matching tail candidates, want %d", len(candidates), frameCount)
	}
	// Keeping these decoded bodies would retain nearly 64 MiB.
	if after.HeapAlloc > before.HeapAlloc+(16<<20) {
		t.Fatalf("tail candidates retained %d additional heap bytes", after.HeapAlloc-before.HeapAlloc)
	}

	page, next, err := r.QueryPage(ctx, q, 3)
	must(t, err)
	if !reflect.DeepEqual(ordinalList(page), []uint64{3, 1, 2}) || next == nil || *next != page[2].Position() {
		t.Fatalf("got checkpoint page %v with continuation %v", ordinalList(page), next)
	}
}

func TestPublicationPreservesLateDestinationContents(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	target := filepath.Join(root, "restore")
	must(t, os.Mkdir(target, 0700))
	sentinel := filepath.Join(target, "existing")
	err := publish(ctx, target, func(stage *os.Root) error {
		if err := writeBytes(stage, "staged", []byte("private")); err != nil {
			return err
		}
		return os.WriteFile(sentinel, []byte("keep"), 0600)
	})
	if err == nil {
		t.Fatal("publication replaced a populated directory")
	}
	got, err := os.ReadFile(sentinel)
	must(t, err)
	if string(got) != "keep" {
		t.Fatal("publication changed destination contents")
	}
	backupChildren(t, root, stagingGuard, "restore")
}
