// Package checkpoint implements an offline, disk-backed legacy WAL checkpoint.
// It is a prototype, not a replacement for the live store or WAL reclamation.
package checkpoint

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"bog/internal/store"
)

const chunkBytes = 8 << 20
const maxRecord = store.MaxBatch + 32

var crc = crc32.MakeTable(crc32.Castagnoli)
var ErrUncertain = errors.New("checkpoint publication uncertain")

type entry struct {
	row     store.Row
	ordinal uint64
}
type manifest struct {
	Version  int    `json:"version"`
	Segment  string `json:"segment"`
	Offset   int64  `json:"offset"`
	Next     uint64 `json:"next"`
	WALDev   uint64 `json:"wal_dev"`
	WALIno   uint64 `json:"wal_ino"`
	Boundary string `json:"boundary"`
}
type envelope struct {
	Data json.RawMessage `json:"data"`
	Sum  string          `json:"sha256"`
}

func lock(dir string) (*os.File, error) {
	d, e := os.Open(dir)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(d.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		d.Close()
		return nil, e
	}
	return d, nil
}
func id() (string, error) {
	var b [16]byte
	_, e := rand.Read(b[:])
	return hex.EncodeToString(b[:]), e
}
func identity(f *os.File) (uint64, uint64, error) {
	s, e := f.Stat()
	if e != nil {
		return 0, 0, e
	}
	st, ok := s.Sys().(*syscall.Stat_t)
	if !ok || !s.Mode().IsRegular() {
		return 0, 0, errors.New("invalid WAL file")
	}
	return uint64(st.Dev), st.Ino, nil
}
func readCapped(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, errors.New("file exceeds size limit")
	}
	return b, nil
}

func committed(dir string, f *os.File) (int64, error) {
	b, e := readCapped(filepath.Join(dir, "bog.commit"), 24)
	if errors.Is(e, os.ErrNotExist) {
		s, x := f.Stat()
		if x == nil && s.Size() == 8 {
			return 8, nil
		}
		return 0, errors.New("missing commit marker")
	}
	if e != nil {
		return 0, e
	}
	if len(b) != 24 || string(b[:8]) != "BOGCMT01" {
		return 0, errors.New("invalid commit marker")
	}
	n := binary.LittleEndian.Uint64(b[8:])
	if binary.LittleEndian.Uint64(b[16:]) != ^n || n < 8 || n > uint64(^uint64(0)>>1) {
		return 0, errors.New("invalid commit offset")
	}
	s, e := f.Stat()
	if e != nil {
		return 0, e
	}
	if n > uint64(s.Size()) {
		return 0, errors.New("commit beyond WAL")
	}
	return int64(n), nil
}
func header(f *os.File) error {
	var b [8]byte
	if _, e := f.ReadAt(b[:], 0); e != nil {
		return e
	}
	if string(b[:]) != "BOGWAL01" {
		return errors.New("invalid WAL magic")
	}
	return nil
}
func boundary(f *os.File, end int64) (string, error) {
	n := int64(64)
	if end < n {
		n = end
	}
	b := make([]byte, n)
	if _, e := f.ReadAt(b, end-n); e != nil {
		return "", e
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

// walk decodes complete committed frames only; no partial batch is exposed.
func walk(f *os.File, start, end int64, fn func([]store.Row) error) error {
	if start < 8 || end < start {
		return errors.New("invalid WAL range")
	}
	p := start
	var h [16]byte
	for p < end {
		if end-p < 16 {
			return errors.New("incomplete committed frame")
		}
		if _, e := f.ReadAt(h[:], p); e != nil {
			return e
		}
		n := binary.LittleEndian.Uint32(h[:])
		if n == 0 || n > store.MaxBatch || binary.LittleEndian.Uint32(h[4:]) != ^n || crc32.Checksum(h[:12], crc) != binary.LittleEndian.Uint32(h[12:]) || int64(n) > end-p-16 {
			return fmt.Errorf("invalid committed frame at %d", p)
		}
		b := make([]byte, n)
		if _, e := f.ReadAt(b, p+16); e != nil {
			return e
		}
		if crc32.Checksum(b, crc) != binary.LittleEndian.Uint32(h[8:]) {
			return fmt.Errorf("invalid committed checksum at %d", p)
		}
		rows, e := store.Decode(b)
		if e != nil {
			return fmt.Errorf("invalid committed payload at %d: %w", p, e)
		}
		if e = fn(rows); e != nil {
			return e
		}
		p += 16 + int64(n)
	}
	return nil
}
func less(a, b entry) bool {
	if a.row.TS.Equal(b.row.TS) {
		return a.ordinal < b.ordinal
	}
	return a.row.TS.Before(b.row.TS)
}

// Segment v1: magic followed by records. Each record has a little-endian
// length, seconds, nanoseconds, ordinal, UTF-8 line and CRC32C of the body.
func put(w io.Writer, v entry) error {
	n := 24 + len(v.row.Line)
	if n > maxRecord {
		return errors.New("record too large")
	}
	b := make([]byte, 4+n+4)
	binary.LittleEndian.PutUint32(b, uint32(n))
	binary.LittleEndian.PutUint64(b[4:], uint64(v.row.TS.Unix()))
	binary.LittleEndian.PutUint32(b[12:], uint32(v.row.TS.Nanosecond()))
	binary.LittleEndian.PutUint64(b[16:], v.ordinal)
	binary.LittleEndian.PutUint32(b[24:], uint32(len(v.row.Line)))
	copy(b[28:], v.row.Line)
	binary.LittleEndian.PutUint32(b[4+n:], crc32.Checksum(b[4:4+n], crc))
	_, e := w.Write(b)
	return e
}
func get(r io.Reader) (entry, error) {
	var v entry
	var h [4]byte
	_, e := io.ReadFull(r, h[:])
	if e != nil {
		return v, e
	}
	n := binary.LittleEndian.Uint32(h[:])
	if n < 24 || n > maxRecord {
		return v, errors.New("invalid segment record length")
	}
	b := make([]byte, int(n)+4)
	if _, e = io.ReadFull(r, b); e != nil {
		return v, e
	}
	if crc32.Checksum(b[:n], crc) != binary.LittleEndian.Uint32(b[n:]) || binary.LittleEndian.Uint32(b[20:]) != n-24 {
		return v, errors.New("invalid segment record")
	}
	ns := binary.LittleEndian.Uint32(b[8:])
	if ns >= 1e9 {
		return v, errors.New("invalid nanoseconds")
	}
	v.row = store.Row{TS: time.Unix(int64(binary.LittleEndian.Uint64(b)), int64(ns)).UTC(), Line: string(b[24:n])}
	if v.row.TS.Year() < 1 || v.row.TS.Year() > 9999 {
		return v, errors.New("invalid segment timestamp")
	}
	v.ordinal = binary.LittleEndian.Uint64(b[12:])
	return v, nil
}
func createRun(dir string, rows []entry) (string, error) {
	sort.Slice(rows, func(i, j int) bool { return less(rows[i], rows[j]) })
	f, e := os.CreateTemp(dir, ".checkpoint-run-")
	if e != nil {
		return "", e
	}
	name := f.Name()
	w := bufio.NewWriterSize(f, 64<<10)
	_, e = w.WriteString("BOGSEG01")
	for _, v := range rows {
		if e != nil {
			break
		}
		e = put(w, v)
	}
	if e == nil {
		e = w.Flush()
	}
	if e == nil {
		e = f.Sync()
	}
	if x := f.Close(); e == nil {
		e = x
	}
	if e != nil {
		os.Remove(name)
		return "", e
	}
	return name, nil
}
func openRun(path string) (*os.File, *bufio.Reader, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, nil, e
	}
	r := bufio.NewReaderSize(f, 64<<10)
	var h [8]byte
	if _, e = io.ReadFull(r, h[:]); e != nil || string(h[:]) != "BOGSEG01" {
		f.Close()
		return nil, nil, errors.New("invalid segment header")
	}
	return f, r, nil
}
func merge(dir, a, b string) (string, error) {
	fa, ra, e := openRun(a)
	if e != nil {
		return "", e
	}
	defer fa.Close()
	fb, rb, e := openRun(b)
	if e != nil {
		return "", e
	}
	defer fb.Close()
	out, e := os.CreateTemp(dir, ".checkpoint-run-")
	if e != nil {
		return "", e
	}
	name := out.Name()
	w := bufio.NewWriterSize(out, 64<<10)
	_, e = w.WriteString("BOGSEG01")
	x, ex := get(ra)
	y, ey := get(rb)
	for e == nil && (ex == nil || ey == nil) {
		if ex != nil && ex != io.EOF {
			e = ex
			break
		}
		if ey != nil && ey != io.EOF {
			e = ey
			break
		}
		if ey == io.EOF || (ex == nil && less(x, y)) {
			e = put(w, x)
			x, ex = get(ra)
		} else {
			e = put(w, y)
			y, ey = get(rb)
		}
	}
	if e == nil && ex != io.EOF {
		e = ex
	}
	if e == nil && ey != io.EOF {
		e = ey
	}
	if e == nil {
		e = w.Flush()
	}
	if e == nil {
		e = out.Sync()
	}
	if x := out.Close(); e == nil {
		e = x
	}
	if e != nil {
		os.Remove(name)
		return "", e
	}
	return name, nil
}

// Build checkpoints a stopped WAL under its directory lock. Existing checkpoint
// generations and WAL inputs are retained; cleanup is intentionally separate.
func Build(dir string) (err error) {
	legacy, err := OpenLegacy(dir)
	if err != nil {
		return err
	}
	defer legacy.Close()
	d, f := legacy.dir, legacy.wal
	dir = fmt.Sprintf("/dev/fd/%d", d.Fd())
	if err = header(f); err != nil {
		return err
	}
	end := legacy.end
	dev, ino, err := identity(f)
	if err != nil {
		return err
	}
	mark, err := boundary(f, end)
	if err != nil {
		return err
	}
	var levels [64]string
	defer func() {
		for _, p := range levels {
			if p != "" {
				os.Remove(p)
			}
		}
	}()
	rows := make([]entry, 0)
	bytes := 0
	var next uint64
	flush := func() error {
		if len(rows) == 0 {
			return nil
		}
		p, e := createRun(dir, rows)
		if e != nil {
			return e
		}
		rows = nil
		bytes = 0
		for i := range levels {
			if levels[i] == "" {
				levels[i] = p
				return nil
			}
			q, e := merge(dir, levels[i], p)
			if e != nil {
				os.Remove(p)
				return e
			}
			os.Remove(levels[i])
			os.Remove(p)
			levels[i] = ""
			p = q
		}
		os.Remove(p)
		return errors.New("too many runs")
	}
	err = walk(f, 8, end, func(batch []store.Row) error {
		for _, r := range batch {
			if next == ^uint64(0) {
				return errors.New("ordinal overflow")
			}
			rows = append(rows, entry{r, next})
			next++
			bytes += len(r.Line) + 64
			if bytes >= chunkBytes {
				if e := flush(); e != nil {
					return e
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err = flush(); err != nil {
		return err
	}
	var final string
	for i, p := range levels {
		if p == "" {
			continue
		}
		if final == "" {
			final = p
			levels[i] = ""
			continue
		}
		q, e := merge(dir, final, p)
		if e != nil {
			os.Remove(final)
			return e
		}
		os.Remove(final)
		os.Remove(p)
		levels[i] = ""
		final = q
	}
	if final == "" {
		final, err = createRun(dir, nil)
		if err != nil {
			return err
		}
	}
	defer os.Remove(final)
	token, err := id()
	if err != nil {
		return err
	}
	name := "segment-" + token + ".bog"
	if err = os.Rename(final, filepath.Join(dir, name)); err != nil {
		return err
	}
	if err = d.Sync(); err != nil {
		return err
	}
	m := manifest{1, name, end, next, dev, ino, mark}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	content, err := json.Marshal(envelope{data, hex.EncodeToString(sum[:])})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".checkpoint-manifest-")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.Write(content); err == nil {
		err = tmp.Sync()
	}
	if e := tmp.Close(); err == nil {
		err = e
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp.Name(), filepath.Join(dir, "checkpoint.manifest")); err != nil {
		return fmt.Errorf("%w: %v", ErrUncertain, err)
	}
	if err = d.Sync(); err != nil {
		return fmt.Errorf("%w: %v", ErrUncertain, err)
	}
	return nil
}

// Reader holds the directory lock for its lifetime, pinning the stopped input
// pair and checkpoint generation. TailBytes caps the committed uncovered suffix.
type Reader struct {
	dir      *os.File
	legacy   *Legacy
	segment  *os.File
	tail     []entry
	count    uint64
	prefix   uint64
	WALBytes int64
}

func Open(dir string, tailBytes int64) (_ *Reader, err error) {
	legacy, err := OpenLegacy(dir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			legacy.Close()
		}
	}()
	d := legacy.dir
	dir = fmt.Sprintf("/dev/fd/%d", d.Fd())
	mf, err := legacy.openFile("checkpoint.manifest")
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(io.LimitReader(mf, (64<<10)+1))
	if e := mf.Close(); err == nil {
		err = e
	}
	if err != nil {
		return nil, err
	}
	if len(b) > 64<<10 {
		return nil, errors.New("manifest too large")
	}
	var env envelope
	if err = json.Unmarshal(b, &env); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(env.Data)
	if env.Sum != hex.EncodeToString(sum[:]) {
		return nil, errors.New("manifest checksum")
	}
	var m manifest
	if err = json.Unmarshal(env.Data, &m); err != nil {
		return nil, err
	}
	if m.Version != 1 || !strings.HasPrefix(m.Segment, "segment-") || filepath.Base(m.Segment) != m.Segment || m.Offset < 8 {
		return nil, errors.New("invalid manifest")
	}
	f := legacy.wal
	if err = header(f); err != nil {
		return nil, err
	}
	dev, ino, err := identity(f)
	if err != nil {
		return nil, err
	}
	if dev != m.WALDev || ino != m.WALIno {
		return nil, errors.New("WAL identity changed")
	}
	end := legacy.end
	if end < m.Offset || tailBytes < 0 || end-m.Offset > tailBytes {
		return nil, errors.New("WAL tail exceeds limit or checkpoint offset")
	}
	mark, err := boundary(f, m.Offset)
	if err != nil {
		return nil, err
	}
	if mark != m.Boundary {
		return nil, errors.New("WAL boundary changed")
	}
	seg, err := legacy.openFile(m.Segment)
	if err != nil {
		return nil, err
	}
	var segmentHeader [8]byte
	if _, err = io.ReadFull(seg, segmentHeader[:]); err != nil || string(segmentHeader[:]) != "BOGSEG01" {
		seg.Close()
		return nil, errors.New("invalid segment header")
	}
	defer func() {
		if err != nil {
			seg.Close()
		}
	}()
	r := &Reader{dir: d, legacy: legacy, segment: seg, count: m.Next, prefix: m.Next, WALBytes: end - m.Offset + 8 + min(m.Offset, 64)}
	err = walk(f, m.Offset, end, func(batch []store.Row) error {
		for _, row := range batch {
			if r.count == ^uint64(0) {
				return errors.New("ordinal overflow")
			}
			r.tail = append(r.tail, entry{row, r.count})
			r.count++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(r.tail, func(i, j int) bool { return less(r.tail[i], r.tail[j]) })
	// Validate all checkpoint data before exposing any rows.
	if err = r.validateSegment(); err != nil {
		return nil, err
	}
	return r, nil
}
func (r *Reader) Close() error {
	e := r.segment.Close()
	if x := r.legacy.Close(); e == nil {
		e = x
	}
	return e
}

// Query streams results in timestamp and append order. The callback can stop
// iteration by returning an error. The caller controls result buffering.
func (r *Reader) Query(start, end time.Time, literal string, emit func(store.Row, uint64) error) error {
	if !start.Before(end) {
		return nil
	}
	if _, e := r.segment.Seek(8, io.SeekStart); e != nil {
		return e
	}
	rd := bufio.NewReaderSize(r.segment, 64<<10)
	a, ea := get(rd)
	i := 0
	var previous entry
	have := false
	var seen uint64
	for ea == nil || i < len(r.tail) {
		if ea != nil && ea != io.EOF {
			return ea
		}
		var v entry
		if i >= len(r.tail) || (ea == nil && less(a, r.tail[i])) {
			v = a
			seen++
			a, ea = get(rd)
			if have && !less(previous, v) {
				return errors.New("segment order invalid")
			}
			previous = v
			have = true
			if v.ordinal >= r.prefix {
				return errors.New("invalid segment ordinal")
			}
		} else {
			v = r.tail[i]
			i++
		}
		if !v.row.TS.Before(start) && v.row.TS.Before(end) && strings.Contains(v.row.Line, literal) {
			if e := emit(v.row, v.ordinal); e != nil {
				return e
			}
		}
	}
	if ea != io.EOF {
		return ea
	}
	if seen != r.prefix {
		return errors.New("segment row count mismatch")
	}
	return nil
}

// validateSegment scans with constant memory and rejects incomplete or unordered data.
func (r *Reader) validateSegment() error {
	if _, err := r.segment.Seek(8, io.SeekStart); err != nil {
		return err
	}
	rd := bufio.NewReaderSize(r.segment, 64<<10)
	var prev entry
	var count uint64
	for {
		v, err := get(rd)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if v.ordinal >= r.prefix || (count > 0 && !less(prev, v)) {
			return errors.New("invalid segment order or ordinal")
		}
		count++
		prev = v
	}
	if count != r.prefix {
		return errors.New("segment row count mismatch")
	}
	return nil
}
