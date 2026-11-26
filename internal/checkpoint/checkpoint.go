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
	Version     int    `json:"version"`
	Segment     string `json:"segment"`
	Offset      int64  `json:"offset"`
	Next        uint64 `json:"next"`
	WALDev      uint64 `json:"wal_dev"`
	WALIno      uint64 `json:"wal_ino"`
	Boundary    string `json:"boundary"`
	SegmentSize int64  `json:"segment_size"`
	SegmentSHA  string `json:"segment_sha256"`
}
type envelope struct {
	Data json.RawMessage `json:"data"`
	Sum  string          `json:"sha256"`
}

func id() (string, error) {
	var b [16]byte
	_, e := rand.Read(b[:])
	return hex.EncodeToString(b[:]), e
}
func createTemp(root *os.Root, prefix string) (*os.File, error) {
	for i := 0; i < 10; i++ {
		token, err := id()
		if err != nil {
			return nil, err
		}
		f, err := root.OpenFile(prefix+token, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		return f, err
	}
	return nil, errors.New("temporary name collisions")
}
func segmentDigest(root *os.Root, name string) (int64, string, error) {
	f, err := root.Open(name)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, "", err
	}
	if !info.Mode().IsRegular() {
		return 0, "", errors.New("invalid segment file")
	}
	h := sha256.New()
	n, err := io.CopyBuffer(h, f, make([]byte, 64<<10))
	return n, hex.EncodeToString(h.Sum(nil)), err
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
func createRun(root *os.Root, rows []entry) (string, error) {
	sort.Slice(rows, func(i, j int) bool { return less(rows[i], rows[j]) })
	f, e := createTemp(root, ".checkpoint-run-")
	if e != nil {
		return "", e
	}
	name := filepath.Base(f.Name())
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
		root.Remove(name)
		return "", e
	}
	return name, nil
}
func openRun(root *os.Root, path string) (*os.File, *bufio.Reader, error) {
	f, e := root.Open(path)
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
func merge(root *os.Root, a, b string) (string, error) {
	fa, ra, e := openRun(root, a)
	if e != nil {
		return "", e
	}
	defer fa.Close()
	fb, rb, e := openRun(root, b)
	if e != nil {
		return "", e
	}
	defer fb.Close()
	out, e := createTemp(root, ".checkpoint-run-")
	if e != nil {
		return "", e
	}
	name := filepath.Base(out.Name())
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
		root.Remove(name)
		return "", e
	}
	return name, nil
}

// Build checkpoints a stopped WAL under its directory lock. Existing checkpoint
// generations and WAL inputs are retained; cleanup is intentionally separate.
func Build(dir string) (err error) {
	l, err := OpenLegacy(dir)
	if err != nil {
		return err
	}
	defer l.Close()
	root, d, f := l.root, l.dir, l.wal
	if err = header(f); err != nil {
		return err
	}
	end := l.end
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
				root.Remove(p)
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
		p, e := createRun(root, rows)
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
			q, e := merge(root, levels[i], p)
			if e != nil {
				root.Remove(p)
				return e
			}
			root.Remove(levels[i])
			root.Remove(p)
			levels[i] = ""
			p = q
		}
		root.Remove(p)
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
		q, e := merge(root, final, p)
		if e != nil {
			root.Remove(final)
			return e
		}
		root.Remove(final)
		root.Remove(p)
		levels[i] = ""
		final = q
	}
	if final == "" {
		final, err = createRun(root, nil)
		if err != nil {
			return err
		}
	}
	defer root.Remove(final)
	token, err := id()
	if err != nil {
		return err
	}
	name := "segment-" + token + ".bog"
	if err = root.Rename(final, name); err != nil {
		return err
	}
	if err = d.Sync(); err != nil {
		return err
	}
	size, digest, err := segmentDigest(root, name)
	if err != nil {
		return err
	}
	m := manifest{Version: 1, Segment: name, Offset: end, Next: next, WALDev: dev, WALIno: ino, Boundary: mark, SegmentSize: size, SegmentSHA: digest}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	content, err := json.Marshal(envelope{data, hex.EncodeToString(sum[:])})
	if err != nil {
		return err
	}
	tmp, err := createTemp(root, ".checkpoint-manifest-")
	if err != nil {
		return err
	}
	defer root.Remove(filepath.Base(tmp.Name()))
	if _, err = tmp.Write(content); err == nil {
		err = tmp.Sync()
	}
	if e := tmp.Close(); err == nil {
		err = e
	}
	if err != nil {
		return err
	}
	if err = root.Rename(filepath.Base(tmp.Name()), "checkpoint.manifest"); err != nil {
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
	legacy   *Legacy
	segment  *os.File
	tail     []entry
	count    uint64
	prefix   uint64
	WALBytes int64
}

func Open(dir string, tailBytes int64) (_ *Reader, err error) {
	l, err := OpenLegacy(dir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			l.Close()
		}
	}()
	root, f := l.root, l.wal
	mf, err := l.openFile("checkpoint.manifest")
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(io.LimitReader(mf, 64<<10+1))
	mf.Close()
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
	if m.Version != 1 || !strings.HasPrefix(m.Segment, "segment-") || strings.ContainsAny(m.Segment, "/\\") || m.Offset < 8 {
		return nil, errors.New("invalid manifest")
	}
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
	end := l.end
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
	size, digest, err := segmentDigest(root, m.Segment)
	if err != nil {
		return nil, err
	}
	if size != m.SegmentSize || digest != m.SegmentSHA {
		return nil, errors.New("segment checksum or size mismatch")
	}
	seg, _, err := openRun(root, m.Segment)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			seg.Close()
		}
	}()
	r := &Reader{legacy: l, segment: seg, count: m.Next, prefix: m.Next, WALBytes: end - m.Offset + 8 + min(m.Offset, 64)}
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
