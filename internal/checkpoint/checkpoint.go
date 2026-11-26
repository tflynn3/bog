// Package checkpoint implements an offline, disk-backed checkpoint for legacy WALs.
// It is deliberately separate from the live store and does not reclaim the WAL.
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

const segmentMagic = "BOGSEG01"
const manifestMagic = "BOGMAN01"
const maxRecord = store.MaxBatch + 32
const maxManifest = 16 << 10

var crc = crc32.MakeTable(crc32.Castagnoli)
var ErrUncertain = errors.New("checkpoint publication uncertain; stop and reopen")

type record struct {
	row     store.Row
	ordinal uint64
}
type manifest struct {
	Version int    `json:"version"`
	Segment string `json:"segment"`
	Offset  int64  `json:"offset"`
	Next    uint64 `json:"next"`
	Device  uint64 `json:"device"`
	Inode   uint64 `json:"inode"`
	Size    int64  `json:"size"`
	Digest  string `json:"digest"`
}

type locked struct {
	dir, wal *os.File
	end      int64
	dev, ino uint64
}

func lock(dir string) (*locked, error) {
	d, e := os.Open(dir)
	if e != nil {
		return nil, e
	}
	fail := func(e error) (*locked, error) { d.Close(); return nil, e }
	if e = syscall.Flock(int(d.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		return fail(e)
	}
	f, e := os.Open(filepath.Join(dir, "bog.wal"))
	if e != nil {
		return fail(e)
	}
	bad := func(e error) (*locked, error) { f.Close(); return fail(e) }
	fi, e := f.Stat()
	if e != nil {
		return bad(e)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || !fi.Mode().IsRegular() {
		return bad(errors.New("invalid WAL file"))
	}
	var magic [8]byte
	if _, e = io.ReadFull(f, magic[:]); e != nil || string(magic[:]) != "BOGWAL01" {
		return bad(errors.New("invalid WAL magic"))
	}
	marker, e := os.Open(filepath.Join(dir, "bog.commit"))
	var b []byte
	if e == nil {
		b, e = io.ReadAll(io.LimitReader(marker, 25))
		marker.Close()
	}
	if os.IsNotExist(e) && fi.Size() == 8 {
		b = nil
		e = nil
	}
	if e != nil {
		return bad(e)
	}
	end := int64(8)
	if b != nil {
		if len(b) != 24 || string(b[:8]) != "BOGCMT01" {
			return bad(errors.New("invalid commit marker"))
		}
		n := binary.LittleEndian.Uint64(b[8:])
		if binary.LittleEndian.Uint64(b[16:]) != ^n || n < 8 || n > uint64(fi.Size()) {
			return bad(errors.New("invalid commit offset"))
		}
		end = int64(n)
	}
	return &locked{d, f, end, uint64(st.Dev), uint64(st.Ino)}, nil
}
func (l *locked) close() { l.wal.Close(); l.dir.Close() }

// frames validates each entire committed frame before delivering any of its rows.
func frames(f *os.File, start, end int64, fn func([]store.Row) error) error {
	if start < 8 || end < start {
		return errors.New("invalid WAL range")
	}
	r := bufio.NewReaderSize(io.NewSectionReader(f, start, end-start), 64<<10)
	for pos := start; pos < end; {
		var h [16]byte
		if end-pos < 16 {
			return errors.New("incomplete committed frame")
		}
		if _, e := io.ReadFull(r, h[:]); e != nil {
			return e
		}
		n := binary.LittleEndian.Uint32(h[:])
		if crc32.Checksum(h[:12], crc) != binary.LittleEndian.Uint32(h[12:]) || n == 0 || n > store.MaxBatch || binary.LittleEndian.Uint32(h[4:]) != ^n || int64(n) > end-pos-16 {
			return fmt.Errorf("invalid frame at %d", pos)
		}
		p := make([]byte, int(n))
		if _, e := io.ReadFull(r, p); e != nil {
			return e
		}
		if crc32.Checksum(p, crc) != binary.LittleEndian.Uint32(h[8:]) {
			return fmt.Errorf("invalid frame checksum at %d", pos)
		}
		rows, e := store.Decode(p)
		if e != nil {
			return fmt.Errorf("invalid frame payload at %d: %w", pos, e)
		}
		if e = fn(rows); e != nil {
			return e
		}
		pos += 16 + int64(n)
	}
	return nil
}
func less(a, b record) bool {
	if a.row.TS.Equal(b.row.TS) {
		return a.ordinal < b.ordinal
	}
	return a.row.TS.Before(b.row.TS)
}
func writeRecord(w io.Writer, r record) error {
	b := make([]byte, 24+len(r.row.Line))
	binary.LittleEndian.PutUint64(b, uint64(r.row.TS.Unix()))
	binary.LittleEndian.PutUint32(b[8:], uint32(r.row.TS.Nanosecond()))
	binary.LittleEndian.PutUint64(b[12:], r.ordinal)
	binary.LittleEndian.PutUint32(b[20:], uint32(len(r.row.Line)))
	copy(b[24:], r.row.Line)
	var h [8]byte
	binary.LittleEndian.PutUint32(h[:], uint32(len(b)))
	binary.LittleEndian.PutUint32(h[4:], crc32.Checksum(b, crc))
	if _, e := w.Write(h[:]); e != nil {
		return e
	}
	_, e := w.Write(b)
	return e
}

type reader struct {
	f         *os.File
	r         *bufio.Reader
	remaining int64
	prev      record
	hasPrev   bool
}

func openSegment(path string) (*reader, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	s, e := f.Stat()
	if e != nil || !s.Mode().IsRegular() || s.Size() < 8 {
		f.Close()
		return nil, errors.New("invalid segment")
	}
	var h [8]byte
	if _, e = io.ReadFull(f, h[:]); e != nil || string(h[:]) != segmentMagic {
		f.Close()
		return nil, errors.New("invalid segment magic")
	}
	return &reader{f: f, r: bufio.NewReaderSize(f, 64<<10), remaining: s.Size() - 8}, nil
}
func (r *reader) next() (record, error) {
	var z record
	if r.remaining == 0 {
		return z, io.EOF
	}
	if r.remaining < 8 {
		return z, errors.New("truncated segment header")
	}
	var h [8]byte
	if _, e := io.ReadFull(r.r, h[:]); e != nil {
		return z, e
	}
	r.remaining -= 8
	n := binary.LittleEndian.Uint32(h[:])
	if n < 24 || n > maxRecord || int64(n) > r.remaining {
		return z, errors.New("invalid segment record length")
	}
	b := make([]byte, n)
	if _, e := io.ReadFull(r.r, b); e != nil {
		return z, e
	}
	r.remaining -= int64(n)
	if crc32.Checksum(b, crc) != binary.LittleEndian.Uint32(h[4:]) || binary.LittleEndian.Uint32(b[20:]) != n-24 || binary.LittleEndian.Uint32(b[8:]) >= 1e9 {
		return z, errors.New("invalid segment record")
	}
	z = record{store.Row{TS: time.Unix(int64(binary.LittleEndian.Uint64(b)), int64(binary.LittleEndian.Uint32(b[8:]))).UTC(), Line: string(b[24:])}, binary.LittleEndian.Uint64(b[12:])}
	if z.row.TS.Year() < 1 || z.row.TS.Year() > 9999 || (r.hasPrev && !less(r.prev, z)) {
		return record{}, errors.New("invalid segment ordering")
	}
	r.prev = z
	r.hasPrev = true
	return z, nil
}
func temp(dir string) (*os.File, error) { return os.CreateTemp(dir, ".bog-checkpoint-") }
func newSegment(dir string) (*os.File, error) {
	f, e := temp(dir)
	if e != nil {
		return nil, e
	}
	if _, e = f.WriteString(segmentMagic); e != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, e
	}
	return f, nil
}
func finish(f *os.File) error {
	e := f.Sync()
	if c := f.Close(); e == nil {
		e = c
	}
	return e
}
func merge(dir, a, b string) (string, error) {
	ra, e := openSegment(a)
	if e != nil {
		return "", e
	}
	defer ra.f.Close()
	rb, e := openSegment(b)
	if e != nil {
		return "", e
	}
	defer rb.f.Close()
	out, e := newSegment(dir)
	if e != nil {
		return "", e
	}
	name := out.Name()
	ok := false
	defer func() {
		if !ok {
			out.Close()
			os.Remove(name)
		}
	}()
	w := bufio.NewWriterSize(out, 64<<10)
	x, ex := ra.next()
	y, ey := rb.next()
	for ex == nil || ey == nil {
		if ex != nil && ex != io.EOF {
			return "", ex
		}
		if ey != nil && ey != io.EOF {
			return "", ey
		}
		if ey == io.EOF || (ex == nil && less(x, y)) {
			e = writeRecord(w, x)
			x, ex = ra.next()
		} else {
			e = writeRecord(w, y)
			y, ey = rb.next()
		}
		if e != nil {
			return "", e
		}
	}
	if ex != io.EOF {
		return "", ex
	}
	if ey != io.EOF {
		return "", ey
	}
	if e = w.Flush(); e == nil {
		e = finish(out)
	}
	if e != nil {
		return "", e
	}
	ok = true
	return name, nil
}

// Build creates a checkpoint while holding the legacy directory lock. chunkBytes
// bounds buffered sort input (one decoded MaxBatch frame can exceed this target).
func Build(dir string, chunkBytes int) error {
	if chunkBytes < 1 {
		return errors.New("invalid chunk size")
	}
	l, e := lock(dir)
	if e != nil {
		return e
	}
	defer l.close()
	var levels []string
	var chunk []record
	size := 0
	var ordinal uint64
	cleanup := func() {
		for _, p := range levels {
			if p != "" {
				os.Remove(p)
			}
		}
	}
	defer cleanup()
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		sort.Slice(chunk, func(i, j int) bool { return less(chunk[i], chunk[j]) })
		f, e := newSegment(dir)
		if e != nil {
			return e
		}
		name := f.Name()
		w := bufio.NewWriterSize(f, 64<<10)
		for _, v := range chunk {
			if e = writeRecord(w, v); e != nil {
				break
			}
		}
		if e == nil {
			e = w.Flush()
		}
		if e == nil {
			e = finish(f)
		} else {
			f.Close()
		}
		if e != nil {
			os.Remove(name)
			return e
		}
		chunk = nil
		size = 0
		for i := 0; ; i++ {
			if i == len(levels) {
				levels = append(levels, name)
				break
			}
			if levels[i] == "" {
				levels[i] = name
				break
			}
			next, err := merge(dir, levels[i], name)
			if err != nil {
				os.Remove(name)
				return err
			}
			os.Remove(levels[i])
			os.Remove(name)
			levels[i] = ""
			name = next
		}
		return nil
	}
	e = frames(l.wal, 8, l.end, func(rows []store.Row) error {
		for _, row := range rows {
			if ordinal == ^uint64(0) {
				return errors.New("ordinal overflow")
			}
			chunk = append(chunk, record{row, ordinal})
			ordinal++
			size += len(row.Line) + 64
			if size >= chunkBytes {
				if e := flush(); e != nil {
					return e
				}
			}
		}
		return nil
	})
	if e != nil {
		return e
	}
	if e = flush(); e != nil {
		return e
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
		next, err := merge(dir, final, p)
		if err != nil {
			os.Remove(final)
			return err
		}
		os.Remove(final)
		os.Remove(p)
		levels[i] = ""
		final = next
	}
	if final == "" {
		f, err := newSegment(dir)
		if err != nil {
			return err
		}
		final = f.Name()
		if err = finish(f); err != nil {
			return err
		}
	}
	defer os.Remove(final)
	var id [16]byte
	if _, e = rand.Read(id[:]); e != nil {
		return e
	}
	name := "bog-segment-" + hex.EncodeToString(id[:])
	if e = os.Rename(final, filepath.Join(dir, name)); e != nil {
		return e
	}
	if e = l.dir.Sync(); e != nil {
		return e
	}
	segSize, digest, e := digestFile(filepath.Join(dir, name))
	if e != nil {
		return e
	}
	m := manifest{Version: 1, Segment: name, Offset: l.end, Next: ordinal, Device: l.dev, Inode: l.ino, Size: segSize, Digest: digest}
	data, e := json.Marshal(m)
	if e != nil {
		return e
	}
	var h [16]byte
	copy(h[:8], manifestMagic)
	binary.LittleEndian.PutUint32(h[8:], uint32(len(data)))
	binary.LittleEndian.PutUint32(h[12:], crc32.Checksum(data, crc))
	f, e := temp(dir)
	if e != nil {
		return e
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, e = f.Write(h[:]); e == nil {
		_, e = f.Write(data)
	}
	if e == nil {
		e = finish(f)
	} else {
		f.Close()
	}
	if e != nil {
		return e
	}
	if e = os.Rename(tmp, filepath.Join(dir, "bog.checkpoint")); e != nil {
		return fmt.Errorf("%w: %v", ErrUncertain, e)
	}
	if e = l.dir.Sync(); e != nil {
		return fmt.Errorf("%w: %v", ErrUncertain, e)
	}
	return nil
}

func digestFile(path string) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, "", err
	}
	if !st.Mode().IsRegular() {
		return 0, "", errors.New("segment is not regular")
	}
	h := sha256.New()
	n, err := io.CopyBuffer(h, f, make([]byte, 64<<10))
	if err != nil {
		return 0, "", err
	}
	if n != st.Size() {
		return 0, "", errors.New("segment changed")
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

func readManifest(dir string) (manifest, error) {
	var m manifest
	f, e := os.Open(filepath.Join(dir, "bog.checkpoint"))
	if e != nil {
		return m, e
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, maxManifest+1))
	if e != nil {
		return m, e
	}
	if len(b) < 16 || len(b) > maxManifest || string(b[:8]) != manifestMagic || int(binary.LittleEndian.Uint32(b[8:])) != len(b)-16 || crc32.Checksum(b[16:], crc) != binary.LittleEndian.Uint32(b[12:]) {
		return m, errors.New("invalid manifest")
	}
	if e = json.Unmarshal(b[16:], &m); e != nil {
		return m, e
	}
	if m.Version != 1 || !strings.HasPrefix(m.Segment, "bog-segment-") || filepath.Base(m.Segment) != m.Segment || m.Offset < 8 {
		return m, errors.New("invalid manifest fields")
	}
	return m, nil
}

// Scan streams matching rows in timestamp and append order. maxTailBytes limits
// committed WAL bytes decoded into memory. The callback may stop by returning an error.
func Scan(dir string, start, end time.Time, literal string, maxTailBytes int64, emit func(store.Row, uint64) error) error {
	l, e := lock(dir)
	if e != nil {
		return e
	}
	defer l.close()
	m, e := readManifest(dir)
	if e != nil {
		return e
	}
	if m.Device != l.dev || m.Inode != l.ino || m.Offset > l.end {
		return errors.New("WAL identity or committed boundary changed")
	}
	if maxTailBytes < 0 || l.end-m.Offset > maxTailBytes {
		return errors.New("WAL tail exceeds limit")
	}
	size, digest, e := digestFile(filepath.Join(dir, m.Segment))
	if e != nil {
		return e
	}
	if size != m.Size || digest != m.Digest {
		return errors.New("segment digest mismatch")
	}
	r, e := openSegment(filepath.Join(dir, m.Segment))
	if e != nil {
		return e
	}
	defer r.f.Close()
	var tail []record
	next := m.Next
	e = frames(l.wal, m.Offset, l.end, func(rows []store.Row) error {
		for _, row := range rows {
			if next == ^uint64(0) {
				return errors.New("ordinal overflow")
			}
			tail = append(tail, record{row, next})
			next++
		}
		return nil
	})
	if e != nil {
		return e
	}
	sort.Slice(tail, func(i, j int) bool { return less(tail[i], tail[j]) })
	i := 0
	x, ex := r.next()
	for ex == nil || i < len(tail) {
		if ex != nil && ex != io.EOF {
			return ex
		}
		var v record
		if i == len(tail) || (ex == nil && less(x, tail[i])) {
			v = x
			if v.ordinal >= m.Next {
				return errors.New("invalid segment ordinal")
			}
			x, ex = r.next()
		} else {
			v = tail[i]
			i++
		}
		if !v.row.TS.Before(start) && v.row.TS.Before(end) && strings.Contains(v.row.Line, literal) {
			if e = emit(v.row, v.ordinal); e != nil {
				return e
			}
		}
	}
	if ex != io.EOF {
		return ex
	}
	return nil
}
