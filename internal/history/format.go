package history

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"syscall"
	"time"

	"bog/internal/checkpoint"
)

const (
	manifestName  = "bog.checkpoint"
	manifestMax   = 256 << 10
	segmentPrefix = "bog-segment-"
	version       = 1

	walMagicSize  = len("BOGWAL01")
	minWALEnd     = walMagicSize + 16 + 1 // Frame header and nonempty payload.
	headerSize    = 44
	rowHeaderSize = 24
	maxSeekPoints = 128
)

var (
	crcTable = crc32.MakeTable(crc32.Castagnoli)
	minTime  = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	maxTime  = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
)

type manifest = checkpoint.Manifest
type segmentMeta = checkpoint.Segment

type seekPoint struct {
	key    Position
	offset int64
}

type segment struct {
	name   string
	digest [sha256.Size]byte
	stat   os.FileInfo
	lo, hi time.Time // Inclusive verified timestamp bounds.
	points []seekPoint

	// Used only while verifying that the manifest covers each ordinal once.
	first, last uint64 // Inclusive ordinal bounds.
	dense       bool
}

func parseDigest(s string) ([sha256.Size]byte, bool) {
	var sum [sha256.Size]byte
	if len(s) != hex.EncodedLen(len(sum)) || strings.Trim(s, "0123456789abcdef") != "" {
		return sum, false
	}
	_, err := hex.Decode(sum[:], []byte(s))
	return sum, err == nil
}

func readManifest(dir *os.File, name string) (_ manifest, _ GenerationID, err error) {
	f, info, err := openRegular(dir, name)
	if err != nil {
		return manifest{}, GenerationID{}, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if info.Size() <= 0 || info.Size() > manifestMax {
		return manifest{}, GenerationID{}, fmt.Errorf("%w: invalid manifest size", ErrIntegrity)
	}
	b, err := io.ReadAll(io.LimitReader(f, manifestMax+1))
	if err != nil {
		return manifest{}, GenerationID{}, err
	}
	if int64(len(b)) != info.Size() {
		return manifest{}, GenerationID{}, fmt.Errorf("%w: manifest size changed", ErrIntegrity)
	}
	m, id, err := checkpoint.DecodeManifest(b)
	if err != nil {
		return manifest{}, GenerationID{}, fmt.Errorf("%w: %w", ErrIntegrity, err)
	}
	if name != manifestName && name != checkpoint.GenerationManifestName(id) {
		return manifest{}, GenerationID{}, fmt.Errorf("%w: selected manifest identity", ErrIntegrity)
	}
	return *m, id, nil
}

func openRegular(dir *os.File, name string) (*os.File, os.FileInfo, error) {
	// All names are either constant or checked against the v1 basename format.
	fd, err := openat(int(dir.Fd()), name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, nil, &os.PathError{Op: "openat", Path: name, Err: err}
	}
	f := os.NewFile(uintptr(fd), name)
	info, err := f.Stat()
	if err == nil {
		st, ok := info.Sys().(*syscall.Stat_t)
		if !info.Mode().IsRegular() || !ok || st.Nlink != 1 || st.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0022 != 0 {
			err = fmt.Errorf("%w: unsafe file %s", ErrIntegrity, name)
		}
	}
	if err != nil {
		return nil, nil, errors.Join(err, f.Close())
	}
	return f, info, nil
}

func openDirectory(path string) (*os.File, os.FileInfo, error) {
	dir, err := os.OpenFile(path, os.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		return nil, nil, err
	}
	return checkDirectory(dir)
}

// Reopen relative to the authority descriptor so the catalog owns an
// independent directory handle without consulting the configured path.
func openPinnedDirectory(dir *os.File) (*os.File, os.FileInfo, error) {
	if dir == nil {
		return nil, nil, errors.New("missing pinned history directory")
	}
	fd, err := openat(int(dir.Fd()), ".", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, nil, err
	}
	return checkDirectory(os.NewFile(uintptr(fd), dir.Name()))
}

func checkDirectory(dir *os.File) (*os.File, os.FileInfo, error) {
	info, err := dir.Stat()
	if err != nil {
		return nil, nil, errors.Join(err, dir.Close())
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || !ok || st.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0022 != 0 {
		return nil, nil, errors.Join(fmt.Errorf("%w: unsafe checkpoint directory", ErrIntegrity), dir.Close())
	}
	return dir, info, nil
}

// loadAt requires the publication lock and does not take ownership of dir.
func loadAt(ctx context.Context, dir *os.File) (*generation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := checkpoint.CheckPublication(dir); err != nil {
		return nil, err
	}
	g, _, err := loadAuthority(ctx, dir, manifestName)
	return g, err
}

// ValidatePinnedAuthority validates the manifest and every referenced segment.
// The caller must hold the publication lock, keep dir open, and independently
// validate the WAL boundary. Recovery can call this with staging present.
func ValidatePinnedAuthority(ctx context.Context, dir *os.File) (*checkpoint.Manifest, error) {
	if dir == nil {
		return nil, errors.New("missing pinned history directory")
	}
	_, m, err := loadAuthority(ctx, dir, manifestName)
	return m, err
}

func loadAuthority(ctx context.Context, dir *os.File, name string) (*generation, *checkpoint.Manifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	m, id, err := readManifest(dir, name)
	if err != nil {
		return nil, nil, err
	}
	source, ok := parseDigest(m.SourceSHA256)
	if m.Version == 1 && !ok {
		return nil, nil, fmt.Errorf("%w: invalid WAL prefix checksum", ErrIntegrity)
	}
	g := &generation{dir: dir, drained: make(chan struct{}), segments: make([]segment, len(m.Segments))}
	g.info = Generation{
		ID: id, Boundary: Boundary{SourceSHA256: source, CoveredOffset: m.CoveredOffset, NextOrdinal: m.NextOrdinal},
		Drained: g.drained,
	}
	seen := make(map[string]bool, len(m.Segments))
	var maxRows uint64
	var maxSize int
	for i, s := range m.Segments {
		if !validSegmentName(s.Name) || seen[s.Name] ||
			s.Size < headerSize+rowHeaderSize || s.Size > checkpoint.MaxBlockSize {
			return nil, nil, fmt.Errorf("%w: invalid segment metadata for entry %d", ErrIntegrity, i)
		}
		sum, ok := parseDigest(s.SHA256)
		if !ok {
			return nil, nil, fmt.Errorf("%w: invalid segment checksum for entry %d", ErrIntegrity, i)
		}
		g.segments[i] = segment{name: s.Name, digest: sum}
		seen[s.Name] = true
		maxRows += uint64((s.Size - headerSize) / rowHeaderSize)
		maxSize = max(maxSize, int(s.Size))
	}
	if m.NextOrdinal < uint64(len(m.Segments)) || m.NextOrdinal > maxRows {
		return nil, nil, fmt.Errorf("%w: impossible checkpoint row count", ErrIntegrity)
	}
	if err := g.validate(ctx, m, make([]byte, maxSize), name != manifestName); err != nil {
		return nil, nil, err
	}
	return g, &m, nil
}

const segmentNameSize = len(segmentPrefix) + 32

func validSegmentName(name string) bool {
	return len(name) == segmentNameSize && strings.HasPrefix(name, segmentPrefix) &&
		strings.Trim(name[len(segmentPrefix):], "0123456789abcdef") == ""
}

func getTime(b []byte) (time.Time, bool) {
	sec := int64(binary.LittleEndian.Uint64(b[:8]))
	ns := binary.LittleEndian.Uint32(b[8:12])
	if ns >= 1e9 || sec < minTime.Unix() || sec > maxTime.Unix() {
		return time.Time{}, false
	}
	t := time.Unix(sec, int64(ns)).UTC()
	return t, t.Before(maxTime)
}

// parseRow checks the fixed header and the declared line bounds before the
// caller reads or allocates the line. offset points to the fixed row header.
func parseRow(b []byte, offset, end int64) (Position, int64, bool) {
	if len(b) < rowHeaderSize || offset < headerSize || end-offset < rowHeaderSize {
		return Position{}, 0, false
	}
	t, ok := getTime(b[:12])
	n := int64(binary.LittleEndian.Uint32(b[20:24]))
	if !ok || n > end-offset-rowHeaderSize {
		return Position{}, 0, false
	}
	return Position{Time: t, Ordinal: binary.LittleEndian.Uint64(b[12:20])}, n, true
}

func readExactAt(f *os.File, b []byte, off int64) error {
	n, err := f.ReadAt(b, off)
	if err == nil && n != len(b) {
		return io.ErrUnexpectedEOF
	}
	return err
}

func (s *segment) readVerified(dir *os.File, b []byte, streaming bool) (err error) {
	f, info, err := openRegular(dir, s.name)
	if err != nil {
		return segmentError(s.name, err)
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if info.Size() != int64(len(b)) || (s.stat != nil && !sameSegment(s.stat, info)) {
		return corrupt(s.name, "segment file changed")
	}
	if err := readExactAt(f, b, 0); err != nil {
		return segmentError(s.name, err)
	}
	if sha256.Sum256(b) != s.digest {
		return corrupt(s.name, "segment checksum")
	}
	if streaming {
		discardValidationCache(f)
	}
	s.stat = info
	return nil
}

func sameSegment(a, b os.FileInfo) bool {
	// Both stats come from openRegular, which checks their concrete type.
	return a.Size() == b.Size() && a.ModTime().Equal(b.ModTime()) &&
		changeTime(a.Sys().(*syscall.Stat_t)) == changeTime(b.Sys().(*syscall.Stat_t)) &&
		os.SameFile(a, b)
}

func corrupt(name, reason string) error {
	return fmt.Errorf("%w: %s: %s", ErrIntegrity, name, reason)
}

func segmentError(name string, err error) error {
	if errors.Is(err, os.ErrNotExist) {
		return corrupt(name, "referenced segment is missing")
	}
	return fmt.Errorf("%w: %s: %w", ErrIntegrity, name, err)
}
