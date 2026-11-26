// Package backup exports and restores pinned Bog checkpoint recovery points.
package backup

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"bog/internal/store"
)

const (
	metadataName  = "bog.backup"
	manifestName  = "bog.checkpoint"
	tailName      = "bog.wal.tail"
	bundleMagic   = "BOGBAK01"
	walMagic      = "BOGWAL01"
	metadataSize  = 128
	manifestLimit = 256 << 10
	segmentLimit  = 1024
	blockHeader   = 44
	rowHeader     = 24
	blockLimit    = store.MaxBatch + blockHeader + rowHeader
	segmentPrefix = "bog-segment-"
)

var (
	// ErrIntegrity identifies inconsistent or malformed backup data.
	ErrIntegrity = errors.New("backup integrity error")
	// ErrPublicationUncertain requires reopening and verifying the destination
	// before another publication attempt.
	ErrPublicationUncertain = errors.New("backup publication uncertain")
	crcTable                = crc32.MakeTable(crc32.Castagnoli)
	little                  = binary.LittleEndian
	minTime                 = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	maxTime                 = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
)

// Point identifies the selected manifest payload and absolute legacy WAL
// offsets. NextOrdinal is the append ordinal of the first uncovered row.
type Point struct {
	Generation    [sha256.Size]byte
	CoveredOffset int64
	NextOrdinal   uint64
	CommittedEnd  int64
}

type metadata struct {
	Point
	tailSum [sha256.Size]byte
}

func (m metadata) tailSize() int64 { return m.CommittedEnd - m.CoveredOffset }

func (m metadata) encode() []byte {
	b := make([]byte, metadataSize)
	copy(b, bundleMagic)
	copy(b[8:40], m.Generation[:])
	little.PutUint64(b[40:48], uint64(m.CoveredOffset))
	little.PutUint64(b[48:56], m.NextOrdinal)
	little.PutUint64(b[56:64], uint64(m.CommittedEnd))
	copy(b[64:96], m.tailSum[:])
	sum := sha256.Sum256(b[:96])
	copy(b[96:], sum[:])
	return b
}

func decodeMetadata(b []byte) (metadata, error) {
	if len(b) != metadataSize || string(b[:8]) != bundleMagic || sha256.Sum256(b[:96]) != [32]byte(b[96:]) {
		return metadata{}, corrupt("invalid bundle metadata")
	}
	return metadata{
		Point: Point{Generation: [32]byte(b[8:40]), CoveredOffset: int64(little.Uint64(b[40:48])),
			NextOrdinal: little.Uint64(b[48:56]), CommittedEnd: int64(little.Uint64(b[56:64]))},
		tailSum: [32]byte(b[64:96]),
	}, nil
}

type segment struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type manifest struct {
	Version       int       `json:"version"`
	SourceSHA256  string    `json:"source_sha256"`
	CoveredOffset int64     `json:"covered_offset"`
	NextOrdinal   uint64    `json:"next_ordinal"`
	Segments      []segment `json:"segments"`
}

func readManifest(b []byte, p Point) (manifest, error) {
	var env struct {
		Payload json.RawMessage `json:"payload"`
		SHA256  string          `json:"sha256"`
	}
	var m manifest
	if err := json.Unmarshal(b, &env); err != nil {
		return m, corrupt("manifest envelope: %v", err)
	}
	id := sha256.Sum256(env.Payload)
	if id != p.Generation || hex.EncodeToString(id[:]) != env.SHA256 {
		return m, corrupt("manifest identity mismatch")
	}
	if err := json.Unmarshal(env.Payload, &m); err != nil {
		return m, corrupt("manifest payload: %v", err)
	}
	empty := m.NextOrdinal == 0 && m.CoveredOffset == int64(len(walMagic))
	populated := m.NextOrdinal != 0 && m.CoveredOffset >= int64(len(walMagic))+17
	if m.Version != 1 || len(m.Segments) > segmentLimit || !(empty || populated) ||
		m.CoveredOffset != p.CoveredOffset || m.NextOrdinal != p.NextOrdinal || p.CommittedEnd < p.CoveredOffset ||
		!lowerHex(m.SourceSHA256, 64) {
		return m, corrupt("invalid manifest boundary, version or size")
	}
	if empty {
		sum := sha256.Sum256([]byte(walMagic))
		if m.SourceSHA256 != hex.EncodeToString(sum[:]) {
			return m, corrupt("empty checkpoint WAL identity mismatch")
		}
	}
	seen := make(map[string]bool, len(m.Segments))
	var maxRows uint64
	for _, s := range m.Segments {
		suffix, ok := strings.CutPrefix(s.Name, segmentPrefix)
		if !ok || !lowerHex(suffix, 32) || seen[s.Name] || !lowerHex(s.SHA256, 64) ||
			s.Size < blockHeader+rowHeader || s.Size > blockLimit {
			return m, corrupt("invalid segment metadata for %q", s.Name)
		}
		seen[s.Name] = true
		maxRows += uint64((s.Size - blockHeader) / rowHeader)
	}
	if m.NextOrdinal < uint64(len(m.Segments)) || m.NextOrdinal > maxRows {
		return m, corrupt("impossible checkpoint row count")
	}
	return m, nil
}

func lowerHex(s string, n int) bool {
	return len(s) == n && strings.Trim(s, "0123456789abcdef") == ""
}

func corrupt(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrIntegrity, fmt.Sprintf(format, args...))
}

type walDecoder struct {
	payload []byte
}

func (d *walDecoder) frame(r io.Reader, off, end int64) ([]store.Row, int64, error) {
	var header [16]byte
	if end-off < int64(len(header)) {
		return nil, 0, corrupt("partial WAL header at %d", off)
	}
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, 0, fmt.Errorf("read WAL header at %d: %w", off, err)
	}
	n := little.Uint32(header[:4])
	if n == 0 || n > store.MaxBatch || int64(n) > end-off-int64(len(header)) ||
		little.Uint32(header[4:8]) != ^n || little.Uint32(header[12:]) != crc32.Checksum(header[:12], crcTable) {
		return nil, 0, corrupt("invalid WAL header or frame boundary at %d", off)
	}
	if cap(d.payload) < int(n) {
		d.payload = make([]byte, n)
	}
	d.payload = d.payload[:n]
	if _, err := io.ReadFull(r, d.payload); err != nil {
		return nil, 0, fmt.Errorf("read WAL payload at %d: %w", off, err)
	}
	if little.Uint32(header[8:12]) != crc32.Checksum(d.payload, crcTable) {
		return nil, 0, corrupt("WAL payload checksum at %d", off)
	}
	rows, err := store.Decode(d.payload)
	if err != nil {
		return nil, 0, corrupt("invalid WAL batch at %d: %v", off, err)
	}
	return rows, off + int64(len(header)) + int64(n), nil
}

type walFrameVisitor func(off int64, firstOrdinal uint64, rows []store.Row) error

// walkWAL consumes exactly the declared interval. The callback receives only
// fully decoded frames and their absolute WAL offsets.
func walkWAL(ctx context.Context, r io.Reader, start, end int64, ordinal uint64, visit walFrameVisitor) (uint64, error) {
	var decoder walDecoder
	for off := start; off < end; {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		rows, next, err := decoder.frame(r, off, end)
		if err != nil {
			return 0, err
		}
		if uint64(len(rows)) > ^uint64(0)-ordinal {
			return 0, corrupt("WAL ordinal overflow")
		}
		if visit != nil {
			if err := visit(off, ordinal, rows); err != nil {
				return 0, err
			}
		}
		ordinal += uint64(len(rows))
		off = next
	}
	return ordinal, ctx.Err()
}

func blockTime(b []byte) (time.Time, bool) {
	sec, ns := int64(little.Uint64(b)), little.Uint32(b[8:])
	if ns >= 1e9 || sec < minTime.Unix() || sec > maxTime.Unix() {
		return time.Time{}, false
	}
	t := time.Unix(sec, int64(ns)).UTC()
	return t, t.Before(maxTime)
}

func rowKey(header []byte, remaining int64) (Position, int64, bool) {
	ts, ok := blockTime(header[:12])
	n := int64(little.Uint32(header[20:24]))
	return Position{ts, little.Uint64(header[12:20])}, n, ok && n <= remaining
}

type bounds struct{ min, max time.Time }

func validateBlock(b []byte, s segment, ordinals []byte, next uint64) (bounds, uint64, error) {
	if sum := sha256.Sum256(b); hex.EncodeToString(sum[:]) != s.SHA256 {
		return bounds{}, 0, corrupt("segment checksum for %s", s.Name)
	}
	count := uint64(little.Uint32(b[4:8]))
	lo, loOK := blockTime(b[12:24])
	hi, hiOK := blockTime(b[24:36])
	if little.Uint32(b[:4]) != 1 || count == 0 || count > uint64((len(b)-blockHeader)/rowHeader) ||
		int64(little.Uint32(b[8:12])) != int64(len(b)-blockHeader) ||
		little.Uint32(b[36:40]) != crc32.Checksum(b[blockHeader:], crcTable) ||
		little.Uint32(b[40:44]) != crc32.Checksum(b[:40], crcTable) || !loOK || !hiOK || hi.Before(lo) {
		return bounds{}, 0, corrupt("invalid block header for %s", s.Name)
	}
	payload := b[blockHeader:]
	var prev Position
	for i := uint64(0); i < count; i++ {
		if len(payload) < rowHeader {
			return bounds{}, 0, corrupt("truncated row header in %s", s.Name)
		}
		key, n, ok := rowKey(payload[:rowHeader], int64(len(payload)-rowHeader))
		payload = payload[rowHeader:]
		if !ok || !utf8.Valid(payload[:n]) || key.Ordinal >= next || (i != 0 && !prev.less(key)) ||
			(i == 0 && !key.Time.Equal(lo)) {
			return bounds{}, 0, corrupt("invalid row or order in %s", s.Name)
		}
		mask := byte(1 << (key.Ordinal % 8))
		if ordinals[key.Ordinal/8]&mask != 0 {
			return bounds{}, 0, corrupt("duplicate ordinal %d", key.Ordinal)
		}
		ordinals[key.Ordinal/8] |= mask
		prev, payload = key, payload[n:]
	}
	if len(payload) != 0 || !prev.Time.Equal(hi) {
		return bounds{}, 0, corrupt("invalid block bounds or length for %s", s.Name)
	}
	return bounds{lo, hi}, count, nil
}
