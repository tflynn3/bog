package checkpoint

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
)

// Manifest records a single generation and the identity and exact committed
// boundary of the WAL from which its segments were built.
type Manifest struct {
	Generation                 [16]byte
	WALDevice, WALInode        uint64
	CoveredOffset, NextOrdinal uint64
	Segments                   [][16]byte
	SegmentRows                []uint64
	FrameStart                 uint64
	FrameHash                  [32]byte
}

const (
	manifestMagic        = "BOGMAN03"
	manifestVersion      = 3
	manifestHeaderSize   = 104
	manifestChecksumSize = 4
	MaxManifestSegments  = 4096
	MaxManifestBytes     = manifestHeaderSize + 24*MaxManifestSegments + manifestChecksumSize
)

var errManifest = errors.New("invalid checkpoint manifest")

func (m Manifest) validate() error {
	if m.Generation == ([16]byte{}) || m.WALInode == 0 || m.CoveredOffset < 8 || m.CoveredOffset > math.MaxInt64 || len(m.Segments) > MaxManifestSegments || (m.CoveredOffset == 8 && (m.NextOrdinal != 0 || len(m.Segments) != 0)) || (m.NextOrdinal == 0 && len(m.Segments) != 0) || (m.NextOrdinal != 0 && len(m.Segments) == 0) {
		return errManifest
	}
	if len(m.SegmentRows) != len(m.Segments) || m.FrameStart < 8 || m.FrameStart > m.CoveredOffset || (m.CoveredOffset == 8 && m.FrameStart != 8) || (m.CoveredOffset > 8 && m.FrameStart >= m.CoveredOffset) {
		return errManifest
	}
	var total uint64
	for _, n := range m.SegmentRows {
		if n == 0 || n > ^uint64(0)-total {
			return errManifest
		}
		total += n
	}
	if total != m.NextOrdinal {
		return errManifest
	}
	seen := make(map[[16]byte]bool, len(m.Segments))
	for _, id := range m.Segments {
		if id == ([16]byte{}) || seen[id] {
			return errManifest
		}
		seen[id] = true
	}
	return nil
}
func (m Manifest) MarshalBinary() ([]byte, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	b := make([]byte, manifestHeaderSize+24*len(m.Segments)+4)
	copy(b, manifestMagic)
	binary.LittleEndian.PutUint32(b[8:], manifestVersion)
	copy(b[12:], m.Generation[:])
	binary.LittleEndian.PutUint64(b[28:], m.WALDevice)
	binary.LittleEndian.PutUint64(b[36:], m.WALInode)
	binary.LittleEndian.PutUint64(b[44:], m.CoveredOffset)
	binary.LittleEndian.PutUint64(b[52:], m.NextOrdinal)
	binary.LittleEndian.PutUint32(b[60:], uint32(len(m.Segments)))
	binary.LittleEndian.PutUint64(b[64:], m.FrameStart)
	copy(b[72:104], m.FrameHash[:])
	for i, id := range m.Segments {
		copy(b[104+i*24:], id[:])
		binary.LittleEndian.PutUint64(b[120+i*24:], m.SegmentRows[i])
	}
	binary.LittleEndian.PutUint32(b[len(b)-4:], crc32.Checksum(b[:len(b)-4], castagnoli))
	return b, nil
}
func ParseManifest(b []byte) (Manifest, error) {
	var m Manifest
	if len(b) < manifestHeaderSize+4 || len(b) > MaxManifestBytes || string(b[:8]) != manifestMagic || binary.LittleEndian.Uint32(b[8:]) != manifestVersion {
		return m, errManifest
	}
	n := binary.LittleEndian.Uint32(b[60:])
	if n > MaxManifestSegments || len(b) != manifestHeaderSize+int(n)*24+4 || crc32.Checksum(b[:len(b)-4], castagnoli) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return m, errManifest
	}
	copy(m.Generation[:], b[12:28])
	m.WALDevice = binary.LittleEndian.Uint64(b[28:])
	m.WALInode = binary.LittleEndian.Uint64(b[36:])
	m.CoveredOffset = binary.LittleEndian.Uint64(b[44:])
	m.NextOrdinal = binary.LittleEndian.Uint64(b[52:])
	m.FrameStart = binary.LittleEndian.Uint64(b[64:72])
	copy(m.FrameHash[:], b[72:104])
	m.Segments = make([][16]byte, n)
	m.SegmentRows = make([]uint64, n)
	for i := range m.Segments {
		copy(m.Segments[i][:], b[104+i*24:])
		m.SegmentRows[i] = binary.LittleEndian.Uint64(b[120+i*24:])
	}
	if err := m.validate(); err != nil {
		return Manifest{}, err
	}
	return m, nil
}
