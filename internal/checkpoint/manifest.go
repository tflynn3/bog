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
}

const (
	manifestMagic        = "BOGMAN02"
	manifestVersion      = 2
	manifestHeaderSize   = 64
	manifestChecksumSize = 4
	MaxManifestSegments  = 4096
	MaxManifestBytes     = manifestHeaderSize + 16*MaxManifestSegments + manifestChecksumSize
)

var errManifest = errors.New("invalid checkpoint manifest")

func (m Manifest) validate() error {
	if m.Generation == ([16]byte{}) || m.WALInode == 0 || m.CoveredOffset < 8 || m.CoveredOffset > math.MaxInt64 || len(m.Segments) > MaxManifestSegments || (m.CoveredOffset == 8 && (m.NextOrdinal != 0 || len(m.Segments) != 0)) || (m.NextOrdinal == 0 && len(m.Segments) != 0) || (m.NextOrdinal != 0 && len(m.Segments) == 0) {
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
	b := make([]byte, manifestHeaderSize+16*len(m.Segments)+4)
	copy(b, manifestMagic)
	binary.LittleEndian.PutUint32(b[8:], manifestVersion)
	copy(b[12:], m.Generation[:])
	binary.LittleEndian.PutUint64(b[28:], m.WALDevice)
	binary.LittleEndian.PutUint64(b[36:], m.WALInode)
	binary.LittleEndian.PutUint64(b[44:], m.CoveredOffset)
	binary.LittleEndian.PutUint64(b[52:], m.NextOrdinal)
	binary.LittleEndian.PutUint32(b[60:], uint32(len(m.Segments)))
	for i, id := range m.Segments {
		copy(b[64+i*16:], id[:])
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
	if n > MaxManifestSegments || len(b) != manifestHeaderSize+int(n)*16+4 || crc32.Checksum(b[:len(b)-4], castagnoli) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return m, errManifest
	}
	copy(m.Generation[:], b[12:28])
	m.WALDevice = binary.LittleEndian.Uint64(b[28:])
	m.WALInode = binary.LittleEndian.Uint64(b[36:])
	m.CoveredOffset = binary.LittleEndian.Uint64(b[44:])
	m.NextOrdinal = binary.LittleEndian.Uint64(b[52:])
	m.Segments = make([][16]byte, n)
	for i := range m.Segments {
		copy(m.Segments[i][:], b[64+i*16:])
	}
	if err := m.validate(); err != nil {
		return Manifest{}, err
	}
	return m, nil
}
