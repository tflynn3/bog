package checkpoint

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
)

// Manifest identifies one authoritative set of immutable BOGQSG01 segments.
// Generation and segment IDs are nonzero opaque 128-bit identities.
type Manifest struct {
	Generation    [16]byte
	CoveredOffset uint64
	NextOrdinal   uint64
	Segments      [][16]byte
}

const (
	manifestMagic        = "BOGMAN01"
	manifestVersion      = 1
	manifestHeaderSize   = 48
	manifestChecksumSize = 4
	// MaxManifestSegments bounds both manifest memory and its on-disk size.
	MaxManifestSegments = 4096
	MaxManifestBytes    = manifestHeaderSize + 16*MaxManifestSegments + manifestChecksumSize
)

var errManifest = errors.New("invalid checkpoint manifest")

func (m Manifest) validate() error {
	if m.Generation == ([16]byte{}) || m.CoveredOffset < 8 || m.CoveredOffset > math.MaxInt64 || len(m.Segments) > MaxManifestSegments {
		return errManifest
	}
	if m.CoveredOffset == 8 && (m.NextOrdinal != 0 || len(m.Segments) != 0) {
		return errManifest
	}
	seen := make(map[[16]byte]struct{}, len(m.Segments))
	for _, id := range m.Segments {
		if id == ([16]byte{}) {
			return errManifest
		}
		if _, ok := seen[id]; ok {
			return errManifest
		}
		seen[id] = struct{}{}
	}
	return nil
}

// MarshalBinary encodes a complete manifest, including its CRC32C.
func (m Manifest) MarshalBinary() ([]byte, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	b := make([]byte, manifestHeaderSize+16*len(m.Segments)+manifestChecksumSize)
	copy(b, manifestMagic)
	binary.LittleEndian.PutUint32(b[8:12], manifestVersion)
	copy(b[12:28], m.Generation[:])
	binary.LittleEndian.PutUint64(b[28:36], m.CoveredOffset)
	binary.LittleEndian.PutUint64(b[36:44], m.NextOrdinal)
	binary.LittleEndian.PutUint32(b[44:48], uint32(len(m.Segments)))
	for i, id := range m.Segments {
		copy(b[manifestHeaderSize+i*16:], id[:])
	}
	binary.LittleEndian.PutUint32(b[len(b)-4:], crc32.Checksum(b[:len(b)-4], castagnoli))
	return b, nil
}

// ParseManifest validates the complete record before allocating its entries.
// Callers reading a file must check its size against MaxManifestBytes first.
func ParseManifest(b []byte) (Manifest, error) {
	var m Manifest
	if len(b) < manifestHeaderSize+manifestChecksumSize || len(b) > MaxManifestBytes || string(b[:8]) != manifestMagic || binary.LittleEndian.Uint32(b[8:12]) != manifestVersion {
		return m, errManifest
	}
	n := binary.LittleEndian.Uint32(b[44:48])
	if n > MaxManifestSegments || len(b) != manifestHeaderSize+int(n)*16+manifestChecksumSize || crc32.Checksum(b[:len(b)-4], castagnoli) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return m, errManifest
	}
	copy(m.Generation[:], b[12:28])
	m.CoveredOffset = binary.LittleEndian.Uint64(b[28:36])
	m.NextOrdinal = binary.LittleEndian.Uint64(b[36:44])
	m.Segments = make([][16]byte, int(n))
	for i := range m.Segments {
		copy(m.Segments[i][:], b[manifestHeaderSize+i*16:])
	}
	if err := m.validate(); err != nil {
		return Manifest{}, err
	}
	return m, nil
}
