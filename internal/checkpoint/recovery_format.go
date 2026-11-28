package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// RecoveryPoint fixes one generation and its committed WAL suffix. Offsets
// include the original eight-byte WAL magic, and ordinals are first-free.
type RecoveryPoint struct {
	Generation       [32]byte
	CoveredOffset    int64
	NextOrdinal      uint64
	CommittedEnd     int64
	CommittedOrdinal uint64
	SuffixSHA256     [32]byte
}

// RecoveryView borrows an operation pin held by the caller through return.
// Successful opens transfer independent readers to the consumer. OpenSuffix
// starts at CoveredOffset and ends exactly at CommittedEnd.
type RecoveryView interface {
	Point() RecoveryPoint
	OpenManifest() (io.ReadCloser, error)
	OpenSegment(name string) (io.ReadCloser, error)
	OpenSuffix() (io.ReadCloser, error)
}

// Advance records the exact predecessor boundary and consumed WAL frames.
type Advance struct {
	Generation     string `json:"generation"`
	CoveredOffset  int64  `json:"covered_offset"`
	NextOrdinal    uint64 `json:"next_ordinal"`
	ConsumedSHA256 string `json:"consumed_sha256"`
}

func lowercaseDigest(sum string) bool {
	return len(sum) == 64 && strings.Trim(sum, "0123456789abcdef") == ""
}

// DecodeManifest validates a v1 or v2 envelope and returns the identity of
// the exact payload. Segment bytes and ordinal coverage are checked separately.
func DecodeManifest(b []byte) (*Manifest, [32]byte, error) {
	var env manifestEnvelope
	var m Manifest
	if len(b) > maxManifestSize || json.Unmarshal(b, &env) != nil ||
		json.Unmarshal(env.Payload, &m) != nil {
		return nil, [32]byte{}, fmt.Errorf("%w: invalid checkpoint envelope", ErrIntegrity)
	}
	id := sha256.Sum256(env.Payload)
	if hex.EncodeToString(id[:]) != env.SHA256 {
		return nil, [32]byte{}, fmt.Errorf("%w: checkpoint envelope digest mismatch", ErrIntegrity)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(env.Payload, &fields); err != nil {
		return nil, [32]byte{}, fmt.Errorf("%w: invalid checkpoint payload: %w", ErrIntegrity, err)
	}
	_, hasSource := fields["source_sha256"]
	_, hasAdvance := fields["advance"]
	switch m.Version {
	case 1:
		if !hasSource || hasAdvance || !validPublicationBoundary(m.CoveredOffset, m.NextOrdinal, m.SourceSHA256) ||
			(m.CoveredOffset == 8 && m.SourceSHA256 != digest([]byte("BOGWAL01"))) {
			return nil, [32]byte{}, fmt.Errorf("%w: invalid v1 checkpoint boundary or digest", ErrIntegrity)
		}
	case 2:
		a := m.Advance
		if hasSource || m.SourceSHA256 != "" || m.Segments == nil || a == nil ||
			!validPublicationBoundary(a.CoveredOffset, a.NextOrdinal, a.Generation) ||
			!lowercaseDigest(a.ConsumedSHA256) || m.CoveredOffset <= a.CoveredOffset ||
			m.CoveredOffset-a.CoveredOffset < 17 || m.NextOrdinal <= a.NextOrdinal {
			return nil, [32]byte{}, fmt.Errorf("%w: invalid v2 checkpoint transition", ErrIntegrity)
		}
		var predecessor map[string]json.RawMessage
		if err := json.Unmarshal(fields["advance"], &predecessor); err != nil {
			return nil, [32]byte{}, fmt.Errorf("%w: invalid predecessor: %w", ErrIntegrity, err)
		}
		if !present(predecessor["next_ordinal"]) || !present(fields["next_ordinal"]) {
			return nil, [32]byte{}, fmt.Errorf("%w: missing checkpoint ordinal", ErrIntegrity)
		}
	default:
		return nil, [32]byte{}, fmt.Errorf("%w: unsupported checkpoint version %d", ErrIntegrity, m.Version)
	}
	if err := validateSegmentList(m); err != nil {
		return nil, [32]byte{}, err
	}
	return &m, id, nil
}

func present(raw json.RawMessage) bool {
	return len(raw) != 0 && string(raw) != "null"
}

func validateSegmentList(m Manifest) error {
	if len(m.Segments) > MaxSegments {
		return fmt.Errorf("%w: too many checkpoint segments", ErrIntegrity)
	}
	seen := make(map[string]bool, len(m.Segments))
	var maxRows uint64
	for _, s := range m.Segments {
		if !validSegmentName(s.Name) || seen[s.Name] || !lowercaseDigest(s.SHA256) ||
			s.Size < blockHeaderSize+blockRowSize || s.Size > MaxBlockSize {
			return fmt.Errorf("%w: invalid segment metadata for %q", ErrIntegrity, s.Name)
		}
		seen[s.Name] = true
		maxRows += uint64((s.Size - blockHeaderSize) / blockRowSize)
	}
	if m.NextOrdinal < uint64(len(m.Segments)) || m.NextOrdinal > maxRows {
		return fmt.Errorf("%w: impossible checkpoint row count", ErrIntegrity)
	}
	return nil
}

func validateManifestBlocks(ctx context.Context, m Manifest, open func(string) (io.ReadCloser, error), keep func([]BlockRow)) error {
	if err := validateSegmentList(m); err != nil {
		return err
	}
	var buf []byte
	ordinals := make([]byte, (m.NextOrdinal+7)/8)
	var count uint64
	for _, s := range m.Segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		r, err := open(s.Name)
		if err != nil {
			return fmt.Errorf("%w: open segment %s: %w", ErrIntegrity, s.Name, err)
		}
		if int64(cap(buf)) < s.Size {
			buf = make([]byte, s.Size)
		}
		b := buf[:s.Size]
		input := recoveryReader{ctx, r}
		_, err = io.ReadFull(input, b)
		if err == nil {
			err = requireEnd(input)
		}
		if err := errors.Join(err, r.Close()); err != nil {
			return fmt.Errorf("read segment %s: %w", s.Name, err)
		}
		if digest(b) != s.SHA256 {
			return fmt.Errorf("%w: segment digest mismatch for %s", ErrIntegrity, s.Name)
		}
		n, err := validateBlock(b)
		if err != nil {
			return fmt.Errorf("%w: decode segment %s: %w", ErrIntegrity, s.Name, err)
		}
		pos := blockHeaderSize
		var previous BlockRow
		for i := uint32(0); i < n; i++ {
			header := b[pos : pos+blockRowSize]
			row := BlockRow{Ordinal: binary.LittleEndian.Uint64(header[12:20])}
			row.Row.TS, _ = getBlockTime(header)
			if row.Ordinal >= m.NextOrdinal || (i > 0 && lessRow(row, previous)) {
				return fmt.Errorf("%w: invalid segment row order or ordinal in %s", ErrIntegrity, s.Name)
			}
			mask := byte(1 << (row.Ordinal % 8))
			if ordinals[row.Ordinal/8]&mask != 0 {
				return fmt.Errorf("%w: duplicate segment ordinal %d", ErrIntegrity, row.Ordinal)
			}
			ordinals[row.Ordinal/8] |= mask
			pos += blockRowSize + int(binary.LittleEndian.Uint32(header[20:24]))
			previous = row
		}
		count += uint64(n)
		if keep != nil {
			rows, err := DecodeBlock(b)
			if err != nil {
				return err
			}
			keep(rows)
		}
	}
	if count != m.NextOrdinal {
		return fmt.Errorf("%w: incomplete checkpoint ordinal coverage", ErrIntegrity)
	}
	return ctx.Err()
}

type recoveryReader struct {
	ctx context.Context
	io.Reader
}

func (r recoveryReader) Read(b []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.Reader.Read(b)
	if len(b) > 0 && n == 0 && err == nil {
		return 0, io.ErrNoProgress
	}
	return n, err
}

func requireEnd(r io.Reader) error {
	var extra [1]byte
	n, err := r.Read(extra[:])
	if n == 0 && err == io.EOF {
		return nil
	}
	if n == 0 && err != nil {
		return err
	}
	if n == 0 {
		return io.ErrNoProgress
	}
	return fmt.Errorf("%w: unexpected trailing bytes", ErrIntegrity)
}
