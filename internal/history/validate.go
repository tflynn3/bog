package history

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"unicode/utf8"
)

func (g *generation) validate(ctx context.Context, m manifest, buf []byte, previous *generation) error {
	// Each v1 publisher run normally covers a contiguous append interval.
	// Reuse this bitmap to prove contiguity even when timestamps are shuffled.
	localBits := make([]byte, (len(buf)/rowHeaderSize+7)/8)
	var total uint64
	known := make(map[string]*segment)
	if previous != nil {
		for i := range previous.segments {
			s := &previous.segments[i]
			known[s.name] = s
		}
	}
	for i := range g.segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		s := &g.segments[i]
		b := buf[:int(m.Segments[i].Size)]
		if old := known[s.name]; old != nil && old.digest == s.digest && old.stat.Size() == int64(len(b)) {
			f, info, err := openRegular(g.dir, s.name)
			if err != nil {
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
			if !sameSegment(old.stat, info) {
				return errors.Join(ErrIntegrity, fmt.Errorf("previous segment %s changed", s.name))
			}
			*s = *old
			total += old.count
			continue
		}
		if err := s.readVerified(g.dir, b); err != nil {
			return err
		}
		count, err := s.index(b, m.NextOrdinal)
		if err != nil {
			return err
		}
		s.count = uint64(count)
		total += s.count
		if s.last-s.first+1 == uint64(count) {
			bits := localBits[:(count+7)/8]
			clear(bits)
			for p := headerSize; p < len(b); p = nextRow(b, p) {
				ord := binary.LittleEndian.Uint64(b[p+12 : p+20])
				if !markOrdinal(bits, ord-s.first) {
					return corrupt(s.name, "duplicate append ordinal")
				}
			}
			s.dense = true
		}
	}
	if total != m.NextOrdinal {
		return fmt.Errorf("%w: checkpoint row count differs from manifest", ErrIntegrity)
	}
	return g.validateOrdinals(ctx, buf, total)
}

func (s *segment) index(b []byte, nextOrdinal uint64) (int, error) {
	if binary.LittleEndian.Uint32(b[:4]) != version ||
		crc32.Checksum(b[:40], crcTable) != binary.LittleEndian.Uint32(b[40:44]) {
		return 0, corrupt(s.name, "block version or header checksum")
	}
	count := uint64(binary.LittleEndian.Uint32(b[4:8]))
	payload := b[headerSize:]
	if count == 0 || count > uint64(len(payload)/rowHeaderSize) ||
		uint64(binary.LittleEndian.Uint32(b[8:12])) != uint64(len(payload)) ||
		crc32.Checksum(payload, crcTable) != binary.LittleEndian.Uint32(b[36:40]) {
		return 0, corrupt(s.name, "block length, row count or payload checksum")
	}
	lo, loOK := getTime(b[12:24])
	hi, hiOK := getTime(b[24:36])
	if !loOK || !hiOK || hi.Before(lo) {
		return 0, corrupt(s.name, "invalid block time bounds")
	}

	step := (int(count) + maxSeekPoints - 1) / maxSeekPoints
	s.points = make([]seekPoint, 0, (int(count)+step-1)/step)
	s.first = nextOrdinal
	p := int64(headerSize)
	var prev Position
	for i := 0; i < int(count); i++ {
		key, n, ok := parseRow(b[p:], p, int64(len(b)))
		if !ok || key.Ordinal >= nextOrdinal || (i != 0 && !prev.less(key)) ||
			!utf8.Valid(b[p+rowHeaderSize:p+rowHeaderSize+n]) {
			return 0, corrupt(s.name, "invalid row data or row order")
		}
		if i%step == 0 {
			s.points = append(s.points, seekPoint{key: key, offset: p})
		}
		s.first = min(s.first, key.Ordinal)
		s.last = max(s.last, key.Ordinal)
		prev = key
		p += rowHeaderSize + n
	}
	if p != int64(len(b)) || !s.points[0].key.Time.Equal(lo) || !prev.Time.Equal(hi) {
		return 0, corrupt(s.name, "block rows disagree with length or time bounds")
	}
	s.lo, s.hi = lo, hi
	return int(count), nil
}

// The block's row lengths have already been validated.
func nextRow(b []byte, p int) int {
	return p + rowHeaderSize + int(binary.LittleEndian.Uint32(b[p+20:p+24]))
}

func markOrdinal(bits []byte, ordinal uint64) bool {
	i, mask := ordinal/8, byte(1<<(ordinal%8))
	if bits[i]&mask != 0 {
		return false
	}
	bits[i] |= mask
	return true
}

const ordinalsPerPage = 1 << 24 // 2 MiB bitmap, independent of history length.

func (g *generation) validateOrdinals(ctx context.Context, buf []byte, total uint64) error {
	byOrdinal := make([]*segment, len(g.segments))
	for i := range g.segments {
		byOrdinal[i] = &g.segments[i]
	}
	sort.Slice(byOrdinal, func(i, j int) bool { return byOrdinal[i].first < byOrdinal[j].first })
	var next uint64
	for _, s := range byOrdinal {
		if !s.dense || s.first != next {
			return g.validateOrdinalPages(ctx, buf, total, byOrdinal)
		}
		next = s.last + 1
	}
	return nil
}

// v1 also permits runs with noncontiguous append ordinals. Validate these
// exactly using bounded ordinal windows and reread only sparse runs intersecting
// each window. Since the overall count is known, uniqueness implies coverage.
func (g *generation) validateOrdinalPages(ctx context.Context, buf []byte, total uint64, ordered []*segment) error {
	bits := make([]byte, (min(total, ordinalsPerPage)+7)/8)
	for lo := uint64(0); lo < total; lo += ordinalsPerPage {
		clear(bits)
		hi := min(total, lo+ordinalsPerPage)
		for _, s := range ordered {
			if err := ctx.Err(); err != nil {
				return err
			}
			if s.first >= hi {
				break
			}
			if s.last < lo {
				continue
			}
			if s.dense {
				for ord := max(lo, s.first); ord < min(hi, s.last+1); ord++ {
					if !markOrdinal(bits, ord-lo) {
						return corrupt(s.name, "duplicate append ordinal")
					}
				}
				continue
			}
			b := buf[:int(s.stat.Size())]
			if err := s.readVerified(g.dir, b); err != nil {
				return err
			}
			for p := headerSize; p < len(b); p = nextRow(b, p) {
				ord := binary.LittleEndian.Uint64(b[p+12 : p+20])
				if lo <= ord && ord < hi && !markOrdinal(bits, ord-lo) {
					return corrupt(s.name, "duplicate append ordinal")
				}
			}
		}
	}
	return nil
}
