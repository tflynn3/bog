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

	"bog/internal/checkpoint"
	"bog/internal/store"
)

const (
	maxManifestSize = 256 << 10
	segmentPrefix   = "bog-segment-"
	blockHeaderSize = 44
	blockRowSize    = 24
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type manifestV1 struct {
	Version       int         `json:"version"`
	SourceSHA256  string      `json:"source_sha256"`
	CoveredOffset int64       `json:"covered_offset"`
	NextOrdinal   uint64      `json:"next_ordinal"`
	Segments      []segmentV1 `json:"segments"`
}

type segmentV1 struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type manifestEnvelope struct {
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

func checksum(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func decodeManifest(b []byte) (manifestV1, error) {
	var env manifestEnvelope
	var m manifestV1
	if err := json.Unmarshal(b, &env); err != nil || checksum(env.Payload) != env.SHA256 {
		return m, errors.New("invalid checkpoint manifest checksum")
	}
	if err := json.Unmarshal(env.Payload, &m); err != nil || m.Version != 1 ||
		m.CoveredOffset < int64(len(walMagic)) || len(m.Segments) > checkpoint.MaxSegments {
		return m, errors.New("invalid checkpoint manifest metadata")
	}
	seen := make(map[string]bool, len(m.Segments))
	var maxRows uint64
	for _, s := range m.Segments {
		id, ok := strings.CutPrefix(s.Name, segmentPrefix)
		if !ok || len(id) != 32 || strings.Trim(id, "0123456789abcdef") != "" ||
			seen[s.Name] || s.Size < blockHeaderSize || s.Size > checkpoint.MaxBlockSize {
			return m, errors.New("invalid checkpoint segment metadata")
		}
		seen[s.Name] = true
		maxRows += uint64((s.Size - blockHeaderSize) / blockRowSize)
	}
	if m.NextOrdinal > maxRows {
		return m, errors.New("invalid checkpoint ordinal count")
	}
	return m, nil
}

func copySegments(ctx context.Context, src, dst *directory, m manifestV1) error {
	// The manifest's capped segment count and per-block row limit bound this
	// bitmap to about 22 MiB, independent of the WAL length.
	ordinals := make([]byte, (m.NextOrdinal+7)/8)
	var count uint64
	for _, s := range m.Segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := src.read(s.Name, int(s.Size))
		if err != nil {
			return err
		}
		if int64(len(b)) != s.Size || checksum(b) != s.SHA256 {
			return fmt.Errorf("checkpoint segment %s size or checksum mismatch", s.Name)
		}
		rows, err := checkpoint.DecodeBlock(b)
		if err != nil {
			return fmt.Errorf("decode %s: %w", s.Name, err)
		}
		for i, row := range rows {
			if row.Ordinal >= m.NextOrdinal {
				return fmt.Errorf("%s: checkpoint ordinal out of bounds", s.Name)
			}
			if i > 0 {
				prev := rows[i-1]
				if row.Row.TS.Before(prev.Row.TS) || (row.Row.TS.Equal(prev.Row.TS) && row.Ordinal <= prev.Ordinal) {
					return fmt.Errorf("%s: invalid checkpoint row order", s.Name)
				}
			}
			mask := byte(1 << (row.Ordinal % 8))
			if ordinals[row.Ordinal/8]&mask != 0 {
				return fmt.Errorf("%s: duplicate checkpoint ordinal", s.Name)
			}
			ordinals[row.Ordinal/8] |= mask
		}
		count += uint64(len(rows))
		if dst != nil {
			if err := dst.write(s.Name, b); err != nil {
				return err
			}
		}
	}
	if count != m.NextOrdinal {
		return errors.New("checkpoint row count disagrees with manifest")
	}
	return nil
}

// scanWAL verifies the exact covered prefix, checks every committed
// frame and writes only the committed bytes. Memory is bounded by one maximum
// payload and its decoded rows, including for shuffled or legacy input.
func scanWAL(ctx context.Context, input io.Reader, dst io.Writer, end int64, m manifestV1) (uint64, [sha256.Size]byte, error) {
	var sum [sha256.Size]byte
	h := sha256.New()
	out := io.MultiWriter(h, dst)
	var magic [len(walMagic)]byte
	if _, err := io.ReadFull(input, magic[:]); err != nil {
		return 0, sum, fmt.Errorf("read WAL magic: %w", err)
	}
	if string(magic[:]) != walMagic {
		return 0, sum, errors.New("invalid WAL magic")
	}
	if _, err := out.Write(magic[:]); err != nil {
		return 0, sum, err
	}
	pos := int64(len(magic))
	var ordinal uint64
	var payload []byte
	var head [16]byte
	boundary := m.Version == 0
	for {
		if err := ctx.Err(); err != nil {
			return 0, sum, err
		}
		if m.Version == 1 && pos == m.CoveredOffset {
			if ordinal != m.NextOrdinal || hex.EncodeToString(h.Sum(nil)) != m.SourceSHA256 {
				return 0, sum, errors.New("checkpoint WAL identity or ordinal mismatch")
			}
			boundary = true
		}
		if pos == end {
			break
		}
		if end-pos < int64(len(head)) {
			return 0, sum, fmt.Errorf("truncated committed WAL header at %d", pos)
		}
		if _, err := io.ReadFull(input, head[:]); err != nil {
			return 0, sum, fmt.Errorf("read WAL header at %d: %w", pos, err)
		}
		n := binary.LittleEndian.Uint32(head[:4])
		if n == 0 || n > store.MaxBatch || int64(n) > end-pos-int64(len(head)) ||
			binary.LittleEndian.Uint32(head[4:8]) != ^n || crc32.Checksum(head[:12], crcTable) != binary.LittleEndian.Uint32(head[12:]) {
			return 0, sum, fmt.Errorf("invalid committed WAL header at %d", pos)
		}
		if payload == nil {
			payload = make([]byte, store.MaxBatch)
		}
		body := payload[:int(n)]
		if _, err := io.ReadFull(input, body); err != nil {
			return 0, sum, fmt.Errorf("read WAL payload at %d: %w", pos, err)
		}
		if crc32.Checksum(body, crcTable) != binary.LittleEndian.Uint32(head[8:12]) {
			return 0, sum, fmt.Errorf("invalid committed WAL checksum at %d", pos)
		}
		rows, err := store.Decode(body)
		if err != nil {
			return 0, sum, fmt.Errorf("decode committed WAL payload at %d: %w", pos, err)
		}
		if uint64(len(rows)) > ^uint64(0)-ordinal {
			return 0, sum, errors.New("WAL append ordinal overflow")
		}
		ordinal += uint64(len(rows))
		if _, err := out.Write(head[:]); err != nil {
			return 0, sum, err
		}
		if _, err := out.Write(body); err != nil {
			return 0, sum, err
		}
		pos += int64(len(head)) + int64(n)
	}
	if !boundary {
		return 0, sum, errors.New("checkpoint coverage is not a WAL frame boundary")
	}
	copy(sum[:], h.Sum(nil))
	return ordinal, sum, nil
}
