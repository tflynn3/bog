package checkpoint

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"bog/internal/store"
)

// MaxSegments limits both manifest size and the number of simultaneously decoded
// blocks during a merge. Large checkpoints must be split by a future compactor.
const MaxSegments = 1024
const maxManifestSize = 256 << 10
const manifestName = "bog.checkpoint"

type segmentMeta struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// Manifest identifies an immutable collection of sorted blocks and the WAL prefix
// they represent. SourceSHA256 is the identity of that exact prefix, including magic.
type Manifest struct {
	Version       int           `json:"version"`
	SourceSHA256  string        `json:"source_sha256"`
	CoveredOffset int64         `json:"covered_offset"`
	NextOrdinal   uint64        `json:"next_ordinal"`
	Segments      []segmentMeta `json:"segments"`
}
type manifestEnvelope struct {
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func (l *Legacy) prefixDigest(end int64) (string, error) {
	if end < 8 || end > l.end {
		return "", errors.New("invalid covered offset")
	}
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(l.wal, 0, end)); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func randomName(prefix string) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(b[:]), nil
}
func (l *Legacy) writeNew(name string, data []byte) error {
	f, err := l.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = f.Sync()
	}
	if e := f.Close(); err == nil {
		err = e
	}
	return err
}

// Publish builds a checkpoint while the Legacy directory lock is held. Any error
// after manifest rename is an uncertain publication: close and reopen before
// making further progress. Inputs and unreferenced files are never removed.
func (l *Legacy) Publish() (*Manifest, error) {
	m := &Manifest{Version: 1, CoveredOffset: l.end, Segments: []segmentMeta{}}
	var err error
	if m.SourceSHA256, err = l.prefixDigest(l.end); err != nil {
		return nil, err
	}
	err = l.Scan(func(_, _ int64, first uint64, rows []store.Row) error {
		run := make([]BlockRow, len(rows))
		for i, r := range rows {
			run[i] = BlockRow{Row: r, Ordinal: first + uint64(i)}
		}
		sort.Slice(run, func(i, j int) bool {
			a, b := run[i], run[j]
			if a.Row.TS.Equal(b.Row.TS) {
				return a.Ordinal < b.Ordinal
			}
			return a.Row.TS.Before(b.Row.TS)
		})
		for start := 0; start < len(run); {
			end, size := start, blockHeaderSize
			for end < len(run) && size+blockRowSize+len(run[end].Row.Line) <= MaxBlockSize {
				size += blockRowSize + len(run[end].Row.Line)
				end++
			}
			if end == start {
				return errors.New("row exceeds block limit")
			}
			if len(m.Segments) >= MaxSegments {
				return errors.New("checkpoint segment limit exceeded")
			}
			b, e := EncodeBlock(run[start:end])
			if e != nil {
				return e
			}
			name, e := randomName("bog-segment-")
			if e != nil {
				return e
			}
			if e = l.writeNew(name, b); e != nil {
				return e
			}
			m.Segments = append(m.Segments, segmentMeta{Name: name, Size: int64(len(b)), SHA256: digest(b)})
			start = end
		}
		m.NextOrdinal = first + uint64(len(rows))
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Sync installed segment names before the manifest can refer to them.
	if err = l.dir.Sync(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(manifestEnvelope{Payload: payload, SHA256: digest(payload)})
	if err != nil {
		return nil, err
	}
	if len(b) > maxManifestSize {
		return nil, errors.New("manifest too large")
	}
	tmp, err := randomName("bog-manifest-")
	if err != nil {
		return nil, err
	}
	if err = l.writeNew(tmp, b); err != nil {
		return nil, err
	}
	if err = l.root.Rename(tmp, manifestName); err != nil {
		return nil, err
	}
	if err = l.dir.Sync(); err != nil {
		return nil, fmt.Errorf("uncertain checkpoint publication: %w", err)
	}
	return m, nil
}

// OpenPublished validates all referenced blocks before returning a reader. It
// also validates the committed WAL and binds the checkpoint to a frame boundary.
// Missing manifests are reported as os.ErrNotExist; corrupt checkpoints fail closed.
func (l *Legacy) OpenPublished() (*Published, error) {
	f, err := l.openFile(manifestName)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() <= 0 || st.Size() > maxManifestSize {
		return nil, errors.New("invalid manifest size")
	}
	b, err := io.ReadAll(io.LimitReader(f, maxManifestSize+1))
	if err != nil {
		return nil, err
	}
	var env manifestEnvelope
	if json.Unmarshal(b, &env) != nil || digest(env.Payload) != env.SHA256 {
		return nil, errors.New("invalid manifest checksum")
	}
	var m Manifest
	if json.Unmarshal(env.Payload, &m) != nil || m.Version != 1 || len(m.Segments) > MaxSegments {
		return nil, errors.New("invalid manifest")
	}
	hash, err := l.prefixDigest(m.CoveredOffset)
	if err != nil || hash != m.SourceSHA256 {
		return nil, errors.New("checkpoint WAL identity mismatch")
	}
	boundary := m.CoveredOffset == 8
	var ord uint64
	if err = l.Scan(func(_, next int64, first uint64, rows []store.Row) error {
		if next == m.CoveredOffset {
			boundary = true
			ord = first + uint64(len(rows))
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if !boundary || ord != m.NextOrdinal {
		return nil, errors.New("invalid checkpoint boundary")
	}
	p := &Published{manifest: m, blocks: make([][]BlockRow, 0, len(m.Segments))}
	seen := map[string]bool{}
	var count uint64
	for _, s := range m.Segments {
		if !strings.HasPrefix(s.Name, "bog-segment-") || len(s.Name) != len("bog-segment-")+32 || strings.Trim(s.Name[len("bog-segment-"):], "0123456789abcdef") != "" || seen[s.Name] || s.Size < blockHeaderSize || s.Size > MaxBlockSize {
			return nil, errors.New("invalid segment metadata")
		}
		seen[s.Name] = true
		f, e := l.openFile(s.Name)
		if e != nil {
			return nil, e
		}
		st, e := f.Stat()
		if e != nil {
			f.Close()
			return nil, e
		}
		if st.Size() != s.Size {
			f.Close()
			return nil, errors.New("invalid segment size")
		}
		b, e := io.ReadAll(io.LimitReader(f, MaxBlockSize+1))
		f.Close()
		if e != nil {
			return nil, e
		}
		if digest(b) != s.SHA256 {
			return nil, errors.New("invalid segment checksum")
		}
		rows, e := DecodeBlock(b)
		if e != nil {
			return nil, e
		}
		for i, r := range rows {
			if r.Ordinal >= m.NextOrdinal || (i > 0 && lessRow(r, rows[i-1])) {
				return nil, errors.New("invalid segment order")
			}
		}
		count += uint64(len(rows))
		p.blocks = append(p.blocks, rows)
	}
	if count != m.NextOrdinal {
		return nil, errors.New("invalid segment row count")
	}
	return p, nil
}
func lessRow(a, b BlockRow) bool {
	if a.Row.TS.Equal(b.Row.TS) {
		return a.Ordinal < b.Ordinal
	}
	return a.Row.TS.Before(b.Row.TS)
}

// Published holds fully validated blocks; its maximum resident decoded input is
// MaxSegments times MaxBlockSize plus decoded row overhead.
type Published struct {
	manifest Manifest
	blocks   [][]BlockRow
}

func (p *Published) Manifest() Manifest {
	m := p.manifest
	m.Segments = append([]segmentMeta(nil), m.Segments...)
	return m
}

// Scan visits rows in timestamp and append-ordinal order, retaining duplicates.
func (p *Published) Scan(visit func(BlockRow) error) error {
	pos := make([]int, len(p.blocks))
	for {
		best := -1
		for i, b := range p.blocks {
			if pos[i] < len(b) && (best < 0 || lessRow(b[pos[i]], p.blocks[best][pos[best]])) {
				best = i
			}
		}
		if best < 0 {
			return nil
		}
		if err := visit(p.blocks[best][pos[best]]); err != nil {
			return err
		}
		pos[best]++
	}
}
