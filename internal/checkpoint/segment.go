package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"bog/internal/store"
)

// MaxSegments limits both manifest size and the number of simultaneously decoded
// runs that a v1 reader may need to merge.
const MaxSegments = 1024
const maxManifestSize = 256 << 10
const manifestName = "bog.checkpoint"

var ErrIntegrity = errors.New("checkpoint integrity error")
var ErrPublicationUncertain = errors.New("checkpoint publication uncertain; reopen required")

const segmentPrefix = "bog-segment-"

func validSegmentName(name string) bool {
	return strings.HasPrefix(name, segmentPrefix) && len(name) == len(segmentPrefix)+32 &&
		strings.Trim(name[len(segmentPrefix):], "0123456789abcdef") == ""
}

// Segment describes an immutable block referenced by a Manifest.
type Segment struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// Manifest identifies immutable sorted blocks and their logical WAL boundary.
// SourceSHA256 binds the exact full prefix, including magic, in version 1.
// Advance binds the consumed suffix range in version 2.
type Manifest struct {
	Version       int       `json:"version"`
	SourceSHA256  string    `json:"source_sha256,omitempty"`
	CoveredOffset int64     `json:"covered_offset"`
	NextOrdinal   uint64    `json:"next_ordinal"`
	Segments      []Segment `json:"segments"`
	Advance       *Advance  `json:"advance,omitempty"`
}
type segmentMeta = Segment

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
	if _, err := io.CopyN(h, io.NewSectionReader(l.wal, 0, end), end); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// OpenPublished validates all referenced blocks before returning a reader. It
// also validates the committed WAL and binds the checkpoint to a frame boundary.
// Missing manifests are reported as os.ErrNotExist; corrupt checkpoints fail closed.
func (l *Legacy) OpenPublished() (*Published, error) {
	if err := l.begin(); err != nil {
		return nil, err
	}
	defer l.active.Done()
	return l.openPublished()
}

// openPublished requires an accepted operation.
func (l *Legacy) openPublished() (p *Published, err error) {
	l.publicationMu.Lock()
	defer l.publicationMu.Unlock()
	if l.uncertain != nil {
		return nil, l.uncertain
	}
	lock, err := LockPublication(l.dir, true)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := lock.Close(); closeErr != nil {
			p = nil
			err = errors.Join(err, closeErr)
		}
	}()
	return l.resolvePublication(true)
}

// readPublished requires the publication lock. Recovery can validate and
// discard one decoded block at a time when no checkpoint reader is requested.
func (l *Legacy) readPublished(keepRows bool) (p *Published, err error) {
	f, err := l.openFile(manifestName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			p = nil
			err = errors.Join(err, closeErr)
		}
	}()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() <= 0 || st.Size() > maxManifestSize {
		return nil, errors.New("invalid manifest size")
	}
	b, err := io.ReadAll(io.LimitReader(f, st.Size()+1))
	if err != nil {
		return nil, err
	}
	if len(b) != int(st.Size()) {
		return nil, errors.New("invalid manifest size")
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
	if err != nil {
		return nil, err
	}
	if hash != m.SourceSHA256 {
		return nil, errors.New("checkpoint WAL identity mismatch")
	}
	boundary := m.CoveredOffset == 8
	var ord uint64
	if err = l.scanFrom(8, 0, func(_, next int64, first uint64, rows []store.Row) error {
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
	p = &Published{manifest: m}
	if keepRows {
		p.blocks = make([][]BlockRow, 0, len(m.Segments))
	}
	err = validateManifestBlocks(context.Background(), m, func(name string) (io.ReadCloser, error) { return l.openFile(name) }, func(rows []BlockRow) {
		if keepRows {
			p.blocks = append(p.blocks, rows)
		}
	})
	if err != nil {
		return nil, err
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

// ScanUncovered visits committed frames after the published boundary in append
// order. OpenPublished must have succeeded on this locked Legacy first. Each
// callback receives a complete frame and its original append ordinal.
// The callback follows the same lifecycle contract as a Scan visitor.
func (l *Legacy) ScanUncovered(p *Published, visit func(firstOrdinal uint64, rows []store.Row) error) error {
	if err := l.begin(); err != nil {
		return err
	}
	defer l.active.Done()
	if p == nil {
		return errors.New("nil published checkpoint")
	}
	// Revalidate the current authoritative manifest before replaying any suffix.
	current, err := l.openPublished()
	if err != nil {
		return err
	}
	if current.manifest.SourceSHA256 != p.manifest.SourceSHA256 || current.manifest.CoveredOffset != p.manifest.CoveredOffset || current.manifest.NextOrdinal != p.manifest.NextOrdinal {
		return errors.New("checkpoint changed")
	}
	return l.scanFrom(current.manifest.CoveredOffset, current.manifest.NextOrdinal, func(_, _ int64, first uint64, rows []store.Row) error {
		return visit(first, rows)
	})
}
