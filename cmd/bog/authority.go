package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"strings"
	"syscall"

	"bog/internal/backup"
	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
	"bog/internal/wal"
)

const (
	authorityName     = "bog.authority"
	canonicalManifest = "bog.checkpoint"
)

type authority struct {
	Version          int    `json:"version"`
	Generation       string `json:"generation"`
	CoveredOffset    int64  `json:"covered_offset"`
	NextOrdinal      uint64 `json:"next_ordinal"`
	CommittedEnd     int64  `json:"committed_end"`
	CommittedOrdinal uint64 `json:"committed_ordinal"`
	SuffixSHA256     string `json:"suffix_sha256"`
	Tail             string `json:"tail"`
	TailBase         int64  `json:"tail_base"`
	Device           uint64 `json:"tail_device"`
	Inode            uint64 `json:"tail_inode"`
	Sequence         uint64 `json:"sequence"`
}

type envelope struct {
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

func hexSum(sum [32]byte) string { return hex.EncodeToString(sum[:]) }

func (r *retained) selectAuthority(tail *tailFile, p checkpoint.RecoveryPoint, sequence uint64) (string, error) {
	info, err := tail.Stat()
	if err != nil {
		return "marker_preparation", err
	}
	st := info.Sys().(*syscall.Stat_t)
	a := authority{2, hexSum(p.Generation), p.CoveredOffset, p.NextOrdinal, p.CommittedEnd, p.CommittedOrdinal,
		hexSum(p.SuffixSHA256), tail.Name(), tail.base, uint64(st.Dev), st.Ino, sequence}
	b, err := json.Marshal(a)
	if err != nil {
		return "marker_preparation", err
	}
	record, err := json.Marshal(envelope{b, hexSum(sha256.Sum256(b))})
	if err != nil {
		return "marker_preparation", err
	}
	return r.owner.Select(authorityName, record)
}

func decodeAuthority(data []byte) (a authority, p checkpoint.RecoveryPoint, err error) {
	var e envelope
	if err := jsonv2.Unmarshal(data, &e); err != nil || hexSum(sha256.Sum256(e.Payload)) != e.SHA256 {
		return a, p, errors.Join(errors.New("invalid selector envelope or checksum"), err)
	}
	var fields map[string]jsontext.Value
	if err := jsonv2.Unmarshal(e.Payload, &fields); err != nil {
		return a, p, err
	}
	for _, name := range []string{"version", "generation", "covered_offset", "next_ordinal", "committed_end",
		"committed_ordinal", "suffix_sha256", "tail", "tail_base", "tail_device", "tail_inode"} {
		if len(fields[name]) == 0 || string(fields[name]) == "null" {
			return a, p, fmt.Errorf("missing selector field %q", name)
		}
	}
	if err := jsonv2.Unmarshal(e.Payload, &a); err != nil {
		return a, p, err
	}
	p = checkpoint.RecoveryPoint{CoveredOffset: a.CoveredOffset, NextOrdinal: a.NextOrdinal,
		CommittedEnd: a.CommittedEnd, CommittedOrdinal: a.CommittedOrdinal}
	for _, value := range []struct {
		src string
		dst *[32]byte
	}{{a.Generation, &p.Generation}, {a.SuffixSHA256, &p.SuffixSHA256}} {
		if len(value.src) != 64 || strings.Trim(value.src, "0123456789abcdef") != "" {
			return a, p, errors.New("invalid selector digest")
		}
		if _, err := hex.Decode(value.dst[:], []byte(value.src)); err != nil {
			return a, p, err
		}
	}
	suffix, named := strings.CutPrefix(a.Tail, "bog.wal.tail-")
	if a.Version != 2 || a.CoveredOffset < 8 || a.TailBase < 0 || a.TailBase > a.CoveredOffset ||
		a.CommittedEnd < a.CoveredOffset || a.CommittedOrdinal < a.NextOrdinal ||
		(a.CommittedEnd == a.CoveredOffset) != (a.CommittedOrdinal == a.NextOrdinal) ||
		!named || suffix == "" || strings.ContainsAny(suffix, "/\\\x00") {
		return a, p, errors.New("invalid selector version, tail name or bounds")
	}
	return a, p, nil
}

func (r *retained) readFile(name string, max int64) ([]byte, error) {
	f, err := r.owner.OpenRegular(name)
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(io.LimitReader(f, max+1))
	if int64(len(b)) > max {
		err = errors.New("file exceeds format size limit")
	}
	return b, errors.Join(err, f.Close())
}

func (r *retained) readManifest(name string) ([]byte, *checkpoint.Manifest, [32]byte, error) {
	raw, err := r.readFile(name, 256<<10)
	if err != nil {
		return nil, nil, [32]byte{}, err
	}
	m, id, err := checkpoint.DecodeManifest(raw)
	return raw, m, id, err
}

type recoveryView struct {
	owner    *wal.Owner
	point    checkpoint.RecoveryPoint
	manifest string
	segments map[string]struct{}
	tail     *tailFile
}

func (v recoveryView) Point() checkpoint.RecoveryPoint { return v.point }
func (v recoveryView) OpenManifest() (io.ReadCloser, error) {
	return v.owner.OpenRegular(v.manifest)
}
func (v recoveryView) OpenSegment(name string) (io.ReadCloser, error) {
	if _, ok := v.segments[name]; !ok {
		return nil, os.ErrNotExist
	}
	return v.owner.OpenRegular(name)
}
func (v recoveryView) OpenSuffix() (io.ReadCloser, error) {
	return io.NopCloser(v.tail.section(v.point.CoveredOffset, v.point.CommittedEnd)), nil
}

func (t *tailFile) section(start, end int64) *io.SectionReader {
	return io.NewSectionReader(t.File, start-t.base, end-start)
}

func (r *retained) recoveryView(p checkpoint.RecoveryPoint, name string, tail *tailFile) (recoveryView, error) {
	_, m, id, err := r.readManifest(name)
	if err != nil {
		return recoveryView{}, err
	}
	if id != p.Generation || m.CoveredOffset != p.CoveredOffset || m.NextOrdinal != p.NextOrdinal {
		return recoveryView{}, errors.New("selected manifest digest or boundary differs")
	}
	segments := make(map[string]struct{}, len(m.Segments))
	for _, s := range m.Segments {
		segments[s.Name] = struct{}{}
	}
	return recoveryView{r.owner, p, name, segments, tail}, nil
}

func (r *retained) recover(ctx context.Context) error {
	data, err := r.readFile(authorityName, 4<<10)
	switch {
	case err == nil:
		return r.recoverAuthority(ctx, data)
	case !errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("open writable authority: %w", err)
	}
	meta, err := r.hasEntry("bog.backup")
	if err != nil {
		return err
	}
	tail, err := r.hasEntry("bog.wal.tail")
	if err != nil {
		return err
	}
	if meta || tail {
		for _, name := range []string{"bog.wal", "bog.commit"} {
			found, err := r.hasEntry(name)
			if err != nil {
				return err
			}
			if found {
				return errors.New("mixed ordinary WAL and restore layout")
			}
		}
		return r.recoverRestore(ctx)
	}
	return r.recoverLegacy(ctx)
}

func (r *retained) recoverAuthority(ctx context.Context, data []byte) error {
	a, p, err := decodeAuthority(data)
	if err != nil {
		return fmt.Errorf("decode writable authority: %w", err)
	}
	f, err := r.owner.OpenTail(a.Tail)
	if err != nil {
		return err
	}
	r.tail = &tailFile{File: f, base: a.TailBase, refs: 1}
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	st := stat.Sys().(*syscall.Stat_t)
	if uint64(st.Dev) != a.Device || st.Ino != a.Inode || stat.Size() < p.CommittedEnd-a.TailBase {
		return errors.New("selected tail file identity or size differs")
	}
	name := checkpoint.GenerationManifestName(p.Generation)
	if err := r.publisher.RecoverRecoveryView(ctx, func() (checkpoint.RecoveryView, error) {
		return r.recoveryView(p, name, r.tail)
	}); err != nil {
		return fmt.Errorf("recover selected checkpoint: %w", err)
	}
	if err := r.installRecovered(ctx, p, name); err != nil {
		return err
	}
	if err := r.owner.FinishRecovery(f, p.CommittedEnd-a.TailBase); err != nil {
		return err
	}
	r.sequence = a.Sequence
	return r.cleanRecoveredTails()
}

func (r *retained) recoverRestore(ctx context.Context) (err error) {
	p, original, err := backup.VerifyPinnedRecovery(ctx, r.root, func(start, end int64) error {
		if end-start > tailLimit {
			return checkpoint.ErrExhausted
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("verify restore: %w", err)
	}
	defer func() { err = errors.Join(err, original.Close()) }()
	f, err := r.owner.NewTail(ctx, io.NewSectionReader(original, p.CoveredOffset, p.CommittedEnd-p.CoveredOffset), p.CommittedEnd-p.CoveredOffset)
	if err != nil {
		return err
	}
	r.tail = &tailFile{File: f, base: p.CoveredOffset, refs: 1}
	raw, _, id, err := r.readManifest(canonicalManifest)
	if err != nil {
		return err
	}
	if id != p.Generation {
		return errors.New("restore manifest changed during activation")
	}
	name := checkpoint.GenerationManifestName(id)
	if _, err := r.owner.Select(name, raw); err != nil {
		return err
	}
	point := checkpoint.RecoveryPoint(p)
	if err := r.publisher.RecoverRecoveryView(ctx, func() (checkpoint.RecoveryView, error) {
		return r.recoveryView(point, name, r.tail)
	}); err != nil {
		return err
	}
	if err := r.installRecovered(ctx, point, name); err != nil {
		return err
	}
	if _, err := r.selectAuthority(r.tail, point, r.sequence); err != nil {
		return err
	}
	return r.cleanRecoveredTails()
}

func digestRange(input io.Reader, size int64) (hash.Cloner, error) {
	h := sha256.New().(hash.Cloner)
	if size != 0 {
		if _, err := io.CopyN(h, input, size); err != nil {
			return nil, err
		}
	}
	return h, nil
}

// ReaderAt coordinates for selected tails are logical, including the magic
// even when the physical tail starts after it.
func (t *tailFile) ReadAt(b []byte, off int64) (int, error) {
	if off < t.base {
		return 0, io.EOF
	}
	return t.File.ReadAt(b, off-t.base)
}

func (r *retained) installRecovered(ctx context.Context, p checkpoint.RecoveryPoint, name string) error {
	if p.CommittedEnd-p.CoveredOffset > tailLimit {
		return checkpoint.ErrExhausted
	}
	h, err := digestRange(r.tail.section(p.CoveredOffset, p.CommittedEnd), p.CommittedEnd-p.CoveredOffset)
	if err != nil {
		return err
	}
	if [32]byte(h.Sum(nil)) != p.SuffixSHA256 {
		return errors.New("committed WAL suffix digest differs")
	}
	ordinal := p.NextOrdinal
	err = checkpoint.ScanCapturedFrames(ctx, r.tail, p.CoveredOffset, p.CommittedEnd, ordinal,
		func(start, end int64, first uint64, rows []store.Row) error {
			if len(r.batches) >= maxTailFrames {
				return checkpoint.ErrExhausted
			}
			b, err := r.reserveBatch(ctx, start, end, first, rows)
			if err != nil {
				return err
			}
			b.index()
			r.batches = append(r.batches, b)
			r.maxLine = max(r.maxLine, b.maxLine)
			ordinal = first + uint64(len(rows))
			return nil
		})
	if err != nil {
		return err
	}
	if ordinal != p.CommittedOrdinal {
		return errors.New("committed WAL ordinal differs")
	}
	if err := r.activateCatalog(ctx, name, p.Generation); err != nil {
		return err
	}
	r.point, r.manifest, r.digest = p, name, h
	return nil
}

func (r *retained) activateCatalog(ctx context.Context, name string, id [32]byte) error {
	ticket, err := r.catalog.ReserveReplacement(ctx)
	if err != nil {
		return err
	}
	defer ticket.Close()
	var gen history.Generation
	if name == canonicalManifest {
		gen, err = ticket.ReplacePinned(ctx, r.dir)
	} else {
		gen, err = ticket.ReplaceSelectedPinned(ctx, r.dir, history.GenerationID(id))
	}
	r.segments, r.maxLine = gen.Segments, max(r.maxLine, gen.MaxLineBytes)
	return err
}

func (r *retained) cleanRecoveredTails() (err error) {
	stream, err := r.root.Open(".")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, stream.Close()) }()
	for {
		names, readErr := stream.Readdirnames(64)
		for _, name := range names {
			suffix, ok := strings.CutPrefix(name, "bog.wal.tail-")
			if !ok || name == r.tail.Name() || len(suffix) != 32 || strings.Trim(suffix, "0123456789abcdef") != "" {
				continue
			}
			f, err := r.owner.OpenTail(name)
			if err != nil {
				return err
			}
			if err := errors.Join(r.owner.Remove(name), f.Close()); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

var _ checkpoint.RecoveryView = recoveryView{}
