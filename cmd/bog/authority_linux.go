package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"strings"
	"syscall"

	"bog/internal/checkpoint"
)

const (
	authorityName     = "bog.authority"
	canonicalManifest = "bog.checkpoint"
	backupMetadata    = "bog.backup"
	backupTail        = "bog.wal.tail"
	tailPrefix        = "bog.wal.tail-"
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
	TailDevice       uint64 `json:"tail_device"`
	TailInode        uint64 `json:"tail_inode"`
	Sequence         uint64 `json:"compaction_generation"`
}

type authorityEnvelope struct {
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

func hexDigest(sum [32]byte) string { return hex.EncodeToString(sum[:]) }

func digestField(value string) ([32]byte, error) {
	var sum [32]byte
	if len(value) != 64 || strings.Trim(value, "0123456789abcdef") != "" {
		return sum, errors.New("invalid authority digest")
	}
	_, err := hex.Decode(sum[:], []byte(value))
	return sum, err
}

func (a authority) point() (checkpoint.RecoveryPoint, error) {
	id, idErr := digestField(a.Generation)
	sum, sumErr := digestField(a.SuffixSHA256)
	if err := errors.Join(idErr, sumErr); err != nil {
		return checkpoint.RecoveryPoint{}, err
	}
	return checkpoint.RecoveryPoint{
		Generation: id, CoveredOffset: a.CoveredOffset, NextOrdinal: a.NextOrdinal,
		CommittedEnd: a.CommittedEnd, CommittedOrdinal: a.CommittedOrdinal, SuffixSHA256: sum,
	}, nil
}

func decodeAuthority(raw []byte) (authority, error) {
	var envelope map[string]json.RawMessage
	var a authority
	var checksum string
	if !jsontext.Value(raw).IsValid() || json.Unmarshal(raw, &envelope) != nil ||
		json.Unmarshal(envelope["sha256"], &checksum) != nil ||
		checksum != hexDigest(sha256.Sum256(envelope["payload"])) {
		return a, errors.New("invalid writable authority envelope")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(envelope["payload"], &fields); err != nil {
		return a, err
	}
	required := make(map[string]json.RawMessage)
	for _, name := range []string{"version", "generation", "covered_offset", "next_ordinal", "committed_end",
		"committed_ordinal", "suffix_sha256", "tail", "tail_base", "tail_device", "tail_inode"} {
		v := fields[name]
		if len(v) == 0 || string(v) == "null" {
			return a, fmt.Errorf("writable authority is missing %s", name)
		}
		required[name] = v
	}
	// Decode exact names without allowing extension fields to override them.
	if sequence := fields["compaction_generation"]; len(sequence) > 0 {
		if string(sequence) == "null" {
			return a, errors.New("invalid compaction generation")
		}
		required["compaction_generation"] = sequence
	}
	canonical, err := json.Marshal(required)
	if err != nil {
		return a, err
	}
	if err := json.Unmarshal(canonical, &a); err != nil {
		return a, err
	}
	_, ok := tailToken(a.Tail)
	if a.Version != 2 || !ok ||
		a.TailBase < 0 || a.TailBase > a.CoveredOffset || a.CoveredOffset < int64(len("BOGWAL01")) ||
		a.CommittedEnd < a.CoveredOffset || a.CommittedOrdinal < a.NextOrdinal ||
		(a.CommittedEnd == a.CoveredOffset) != (a.CommittedOrdinal == a.NextOrdinal) {
		return a, errors.New("invalid writable authority bounds, version or tail name")
	}
	_, err = a.point()
	return a, err
}

func (s *retained) selectAuthority(a authority) (string, error) {
	payload, err := json.Marshal(a)
	if err != nil {
		return "marker_preparation", err
	}
	raw, err := json.Marshal(authorityEnvelope{Payload: payload, SHA256: hexDigest(sha256.Sum256(payload))})
	if err != nil {
		return "marker_preparation", err
	}
	return s.dir.Select(authorityName, raw)
}

func randomTailName() (string, error) {
	var id [16]byte
	_, err := rand.Read(id[:])
	return tailPrefix + hex.EncodeToString(id[:]), err
}

func (s *retained) stageTail(ctx context.Context, point checkpoint.RecoveryPoint, source io.ReaderAt, sourceBase int64) (_ *os.File, _ authority, _ hash.Cloner, err error) {
	name, err := randomTailName()
	if err != nil {
		return nil, authority{}, nil, err
	}
	staged := ".bog-wal-init-" + strings.TrimPrefix(name, tailPrefix)
	f, err := s.dir.OpenRegular(staged, os.O_CREATE|os.O_EXCL|os.O_RDWR)
	if err != nil {
		return nil, authority{}, nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, f.Close(), s.remove(staged))
		}
	}()
	if err := f.Chmod(0600); err != nil {
		return nil, authority{}, nil, err
	}
	h := sha256.New().(hash.Cloner)
	if source != nil {
		in := io.NewSectionReader(source, point.CoveredOffset-sourceBase, point.CommittedEnd-point.CoveredOffset)
		n, err := io.Copy(io.MultiWriter(f, h), checkedReader{ctx, in})
		if err != nil {
			return nil, authority{}, nil, err
		}
		if n != point.CommittedEnd-point.CoveredOffset {
			return nil, authority{}, nil, io.ErrUnexpectedEOF
		}
	}
	if err := f.Sync(); err != nil {
		return nil, authority{}, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, authority{}, nil, err
	}
	st := info.Sys().(*syscall.Stat_t)
	a := authority{
		Version: 2, Generation: hexDigest(point.Generation), CoveredOffset: point.CoveredOffset,
		NextOrdinal: point.NextOrdinal, CommittedEnd: point.CommittedEnd, CommittedOrdinal: point.CommittedOrdinal,
		SuffixSHA256: hexDigest([32]byte(h.Sum(nil))), Tail: name, TailBase: point.CoveredOffset,
		TailDevice: uint64(st.Dev), TailInode: st.Ino,
	}
	return f, a, h, nil
}

type checkedReader struct {
	ctx context.Context
	io.Reader
}

func (r checkedReader) Read(b []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(b)
}

func fileMatches(info os.FileInfo, device, inode uint64) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && info.Mode().IsRegular() && st.Nlink == 1 && st.Uid == uint32(os.Geteuid()) &&
		info.Mode().Perm()&0022 == 0 && uint64(st.Dev) == device && st.Ino == inode
}
