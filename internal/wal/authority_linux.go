package wal

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	strictjson "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
)

const (
	authorityName = "bog.authority"
	authorityMax  = 4 << 10
	tailPrefix    = "bog.wal.tail-"
)

// Point binds a generation to the exact committed suffix in logical WAL
// coordinates. The ordinary WAL begins with eight bytes of magic.
type Point struct {
	Sequence     uint64
	Generation   [32]byte
	Covered, End int64
	First, Next  uint64
	Suffix       [32]byte
}

type fileID struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
}

func identity(info os.FileInfo) fileID {
	st := info.Sys().(*syscall.Stat_t)
	return fileID{uint64(st.Dev), st.Ino}
}

type selector struct {
	Sequence     uint64  `json:"sequence,omitempty"`
	Version      int     `json:"version"`
	Generation   string  `json:"generation"`
	Covered      int64   `json:"covered_offset"`
	First        uint64  `json:"next_ordinal"`
	End          int64   `json:"committed_end"`
	Next         uint64  `json:"committed_ordinal"`
	Suffix       string  `json:"suffix_sha256"`
	Tail         string  `json:"tail"`
	Base         int64   `json:"tail_base"`
	Device       uint64  `json:"tail_device"`
	Inode        uint64  `json:"tail_inode"`
	FromRestore  bool    `json:"from_restore,omitempty"`
	LegacyWAL    *fileID `json:"legacy_wal,omitempty"`
	LegacyMarker *fileID `json:"legacy_marker,omitempty"`
}

type envelope struct {
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

func hexDigest(b [32]byte) string { return hex.EncodeToString(b[:]) }

func (s selector) encode() ([]byte, error) {
	payload, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope{payload, hexDigest(sha256.Sum256(payload))})
}

// requireFields rejects duplicate keys and null required fields. Unknown
// fields are covered by the checksum and reserved for compatible additions.
func requireFields(b []byte, fields ...string) error {
	d := json.NewDecoder(bytes.NewReader(b))
	tok, err := d.Token()
	if err != nil || tok != json.Delim('{') {
		return errors.New("expected authority object")
	}
	seen := make(map[string]bool)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return err
		}
		name, ok := key.(string)
		if !ok || seen[name] {
			return errors.New("duplicate or invalid authority field")
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return err
		}
		seen[name] = string(value) != "null"
		if !seen[name] {
			return fmt.Errorf("null authority field %q", name)
		}
	}
	if _, err := d.Token(); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing authority data")
	}
	for _, field := range fields {
		if !seen[field] {
			return fmt.Errorf("missing authority field %q", field)
		}
	}
	return nil
}

func decodeSelector(b []byte) (s selector, p Point, err error) {
	var env envelope
	if err := requireFields(b, "payload", "sha256"); err != nil {
		return s, p, err
	}
	if err := json.Unmarshal(b, &env); err != nil {
		return s, p, err
	}
	if env.SHA256 != hexDigest(sha256.Sum256(env.Payload)) {
		return s, p, errors.New("authority checksum mismatch")
	}
	if err := requireFields(env.Payload, "version", "generation", "covered_offset", "next_ordinal",
		"committed_end", "committed_ordinal", "suffix_sha256", "tail", "tail_base", "tail_device", "tail_inode"); err != nil {
		return s, p, err
	}
	if err := strictjson.Unmarshal(env.Payload, &s); err != nil {
		return s, p, err
	}
	p = Point{Sequence: s.Sequence, Covered: s.Covered, End: s.End, First: s.First, Next: s.Next}
	if s.Version != 2 || !validTail(s.Tail) || s.Base < 0 || s.Base > s.Covered ||
		s.Covered < int64(len(magic)) || s.End < s.Covered || s.Next < s.First ||
		(s.End == s.Covered) != (s.Next == s.First) || s.Inode == 0 ||
		(s.LegacyWAL == nil) != (s.LegacyMarker == nil) ||
		!parseDigest(s.Generation, &p.Generation) || !parseDigest(s.Suffix, &p.Suffix) {
		return s, p, errors.New("invalid writable authority")
	}
	return s, p, nil
}

func parseDigest(s string, dst *[32]byte) bool {
	if len(s) != 64 || strings.Trim(s, "0123456789abcdef") != "" {
		return false
	}
	_, err := hex.Decode(dst[:], []byte(s))
	return err == nil
}

func validTail(name string) bool {
	id, ok := strings.CutPrefix(name, tailPrefix)
	return ok && len(id) == stageIDSize*2 && strings.Trim(id, "0123456789abcdef") == ""
}

func (l *Log) readSmall(name string, maxBytes int64) (b []byte, err error) {
	f, err := openRegular(l.dir, name, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	b, err = io.ReadAll(io.LimitReader(f, maxBytes+1))
	if err == nil && int64(len(b)) > maxBytes {
		err = fmt.Errorf("%s exceeds size limit", name)
	}
	return b, err
}
