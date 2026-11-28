package wal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	AuthorityName   = "bog.authority"
	tailPrefix      = "bog.wal.tail-"
	restoreTailName = "bog.wal.tail"
)

// Authority binds the selected immutable generation to its writable suffix.
// All byte and row coordinates are logical, even after the tail is rebased.
type Authority struct {
	Version       int    `json:"version"`
	Sequence      uint64 `json:"sequence"`
	Generation    string `json:"generation"`
	CoveredOffset int64  `json:"covered_offset"`
	NextOrdinal   uint64 `json:"next_ordinal"`
	End           int64  `json:"committed_end"`
	Ordinal       uint64 `json:"committed_ordinal"`
	SuffixSHA256  string `json:"suffix_sha256"`
	Tail          string `json:"tail"`
	Base          int64  `json:"tail_base"`
	Device        uint64 `json:"tail_device"`
	Inode         uint64 `json:"tail_inode"`
}

type authorityEnvelope struct {
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

func digestString(sum []byte) string { return hex.EncodeToString(sum) }

func validHex(s string, length int) bool {
	return len(s) == length && strings.Trim(s, "0123456789abcdef") == ""
}

func ownedTail(name string) bool {
	suffix, ok := strings.CutPrefix(name, tailPrefix)
	return ok && validHex(suffix, 2*stageIDSize)
}

func (a Authority) valid() bool {
	return a.Version == 2 && a.Sequence != 0 && validHex(a.Generation, 64) && validHex(a.SuffixSHA256, 64) &&
		ownedTail(a.Tail) && a.Base == a.CoveredOffset && a.Base >= int64(len(magic)) &&
		a.End >= a.Base && a.Ordinal >= a.NextOrdinal &&
		(a.End == a.Base) == (a.Ordinal == a.NextOrdinal)
}

func (o *Owner) ReadAuthority() (Authority, error) {
	f, err := o.OpenFile(AuthorityName)
	if err != nil {
		return Authority{}, err
	}
	const limit = 4096
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err := errors.Join(err, f.Close()); err != nil {
		return Authority{}, err
	}
	a, err := decodeAuthority(raw, limit)
	if err != nil {
		return Authority{}, fmt.Errorf("%s: %w", AuthorityName, err)
	}
	return a, nil
}

func decodeAuthority(raw []byte, limit int) (Authority, error) {
	var a Authority
	var env authorityEnvelope
	if len(raw) > limit || objectFields(raw, "payload", "sha256") != nil ||
		json.Unmarshal(raw, &env) != nil {
		return a, errors.New("invalid authority envelope")
	}
	sum := sha256.Sum256(env.Payload)
	if env.SHA256 != digestString(sum[:]) {
		return a, errors.New("authority checksum mismatch")
	}
	if err := objectFields(env.Payload, "version", "sequence", "generation", "covered_offset", "next_ordinal",
		"committed_end", "committed_ordinal", "suffix_sha256", "tail", "tail_base", "tail_device", "tail_inode"); err != nil {
		return a, err
	}
	if json.Unmarshal(env.Payload, &a) != nil || !a.valid() {
		return Authority{}, errors.New("invalid authority fields")
	}
	return a, nil
}

func objectFields(raw []byte, required ...string) error {
	d := json.NewDecoder(strings.NewReader(string(raw)))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return errors.New("expected authority object")
	}
	fields := make(map[string]json.RawMessage)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return err
		}
		name := key.(string)
		if _, exists := fields[name]; exists {
			return fmt.Errorf("duplicate authority field %q", name)
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return err
		}
		for _, canonical := range required {
			if name != canonical && strings.EqualFold(name, canonical) {
				return fmt.Errorf("mis-cased authority field %q", name)
			}
		}
		fields[name] = value
	}
	if _, err := d.Token(); err != nil {
		return err
	}
	for _, name := range required {
		if len(fields[name]) == 0 || string(fields[name]) == "null" {
			return fmt.Errorf("missing authority field %q", name)
		}
	}
	var extra json.RawMessage
	if d.Decode(&extra) != io.EOF {
		return errors.New("trailing authority data")
	}
	return nil
}

func (o *Owner) selectAuthority(a Authority) (string, error) {
	if !a.valid() {
		return "marker_preparation", errors.New("invalid writable authority")
	}
	payload, err := json.Marshal(a)
	if err != nil {
		return "marker_preparation", err
	}
	sum := sha256.Sum256(payload)
	raw, err := json.Marshal(authorityEnvelope{payload, digestString(sum[:])})
	if err != nil {
		return "marker_preparation", err
	}
	name, err := o.files.writeStaged(markerStagePrefix, raw)
	if err != nil {
		return "marker_preparation", err
	}
	if err := o.Root().Rename(name, AuthorityName); err != nil {
		return "marker_rename", errors.Join(ErrUncertain, err)
	}
	if err := o.Directory().Sync(); err != nil {
		return "marker_sync", errors.Join(ErrUncertain, err)
	}
	return "", nil
}

func (o *Owner) OpenFile(name string) (*os.File, error) {
	return openRegular(o.Directory(), name, os.O_RDONLY)
}
