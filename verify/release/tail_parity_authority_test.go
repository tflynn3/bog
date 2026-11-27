//go:build unix

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

const (
	tailAuthorityName  = "bog.authority"
	tailAuthorityLimit = 4 << 10
	tailTestWAL        = "bog.wal.tail-fixture"
)

// tailParityAuthority decodes the v2 writable selector independently of
// Storage. ADR 0038 defines the wire contract used by this probe.
type tailParityAuthority struct {
	Version int `json:"version"`
	tailParityPoint
	Tail       string `json:"tail"`
	TailBase   int64  `json:"tail_base"`
	TailDevice uint64 `json:"tail_device"`
	TailInode  uint64 `json:"tail_inode"`
}

type tailAuthorityEnvelope struct {
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

func readTailParityAuthority(ctx context.Context, root *os.Root) (tailParityAuthority, error) {
	var a tailParityAuthority
	raw, err := readArtifact(ctx, root, tailAuthorityName, tailAuthorityLimit)
	if err != nil {
		return a, err
	}
	a, err = decodeTailParityAuthority(raw)
	if err != nil {
		return a, fmt.Errorf("decode selector %q: %w: %w", tailAuthorityName, errArtifactMalformed, err)
	}
	return a, nil
}

func decodeTailParityAuthority(raw []byte) (tailParityAuthority, error) {
	var a tailParityAuthority
	var envelope tailAuthorityEnvelope
	if _, err := decodeFields(raw, map[string]any{"payload": &envelope.Payload, "sha256": &envelope.SHA256}); err != nil {
		return a, err
	}
	if envelope.SHA256 != parityDigest(envelope.Payload) {
		return a, errors.New("selector payload checksum mismatch")
	}
	if _, err := decodeFields(envelope.Payload, map[string]any{
		"version": &a.Version, "generation": &a.Generation,
		"covered_offset": &a.CoveredOffset, "next_ordinal": &a.NextOrdinal,
		"committed_end": &a.CommittedEnd, "committed_ordinal": &a.CommittedOrdinal,
		"suffix_sha256": &a.SuffixSHA256, "tail": &a.Tail, "tail_base": &a.TailBase,
		"tail_device": &a.TailDevice, "tail_inode": &a.TailInode,
	}); err != nil {
		return a, err
	}
	if a.Version != 2 {
		return a, fmt.Errorf("unsupported selector version %d", a.Version)
	}
	if a.CoveredOffset < walMagicBytes || a.CommittedEnd < a.CoveredOffset {
		return a, fmt.Errorf("invalid selector byte bounds [%d, %d)", a.CoveredOffset, a.CommittedEnd)
	}
	if a.NextOrdinal < 0 || a.CommittedOrdinal < a.NextOrdinal ||
		(a.CommittedOrdinal == a.NextOrdinal) != (a.CommittedEnd == a.CoveredOffset) {
		return a, fmt.Errorf("invalid selector ordinal bounds [%d, %d) for byte bounds [%d, %d)",
			a.NextOrdinal, a.CommittedOrdinal, a.CoveredOffset, a.CommittedEnd)
	}
	if !strings.HasPrefix(a.Tail, "bog.wal.tail-") || a.Tail == "bog.wal.tail-" ||
		strings.ContainsAny(a.Tail, "/\\\x00") {
		return a, fmt.Errorf("invalid selected tail basename %q", a.Tail)
	}
	return a, nil
}

// Hold the original descriptor across restart so inode reuse cannot make a
// replacement file look like the same physical tail.
type selectedParityTail struct {
	authority tailParityAuthority
	file      *os.File
	info      os.FileInfo
	suffix    []byte
}

// openSelectedParityTail observes a stopped candidate through its pinned
// directory. On success the caller owns the returned file.
func openSelectedParityTail(ctx context.Context, root *os.Root, want tailParityPoint, suffix []byte) (selectedParityTail, error) {
	a, err := readTailParityAuthority(ctx, root)
	if err != nil {
		return selectedParityTail{}, fmt.Errorf("read writable selector %s: %w", tailAuthorityName, err)
	}
	if err := checkTailParityPoint(a.tailParityPoint, want); err != nil {
		return selectedParityTail{}, fmt.Errorf("selected recovery point in %q: %w: %w", tailAuthorityName, errArtifactMalformed, err)
	}
	if a.TailBase != a.CoveredOffset {
		return selectedParityTail{}, fmt.Errorf("selected tail base in %q: %w: got %d, want %d", tailAuthorityName, errArtifactMalformed, a.TailBase, a.CoveredOffset)
	}
	size := int64(len(suffix))
	if size > tailParityByteLimit {
		return selectedParityTail{}, fmt.Errorf("read selected tail %q: %w", a.Tail, artifactSizeError(tailParityByteLimit))
	}
	if a.CommittedEnd-a.TailBase != size {
		return selectedParityTail{}, fmt.Errorf("read selected tail %q: %w: committed byte count differs from fixture size %d", a.Tail, errArtifactMalformed, size)
	}
	f, info, err := openRegular(ctx, root, a.Tail, size)
	if err != nil {
		return selectedParityTail{}, fmt.Errorf("open selected tail %q: %w", a.Tail, err)
	}
	actual, err := checkSelectedParityTail(ctx, f, info, a, suffix)
	if err != nil {
		return selectedParityTail{}, fmt.Errorf("read selected tail %q: %w", a.Tail, errors.Join(err, f.Close()))
	}
	return selectedParityTail{authority: a, file: f, info: info, suffix: actual}, nil
}

func checkSelectedParityTail(ctx context.Context, f *os.File, info os.FileInfo, a tailParityAuthority, suffix []byte) ([]byte, error) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint64(st.Dev) != a.TailDevice || uint64(st.Ino) != a.TailInode {
		return nil, fmt.Errorf("%w: selected tail file identity differs from writable selector", errArtifactMalformed)
	}
	actual, err := io.ReadAll(newArtifactReader(ctx, f, int64(len(suffix))))
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(actual, suffix) || parityDigest(actual) != a.SuffixSHA256 {
		return nil, fmt.Errorf("%w: selected tail bytes or digest differ from expected suffix", errArtifactMalformed)
	}
	return actual, nil
}

func TestSelectedTailAuthorityFields(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*tailParityAuthority)
		want   string
	}{
		{"version", func(a *tailParityAuthority) { a.Version++ }, "unsupported selector version"},
		{"generation", func(a *tailParityAuthority) { a.Generation = strings.Repeat("2", 64) }, "selected recovery point"},
		{"path", func(a *tailParityAuthority) { a.Tail = "bog.wal.tail-orphan/../bog.wal.tail-fixture" }, "invalid selected tail basename"},
		{"logical base", func(a *tailParityAuthority) { a.TailBase-- }, "selected tail base"},
		{"covered offset", func(a *tailParityAuthority) { a.CoveredOffset++ }, "selected recovery point"},
		{"covered ordinal", func(a *tailParityAuthority) { a.NextOrdinal++ }, "selected recovery point"},
		{"committed offset", func(a *tailParityAuthority) { a.CommittedEnd++ }, "selected recovery point"},
		{"committed ordinal", func(a *tailParityAuthority) { a.CommittedOrdinal++ }, "selected recovery point"},
		{"covered offset before magic", func(a *tailParityAuthority) { a.CoveredOffset = walMagicBytes - 1 }, "invalid selector byte bounds"},
		{"reversed offsets", func(a *tailParityAuthority) { a.CommittedEnd = a.CoveredOffset - 1 }, "invalid selector byte bounds"},
		{"reversed ordinals", func(a *tailParityAuthority) { a.CommittedOrdinal = a.NextOrdinal - 1 }, "invalid selector ordinal bounds"},
		{"negative ordinal", func(a *tailParityAuthority) { a.NextOrdinal = -1 }, "invalid selector ordinal bounds"},
		{"nonempty bytes with empty ordinal span", func(a *tailParityAuthority) { a.CommittedOrdinal = a.NextOrdinal }, "invalid selector ordinal bounds"},
		{"empty bytes with nonempty ordinal span", func(a *tailParityAuthority) { a.CommittedEnd = a.CoveredOffset }, "invalid selector ordinal bounds"},
		{"suffix digest", func(a *tailParityAuthority) { a.SuffixSHA256 = strings.Repeat("0", 64) }, "selected recovery point"},
		{"device", func(a *tailParityAuthority) { a.TailDevice++ }, "file identity"},
		{"inode", func(a *tailParityAuthority) { a.TailInode++ }, "file identity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir, root, f, a := newTailFileFixture(t)
			tc.mutate(&a)
			writeTailSelector(t, dir, a)
			tail, err := openSelectedParityTail(t.Context(), root, f.export, f.suffix)
			if err == nil {
				closeTailFixture(t, tail.file)
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("read selected tail = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestSelectedTailAuthorityEnvelope(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		edit func(payload []byte) (json.RawMessage, string)
		want string
	}{
		{name: "checksum of exact payload", edit: func(payload []byte) (json.RawMessage, string) {
			spaced := append([]byte("{\n "), payload[1:]...)
			return spaced, parityDigest(spaced)
		}},
		{name: "checksum of reencoded payload", want: "checksum mismatch", edit: func(payload []byte) (json.RawMessage, string) {
			return append([]byte("{\n "), payload[1:]...), parityDigest(payload)
		}},
		{name: "duplicate version", want: "malformed JSON object", edit: func(payload []byte) (json.RawMessage, string) {
			duplicate := append([]byte(`{"version":1,`), payload[1:]...)
			return duplicate, parityDigest(duplicate)
		}},
		{name: "escaped duplicate version", want: "malformed JSON object", edit: func(payload []byte) (json.RawMessage, string) {
			duplicate := append([]byte(`{"versio\u006e":2,`), payload[1:]...)
			return duplicate, parityDigest(duplicate)
		}},
		{name: "wrong casing", want: `missing or null "tail"`, edit: func(payload []byte) (json.RawMessage, string) {
			changed := bytes.Replace(payload, []byte(`"tail":`), []byte(`"Tail":`), 1)
			return changed, parityDigest(changed)
		}},
		{name: "checksummed extension", edit: func(payload []byte) (json.RawMessage, string) {
			extended := append([]byte(`{"extension":{"detail":true},`), payload[1:]...)
			return extended, parityDigest(extended)
		}},
		{name: "null required field", want: `missing or null "tail"`, edit: func(payload []byte) (json.RawMessage, string) {
			missing := bytes.Replace(payload, []byte(`"tail":"`+tailTestWAL+`"`), []byte(`"tail":null`), 1)
			return missing, parityDigest(missing)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir, root, f, a := newTailFileFixture(t)
			payload, err := json.Marshal(a)
			if err != nil {
				t.Fatal(err)
			}
			raw, digest := tc.edit(payload)
			// Compose the envelope directly so Marshal cannot compact the payload.
			envelope := fmt.Appendf(nil, `{"payload":%s,"sha256":%q}`, raw, digest)
			writeFixture(t, filepath.Join(dir, tailAuthorityName), envelope)
			got, err := openSelectedParityTail(t.Context(), root, f.export, f.suffix)
			if err == nil {
				t.Cleanup(func() { closeTailFixture(t, got.file) })
			}
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				if got.authority != a || !bytes.Equal(got.suffix, f.suffix) {
					t.Fatalf("unexpected selected tail: %+v", got.authority)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("read selected tail = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestVerifyParityRestartAppend(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		stale    bool
		reselect bool
		replace  bool
		want     string
	}{
		{name: "same physical tail"},
		{name: "stale committed bounds", stale: true, want: "selected recovery point"},
		{name: "reselected identical tail", reselect: true, want: "different physical tail"},
		{name: "replacement at selected name", replace: true, want: "different physical tail"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir, root, f, a := newTailFileFixture(t)
			writeTailSelector(t, dir, a)
			before, err := openSelectedParityTail(t.Context(), root, f.export, f.suffix)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { closeTailFixture(t, before.file) })
			frame, err := fixtureFrame([]row{fixtureWireRow(1, "restart probe")})
			if err != nil {
				t.Fatal(err)
			}
			orphan := "bog.wal.tail-orphan"
			writeFixture(t, filepath.Join(dir, orphan), f.suffix)
			if tc.reselect {
				a.Tail = orphan
			}
			if tc.replace {
				removeTailFixture(t, dir, a.Tail)
				writeFixture(t, filepath.Join(dir, a.Tail), f.suffix)
			}
			writable, err := root.OpenFile(a.Tail, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, writeErr := writable.Write(frame)
			if err := errors.Join(writeErr, writable.Close()); err != nil {
				t.Fatal(err)
			}
			extended := append(bytes.Clone(f.suffix), frame...)
			if !tc.stale {
				a.CommittedEnd += int64(len(frame))
				a.CommittedOrdinal++
				a.SuffixSHA256 = parityDigest(extended)
				setTailFixtureIdentity(t, dir, &a)
				writeTailSelector(t, dir, a)
			}
			err = verifyParityRestartAppend(t.Context(), root, before, frame, 1)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("restart evidence = %v, want %q", err, tc.want)
			}

			want := extended
			if tc.reselect || tc.replace {
				want = f.suffix
			}
			// The held descriptor must still read the original physical tail,
			// even when the pathname now identifies a replacement.
			actual, err := io.ReadAll(io.NewSectionReader(before.file, 0, int64(len(extended)+1)))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(actual, want) {
				t.Fatalf("held tail = %x, want %x", actual, want)
			}
		})
	}
}

func newTailFileFixture(t *testing.T) (string, *os.Root, tailCycleFixture, tailParityAuthority) {
	t.Helper()
	fixtures, err := tailParityFixtures()
	if err != nil {
		t.Fatal(err)
	}
	f := fixtures[0]
	f.export.Generation = strings.Repeat("1", 64)
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeTailFixture(t, root) })
	a := tailParityAuthority{Version: 2, tailParityPoint: f.export, Tail: tailTestWAL, TailBase: f.export.CoveredOffset}
	writeFixture(t, filepath.Join(dir, a.Tail), f.suffix)
	setTailFixtureIdentity(t, dir, &a)
	return dir, root, f, a
}

func setTailFixtureIdentity(t *testing.T, dir string, a *tailParityAuthority) {
	t.Helper()
	info, err := os.Stat(filepath.Join(dir, a.Tail))
	if err != nil {
		t.Fatal(err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("unexpected tail stat type %T", info.Sys())
	}
	a.TailDevice, a.TailInode = uint64(st.Dev), uint64(st.Ino)
}

func writeTailSelector(t *testing.T, dir string, a tailParityAuthority) {
	t.Helper()
	payload, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(tailAuthorityEnvelope{Payload: payload, SHA256: parityDigest(payload)})
	if err != nil {
		t.Fatal(err)
	}
	writeFixture(t, filepath.Join(dir, tailAuthorityName), raw)
}
