package wal

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
	"testing"

	"bog/internal/store"
)

func TestSelectedAuthorityExactFields(t *testing.T) {
	base := selector{Version: 2, Covered: 8, End: 8, Base: 8, Inode: 5,
		Tail: tailPrefix + strings.Repeat("a", 32), Generation: hexDigest([32]byte{}), Suffix: hexDigest(sha256.Sum256(nil))}
	good, err := base.encode()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, old, new string
		ok             bool
	}{
		{"valid", "", "", true},
		{"duplicate", `"version":2`, `"version":2,"version":2`, false},
		{"unknown additive field", `"version":2`, `"version":2,"VERSION":9`, true},
		{"wrong case", `"next_ordinal":0`, `"Next_Ordinal":0`, false},
		{"null ordinal", `"next_ordinal":0`, `"next_ordinal":null`, false},
		{"unsupported", `"version":2`, `"version":3`, false},
		{"invalid base", `"tail_base":8`, `"tail_base":9`, false},
		{"inconsistent empty", `"committed_ordinal":0`, `"committed_ordinal":1`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := good
			if tc.old != "" {
				payload := string(good[len(`{"payload":`):bytes.LastIndex(good, []byte(`,"sha256":`))])
				payload = strings.Replace(payload, tc.old, tc.new, 1)
				b = []byte(`{"payload":` + payload + `,"sha256":"` + hexDigest(sha256.Sum256([]byte(payload))) + `"}`)
			}
			if _, _, err := decodeSelector(b); (err == nil) != tc.ok {
				t.Fatalf("decode = %v, want success %t", err, tc.ok)
			}
		})
	}
}

func TestOwnerSelectionSyncFailureFencesUntilRecovery(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failAt   int
		selected bool
	}{
		{"before selector install", 1, false},
		{"after selector rename", 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			o, err := OpenOwner(dir)
			if err != nil {
				t.Fatal(err)
			}
			cause := syscall.EIO
			err = o.WithLock(func(l *Locked) error {
				end, err := l.OpenOrdinary()
				if err != nil {
					return err
				}
				p := Point{Covered: end, End: end, Suffix: sha256.Sum256(nil)}
				if err := l.FinishRecovery(p); err != nil {
					return err
				}
				const body = `{"ts":"2025-01-01T00:00:00Z","line":"acknowledged"}`
				if err := l.Append([]byte(body), 1); err != nil {
					return err
				}
				calls := 0
				o.log.syncDir = func() error {
					calls++
					if calls == tc.failAt {
						return cause
					}
					return o.log.dir.Sync()
				}
				p, tail := l.State()
				_, err = l.Select(p, tail.Section(p.Covered, p.End), p.Suffix)
				return err
			})
			if !errors.Is(err, cause) || errors.Is(err, ErrUncertain) != tc.selected {
				t.Fatalf("select = %v", err)
			}
			err = o.WithLock(func(*Locked) error { return nil })
			if errors.Is(err, ErrUncertain) != tc.selected {
				t.Fatalf("next admission = %v", err)
			}
			if err := o.Close(); err != nil {
				t.Fatal(err)
			}
			o, err = OpenOwner(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := o.Close(); err != nil {
					t.Error(err)
				}
			}()
			err = o.WithLock(func(l *Locked) error {
				selected, err := l.OpenSelected()
				if err != nil {
					return err
				}
				if selected != tc.selected {
					t.Fatalf("selected = %t, want %t", selected, tc.selected)
				}
				if !selected {
					end, err := l.OpenOrdinary()
					if err != nil {
						return err
					}
					_, tail := l.State()
					raw, err := io.ReadAll(tail.Section(8, end))
					if err != nil {
						return err
					}
					return l.FinishRecovery(Point{Covered: 8, End: end, Next: 1, Suffix: sha256.Sum256(raw)})
				}
				p, tail := l.State()
				if p.Next != 1 {
					t.Fatalf("lost acknowledged ordinal: %+v", p)
				}
				raw, err := io.ReadAll(tail.Section(p.Covered, p.End))
				if err != nil {
					return err
				}
				if _, err := store.Decode(raw[FrameOverhead:]); err != nil {
					return err
				}
				if err := l.FinishRecovery(p); err != nil {
					return err
				}
				return l.CleanStartup()
			})
			if err != nil {
				t.Fatal(err)
			}
			if tc.selected {
				if _, err := os.Stat(dir + "/bog.wal"); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("recovered selector left legacy predecessor: %v", err)
				}
			}
		})
	}
}
