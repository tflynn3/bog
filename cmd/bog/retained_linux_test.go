package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/internal/wal"
)

func closeRetainedTest(t *testing.T, s *retained) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.owner.Close(); err != nil {
		t.Fatal(err)
	}
}

func appendRetainedTest(t *testing.T, s *retained, body string) {
	t.Helper()
	rows, err := store.DecodeIngest([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append([]byte(body), rows); err != nil {
		t.Fatal(err)
	}
}

func advanceRetainedTest(t *testing.T, s *retained, ordinal uint64) {
	t.Helper()
	var done <-chan struct{}
	if err := s.owner.WithLock(func(l *wal.Locked) error {
		p, _ := l.State()
		for _, b := range s.active.batches {
			if b.next == ordinal {
				s.schedule(p, b.end, ordinal)
				done = s.job.done
				return nil
			}
		}
		return errors.New("missing whole-frame cutoff")
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint advance did not finish")
	}
	if err := s.owner.WithLock(func(l *wal.Locked) error {
		return s.lastFailure
	}); err != nil {
		t.Fatal(err)
	}
}

func retainedFilter(t *testing.T) httpapi.PageFilter {
	t.Helper()
	start, err := store.ParseTime("2025-01-01T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	return httpapi.PageFilter{Start: start, End: start.Add(2 * time.Second), Contains: "needle"}
}

func requirePageLines(t *testing.T, result httpapi.PageResult, want ...string) {
	t.Helper()
	var got []string
	for _, row := range result.Rows {
		got = append(got, row.Row.Line)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("page lines = %q, want %q", got, want)
	}
}

func TestRetainedSelectionPinsExactSuffixThroughReplacement(t *testing.T) {
	ctx := t.Context()
	dir := privateDataDir(t)
	s, err := openRetained(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if s != nil {
			closeRetainedTest(t, s)
		}
	}()
	const first = `{"ts":"2025-01-01T00:00:01Z","line":"needle first"}` + "\n"
	const mixed = `{"ts":"2025-01-01T00:00:00Z","line":"needle old"}` + "\n" + first +
		`{"ts":"2025-01-01T00:00:01Z","line":"NEEDLE hidden"}` + "\n" +
		`{"ts":"2025-01-01T00:00:02Z","line":"needle outside"}` + "\n"
	const backfill = `{"ts":"2024-12-31T19:00:00-05:00","line":"needle backfill"}` + "\n"
	appendRetainedTest(t, s, first)
	appendRetainedTest(t, s, mixed)
	pages := &retainedPages{storage: s}
	req := httpapi.PageRequest{Filter: retainedFilter(t), Limit: 1, Snapshot: true}
	opening, err := pages.Page(ctx, req, store.NewQueryWork(ctx, 1<<20))
	if err != nil || !opening.More || opening.Capture == "" {
		t.Fatalf("opening = %+v, %v", opening, err)
	}
	requirePageLines(t, opening, "needle old")
	req.Limit, req.Capture, req.After = 10, opening.Capture, opening.Rows[0].Position
	advanceRetainedTest(t, s, 1)
	appendRetainedTest(t, s, backfill)
	old := filepath.Join(dir, "bog.wal")
	if _, err := os.Stat(old); err != nil {
		t.Fatalf("held predecessor tail: %v", err)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			result, err := pages.Page(ctx, req, store.NewQueryWork(ctx, 1<<20))
			if err != nil || result.More {
				t.Errorf("held page = %+v, %v", result, err)
				return
			}
			requirePageLines(t, result, "needle first", "needle first")
			if result.Rows[0].Position == result.Rows[1].Position {
				t.Error("duplicate lines lost their ordinal identity")
			}
		})
	}
	wg.Wait()
	if err := pages.Release(opening.Capture); err != nil {
		t.Fatal(err)
	}
	if _, err := pages.Page(ctx, req, nil); !errors.Is(err, httpapi.ErrCaptureExpired) {
		t.Fatalf("released capture: %v", err)
	}
	verifyFresh := func() {
		result, err := pages.Page(ctx, httpapi.PageRequest{Filter: req.Filter, Limit: 10}, nil)
		if err != nil {
			t.Fatal(err)
		}
		requirePageLines(t, result, "needle old", "needle backfill", "needle first", "needle first")
		if err := result.Finish(); err != nil {
			t.Fatal(err)
		}
	}
	verifyFresh()
	closeRetainedTest(t, s)
	s = nil
	if _, err := os.Stat(old); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("predecessor was not reclaimed: %v", err)
	}
	s, err = openRetained(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	pages.storage = s
	verifyFresh()
	if err := s.owner.WithLock(func(l *wal.Locked) error {
		p, tail := l.State()
		raw, err := io.ReadAll(tail.Section(p.Covered, p.End))
		if err != nil {
			return err
		}
		if p.First != 1 || p.Next != 6 || p.Suffix != sha256.Sum256(raw) {
			return fmt.Errorf("unexpected recovered authority %+v", p)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRetainedRejectsCorruptSelectedAuthority(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(string, string) error
	}{
		{"selector checksum", func(dir, _ string) error {
			return os.WriteFile(filepath.Join(dir, "bog.authority"), []byte("{}"), 0600)
		}},
		{"tail content", func(dir, tail string) error {
			b, err := os.ReadFile(filepath.Join(dir, tail))
			if err != nil {
				return err
			}
			b[len(b)-1] ^= 1
			return os.WriteFile(filepath.Join(dir, tail), b, 0600)
		}},
		{"tail substitution", func(dir, tail string) error {
			path := filepath.Join(dir, tail)
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if err := os.Rename(path, path+".held"); err != nil {
				return err
			}
			return os.WriteFile(path, b, 0600)
		}},
		{"mixed authorities", func(dir, _ string) error {
			return os.WriteFile(filepath.Join(dir, "bog.wal"), []byte("BOGWAL01"), 0600)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := privateDataDir(t)
			s, err := openRetained(t.Context(), dir)
			if err != nil {
				t.Fatal(err)
			}
			appendRetainedTest(t, s, `{"ts":"2025-01-01T00:00:00Z","line":"needle"}`)
			appendRetainedTest(t, s, `{"ts":"2025-01-01T00:00:01Z","line":"suffix"}`)
			advanceRetainedTest(t, s, 1)
			closeRetainedTest(t, s)
			paths, err := filepath.Glob(filepath.Join(dir, "bog.wal.tail-*"))
			if err != nil || len(paths) != 1 {
				t.Fatalf("selected tail paths = %v, %v", paths, err)
			}
			if err := tc.mutate(dir, filepath.Base(paths[0])); err != nil {
				t.Fatal(err)
			}
			if recovered, err := openRetained(t.Context(), dir); err == nil {
				closeRetainedTest(t, recovered)
				t.Fatal("accepted corrupt or ambiguous committed authority")
			}
			owner, err := wal.OpenOwner(dir)
			if err != nil {
				t.Fatalf("failed recovery retained the directory lock: %v", err)
			}
			if err := owner.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWritableRestoreVersions(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			dir := privateDataDir(t)
			source := privateDataDir(t)
			legacy, err := wal.Open(source, &store.Store{})
			if err != nil {
				t.Fatal(err)
			}
			body := []byte(`{"ts":"2025-01-01T00:00:00Z","line":"needle restored"}`)
			rows, err := store.Decode(body)
			if err != nil {
				t.Fatal(err)
			}
			if err := errors.Join(legacy.Append(body, rows), legacy.Close()); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(filepath.Join(source, "bog.wal"))
			if err != nil {
				t.Fatal(err)
			}
			// Use the ordinary bootstrap to obtain a valid empty v1 envelope.
			original, err := openRetained(t.Context(), dir)
			if err != nil {
				t.Fatal(err)
			}
			_, id, err := readManifest(original.dir, canonicalManifest)
			if err != nil {
				t.Fatal(err)
			}
			closeRetainedTest(t, original)
			if err := errors.Join(os.Remove(filepath.Join(dir, "bog.wal")), os.Remove(filepath.Join(dir, "bog.commit"))); err != nil {
				t.Fatal(err)
			}
			suffixAt := 64
			if version == 2 {
				suffixAt = 72
			}
			meta := make([]byte, suffixAt+64)
			copy(meta, fmt.Sprintf("BOGBAK0%d", version))
			copy(meta[8:40], id[:])
			binary.LittleEndian.PutUint64(meta[40:48], 8)
			binary.LittleEndian.PutUint64(meta[56:64], uint64(len(raw)))
			if version == 2 {
				binary.LittleEndian.PutUint64(meta[64:72], 1)
			}
			sum := sha256.Sum256(raw[8:])
			copy(meta[suffixAt:], sum[:])
			sum = sha256.Sum256(meta[:suffixAt+32])
			copy(meta[suffixAt+32:], sum[:])
			if err := errors.Join(os.WriteFile(filepath.Join(dir, "bog.backup"), meta, 0600),
				os.WriteFile(filepath.Join(dir, "bog.wal.tail"), raw[8:], 0600)); err != nil {
				t.Fatal(err)
			}

			s, err := openRetained(t.Context(), dir)
			if err != nil {
				t.Fatal(err)
			}
			appendRetainedTest(t, s, `{"ts":"2025-01-01T00:00:00Z","line":"needle writable"}`)
			advanceRetainedTest(t, s, 1)
			closeRetainedTest(t, s)
			s, err = openRetained(t.Context(), dir)
			if err != nil {
				t.Fatal(err)
			}
			defer closeRetainedTest(t, s)
			pages := &retainedPages{storage: s}
			result, err := pages.Page(t.Context(), httpapi.PageRequest{Filter: retainedFilter(t), Limit: 10}, nil)
			if err != nil {
				t.Fatal(err)
			}
			requirePageLines(t, result, "needle restored", "needle writable")
			if err := result.Finish(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
