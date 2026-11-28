package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/httpapi"
	"bog/internal/store"
	"bog/internal/wal"
)

func TestBootstrapLargeOrdinaryWAL(t *testing.T) {
	ctx, dir := t.Context(), t.TempDir()
	owner, err := wal.LockDirectory(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	if err := owner.RecoverOrdinary(nil); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"ts":"2025-01-01T00:00:00Z","line":"` + strings.Repeat("x", 3<<20) + `"}` + "\n")
	rows, err := store.DecodeIngest(payload)
	if err != nil {
		t.Fatal(err)
	}
	const frames = 6
	for range frames {
		if err := owner.Append(payload, rows); err != nil {
			t.Fatal(err)
		}
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}

	for range 2 {
		s, err := openRetained(ctx, dir)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = errors.Join(s.catalog.Close(), s.owner.Close()) })
		a := s.owner.Selected()
		if a.NextOrdinal != frames || a.CommittedOrdinal != frames || a.CoveredOffset != a.CommittedEnd || len(s.tail) != 0 {
			t.Fatalf("bootstrap did not cover the ordinary WAL: %+v, %d live frames", a, len(s.tail))
		}
		page, err := s.Page(ctx, httpapi.PageRequest{Limit: 1, Filter: httpapi.PageFilter{
			Start: rows[0].TS, End: rows[0].TS.Add(time.Second), Contains: "xxx",
		}}, nil)
		if err != nil || len(page.Rows) != 1 || page.Rows[0].Row != rows[0] {
			t.Fatalf("bootstrap query returned %d rows or changed the original line: %v", len(page.Rows), err)
		}
		if err := errors.Join(s.drain(ctx), s.owner.Close()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "bog.wal")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("original WAL was not reclaimed: %v", err)
	}
}

func TestRecoverInterruptedSelection(t *testing.T) {
	interrupted := errors.New("interrupted checkpoint publication")
	for _, tc := range []struct {
		name     string
		selected bool
		covered  uint64
	}{
		{"before selector", false, 1},
		{"after selector", true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, dir := t.Context(), t.TempDir()
			appendLine := func(s *retainedStorage, line string) store.Row {
				t.Helper()
				payload := []byte(`{"ts":"2025-01-01T00:00:00Z","line":"` + line + `"}` + "\n")
				rows, err := store.DecodeIngest(payload)
				if err != nil {
					t.Fatal(err)
				}
				if err := s.Append(payload, rows); err != nil {
					t.Fatal(err)
				}
				return rows[0]
			}
			open := func() *retainedStorage {
				t.Helper()
				s, err := openRetained(ctx, dir)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.owner.Close() })
				t.Cleanup(func() { _ = s.catalog.Close() })
				return s
			}

			s := open()
			first := appendLine(s, "first")
			if err := s.advance(ctx, 1); err != nil {
				t.Fatal(err)
			}
			second := appendLine(s, "second")
			s.mu.Lock()
			pin, err := s.acquireLocked(ctx, false)
			s.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			view := recoveryView{a: pin.a, wal: pin.wal, manifest: pin.reader.OpenManifest, segment: pin.reader.OpenSegment}
			var third store.Row
			m, err := s.pub.PublishRecoveryView(ctx, view, pin.a.CommittedEnd, 2, func(pubsCtx context.Context, p checkpoint.RecoveryPoint) error {
				// This commit lands after capture and must survive either outcome.
				third = appendLine(s, "second")
				if tc.selected {
					if _, err := s.owner.Select(pubsCtx, toAuthority(p), nil, false); err != nil {
						return err
					}
				}
				return interrupted
			})
			if releaseErr := pin.release(); releaseErr != nil {
				t.Fatal(releaseErr)
			}
			if m != nil || !errors.Is(err, checkpoint.ErrPublicationUncertain) || !errors.Is(err, interrupted) {
				t.Fatalf("interrupted publication = %v, %v", m, err)
			}
			if err := errors.Join(s.catalog.Close(), s.owner.Close()); err != nil {
				t.Fatal(err)
			}

			for range 2 {
				s = open()
				a := s.owner.Selected()
				if a.NextOrdinal != tc.covered || a.CommittedOrdinal != 3 {
					t.Fatalf("recovered authority %+v, want %d covered and 3 committed rows", a, tc.covered)
				}
				if _, err := os.Stat(filepath.Join(dir, ".bog-checkpoint-stage")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("checkpoint staging was not reclaimed: %v", err)
				}
				page, err := s.Page(ctx, httpapi.PageRequest{Limit: 10, Filter: httpapi.PageFilter{
					Start: first.TS, End: first.TS.Add(time.Nanosecond),
				}}, nil)
				var got []store.Row
				for _, row := range page.Rows {
					got = append(got, row.Row)
				}
				if err != nil || !slices.Equal(got, []store.Row{first, second, third}) {
					t.Fatalf("recovered rows = %v, %v", got, err)
				}
				if err := errors.Join(s.drain(ctx), s.owner.Close()); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Remove(filepath.Join(dir, "bog.authority")); err != nil {
				t.Fatal(err)
			}
			s, err = openRetained(ctx, dir)
			if s != nil {
				t.Cleanup(func() { _ = errors.Join(s.catalog.Close(), s.owner.Close()) })
			}
			if err == nil || !strings.Contains(err.Error(), "missing writable selector") {
				t.Fatalf("recovery with a missing selector = %v", err)
			}
		})
	}
}
