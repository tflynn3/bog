package main

import (
	"context"
	"errors"
	"os"
	"testing"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

func TestRecoverInterruptedFirstRetainedSelection(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	s, err := openRetained(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if s != nil {
			if err := s.close(context.Background()); err != nil {
				t.Error(err)
			}
		}
	})

	payload := []byte("{\"ts\":\"2025-01-01T00:00:00Z\",\"line\":\"before selection\"}\n")
	rows, err := store.DecodeIngest(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append(payload, rows); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	pin, err := s.pinLocked(ctx)
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	pin.point.SuffixSHA256, err = rangeDigest(pin.tail, pin.point.CoveredOffset, pin.point.CommittedEnd)
	if err != nil {
		t.Fatal(errors.Join(err, pin.close()))
	}
	interrupt := errors.New("interrupted before selector")
	var staged string
	_, err = s.publisher.PublishRecoveryView(ctx, &pin.recoveryView, pin.point.CommittedEnd, pin.point.CommittedOrdinal,
		func(_ context.Context, next checkpoint.RecoveryPoint) error {
			staged = checkpoint.GenerationManifestName(next.Generation)
			return interrupt
		})
	if err := errors.Join(pin.close(), s.close(ctx)); err != nil {
		t.Fatal(err)
	}
	s = nil
	if !errors.Is(err, checkpoint.ErrPublicationUncertain) || !errors.Is(err, interrupt) {
		t.Fatalf("publication error = %v", err)
	}

	s, err = openRetained(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.root.Stat(staged); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("interrupted successor manifest remains: %v", err)
	}
	if s.current.Boundary.NextOrdinal != 0 {
		t.Fatalf("wrong checkpoint selected: %+v", s.current.Boundary)
	}
	c, err := s.wal.Capture(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Release()
	if c.NextOrdinal() != 1 {
		t.Fatalf("acknowledged row lost: committed ordinal %d", c.NextOrdinal())
	}
}
