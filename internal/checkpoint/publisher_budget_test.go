package checkpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"syscall"
	"testing"

	"bog/internal/store"
)

func TestPublisherStagingExactLimit(t *testing.T) {
	const magic = "BOGWAL01"
	source := digest([]byte(magic))
	manifest, err := json.Marshal(newManifest(int64(len(magic)), source))
	requirePublicationOK(t, err)
	envelope, err := json.Marshal(manifestEnvelope{Payload: manifest, SHA256: digest(manifest)})
	requirePublicationOK(t, err)
	intent, err := json.Marshal(publicationIntent{})
	requirePublicationOK(t, err)
	intentEnvelope, err := json.Marshal(manifestEnvelope{Payload: intent, SHA256: digest(intent)})
	requirePublicationOK(t, err)
	limit := uint64(len(envelope) + len(intentEnvelope))

	for _, capacity := range []uint64{limit - 1, limit} {
		dir, l, _ := capturedPublicationFixture(t)
		budget := NewBudget(Limits{StagingBytes: capacity})
		publisher := NewPublisher(l.root, l.dir, budget)
		m, err := publisher.PublishCaptured(t.Context(), bytes.NewReader([]byte(magic)), int64(len(magic)), nil, 0, source)
		if capacity == limit {
			requirePublicationOK(t, err)
			if m == nil || budget.Usage().Peak[StagingBytes] != limit {
				t.Fatalf("manifest = %+v, usage = %+v", m, budget.Usage())
			}
		} else {
			requireExhaustion(t, err, StagingBytes, capacity, uint64(len(envelope)), uint64(len(intentEnvelope)))
			if _, err := l.root.Stat(manifestName); m != nil || !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("rejected publication installed authority: %+v, %v", m, err)
			}
		}
		if budget.Usage().InUse[StagingBytes] != 0 {
			t.Fatalf("staging bytes leaked: directory entries %v, usage %+v", checkpointNames(t, dir), budget.Usage())
		}
		requirePublicationOK(t, CheckPublication(l.dir))
	}
}

func TestPublisherCleanupAndRecoveryKeepStagingCharges(t *testing.T) {
	for _, mode := range []string{"known cancellation", "rename lost", "rename won"} {
		t.Run(mode, func(t *testing.T) {
			dir, l, wal := capturedPublicationFixture(t)
			initial, err := l.Publish()
			requirePublicationOK(t, err)
			before := readFixture(t, dir, manifestName)
			names := checkpointNames(t, dir)
			budget := NewBudget(Limits{StagingBytes: MaxBlockSize})
			publisher := NewPublisher(l.root, l.dir, budget)
			p := l.publication()
			p.stage = &publisher.stage
			buffer := append(bytes.Clone(wal), wal[len("BOGWAL01"):]...)
			end, sum := int64(len(buffer)), digest(buffer)
			failure := syscall.EIO
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch mode {
			case "known cancellation":
				p.segmentDirSync = func() error { cancel(); return l.dir.Sync() }
			case "rename lost":
				p.manifestRename = func(_, _ string) error { return failure }
			case "rename won":
				p.manifestDirSync = func() error { return failure }
			}
			m := newManifest(end, sum)
			m.Segments = slices.Clone(initial.Segments)
			m.NextOrdinal = initial.NextOrdinal
			_, err = p.publish(ctx, m, nil, func(visit func(uint64, []store.Row) error) error {
				return scanCapturedFrames(ctx, bytes.NewReader(buffer), initial.CoveredOffset, end, initial.NextOrdinal,
					func(_, _ int64, first uint64, rows []store.Row) error { return visit(first, rows) })
			})
			usage := budget.Usage()
			if mode == "known cancellation" {
				if !errors.Is(err, context.Canceled) || usage.InUse[StagingBytes] != 0 ||
					!slices.Equal(names, checkpointNames(t, dir)) || !bytes.Equal(before, readFixture(t, dir, manifestName)) {
					t.Fatalf("cancellation: err = %v, staging = %+v, entries = %v", err, usage, checkpointNames(t, dir))
				}
			} else {
				if !errors.Is(err, ErrPublicationUncertain) || !errors.Is(err, failure) ||
					usage.InUse[StagingBytes] == 0 || usage.InUse[StagingBytes] != usage.Peak[StagingBytes] {
					t.Fatalf("uncertain publication: err = %v, staging = %+v", err, usage)
				}
				_, err := publisher.PublishCaptured(t.Context(), bytes.NewReader(buffer), end, initial, 4, sum)
				if !errors.Is(err, ErrPublicationUncertain) {
					t.Fatalf("published over unresolved staging: %v", err)
				}
				validatorErr := errors.New("authority unavailable")
				err = publisher.RecoverCaptured(func() (*Manifest, error) { return nil, validatorErr })
				if !errors.Is(err, validatorErr) || budget.Usage().InUse != usage.InUse {
					t.Fatalf("failed recovery released staging: %v, %+v", err, budget.Usage())
				}
				requirePublicationOK(t, publisher.RecoverCaptured(func() (*Manifest, error) {
					authority := initial
					if mode == "rename won" {
						authority = m
					}
					return authority, p.checkPrevious(t.Context(), authority)
				}))
			}
			if budget.Usage().InUse[StagingBytes] != 0 {
				t.Fatalf("staging leaked after settlement: %+v", budget.Usage())
			}
			requirePublicationOK(t, CheckPublication(l.dir))
			if mode == "rename won" {
				requirePublicationOK(t, p.checkPrevious(t.Context(), m))
			} else {
				if !bytes.Equal(before, readFixture(t, dir, manifestName)) {
					t.Fatal("old authority changed")
				}
				_, err := publisher.PublishCaptured(t.Context(), bytes.NewReader(buffer), end, initial, 4, sum)
				requirePublicationOK(t, err)
			}
		})
	}
}
