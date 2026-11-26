package checkpoint

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

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

func TestMergeAtCeilingWithLargeBlocks(t *testing.T) {
	for _, tc := range []struct {
		name      string
		oldBlocks int
		oldParts  []int
		tailParts []int
		replaced  int
		reject    bool
	}{
		{
			name: "repack rows into two blocks", oldBlocks: 2,
			oldParts: []int{40, 20}, tailParts: []int{59}, replaced: 2,
		},
		{
			name: "use slack across more than four blocks", oldBlocks: 5,
			oldParts: []int{85}, tailParts: []int{15, 15, 15, 15, 15}, replaced: 5,
		},
		{
			name: "fold into one small original when the others do not fit", oldBlocks: 4,
			oldParts: []int{31}, tailParts: []int{46}, replaced: 1,
		},
		{
			name: "coalesce two old blocks for a separate suffix block", oldBlocks: 2,
			oldParts: []int{15}, tailParts: []int{90}, replaced: 2,
		},
		{
			name: "refuse a row that cannot fit in a full generation", oldBlocks: 5,
			oldParts: []int{85}, tailParts: []int{30}, reject: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, l, _ := capturedPublicationFixture(t)
			budget := NewBudget(Limits{StagingBytes: uint64(tc.oldBlocks) * MaxBlockSize})
			account := &stagingAccount{budget: budget}
			defer account.settle()
			p := l.publication()
			p.stage = account
			requirePublicationOK(t, l.root.Mkdir(stageDir, 0700))
			defer func() { requirePublicationOK(t, p.clearStaging()) }()
			a := publicationAttempt{pub: p, ctx: t.Context()}

			stamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			var ordinal uint64
			largeRows := func(parts []int, ts time.Time) []BlockRow {
				var rows []BlockRow
				for _, part := range parts {
					size := part * blockPayload / 100
					rows = append(rows, BlockRow{
						Row: store.Row{TS: ts, Line: strings.Repeat("a", size-blockRowSize)}, Ordinal: ordinal,
					})
					ordinal++
				}
				return rows
			}
			var expected []BlockRow
			var inputs []Segment
			for i := 0; i < tc.oldBlocks; i++ {
				rows := largeRows(tc.oldParts, stamp)
				expected = append(expected, rows...)
				data, err := EncodeBlock(rows)
				requirePublicationOK(t, err)
				name := fmt.Sprintf("%s%032x", segmentPrefix, i)
				requirePublicationOK(t, p.writeNew(name, data))
				inputs = append(inputs, Segment{Name: name, Size: int64(len(data)), SHA256: digest(data)})
			}
			b := segmentBuilder{attempt: &a, old: inputs, oldOrdinal: ordinal}
			// Model slots already held by other, larger segments. At
			// pressure the builder will only read the smaller inputs.
			for i := len(inputs); i < MaxSegments; i++ {
				b.old = append(b.old, Segment{Name: fmt.Sprintf("%s%032x", segmentPrefix, i), Size: MaxBlockSize})
			}
			run := largeRows(tc.tailParts, stamp.Add(-time.Second))
			expected = append(expected, run...)
			size := 0
			for _, row := range run {
				size += blockRowSize + len(row.Row.Line)
			}
			err := b.flush(run, size)
			if tc.reject {
				if !errors.Is(err, errSegmentCapacity) || len(a.files) != 0 || budget.Usage().InUse[StagingBytes] != 0 {
					t.Fatalf("capacity failure staged outputs: %v, files %d, usage %+v", err, len(a.files), budget.Usage())
				}
				return
			}
			requirePublicationOK(t, err)
			if len(b.manifestSegments()) != MaxSegments || b.replacedCount != tc.replaced {
				t.Fatalf("repack used %d slots and replaced %d old blocks", len(b.manifestSegments()), b.replacedCount)
			}
			var actual []BlockRow
			var written uint64
			for i, s := range inputs {
				if !b.replaced[i] {
					data, err := l.root.ReadFile(s.Name)
					requirePublicationOK(t, err)
					rows, err := DecodeBlock(data)
					requirePublicationOK(t, err)
					actual = append(actual, rows...)
				}
			}
			for _, s := range b.outputs {
				data, err := l.root.ReadFile(stageDir + "/" + s.Name)
				requirePublicationOK(t, err)
				rows, err := DecodeBlock(data)
				requirePublicationOK(t, err)
				if !slices.IsSortedFunc(rows, compareMergeRows) {
					t.Fatal("merge produced unsorted rows")
				}
				actual = append(actual, rows...)
				written += uint64(len(data))
			}
			slices.SortFunc(actual, compareMergeRows)
			slices.SortFunc(expected, compareMergeRows)
			if !slices.Equal(actual, expected) || budget.Usage().InUse[StagingBytes] != written {
				t.Fatalf("repack changed rows or staging charge: got %d rows, want %d, staged %d bytes", len(actual), len(expected), written)
			}
		})
	}
}

func TestCeilingMergeRecoversBothManifestRenameOutcomes(t *testing.T) {
	for _, won := range []bool{false, true} {
		t.Run(fmt.Sprintf("rename_won_%t", won), func(t *testing.T) {
			path, legacy, small := capturedPublicationFixture(t)
			requirePublicationOK(t, legacy.Close())

			const magic = "BOGWAL01"
			frame := small[len(magic):]
			wal := append([]byte(magic), bytes.Repeat(frame, MaxSegments/2+1)...)
			cutoff := len(wal) - len(frame)
			end := int64(len(wal))
			requirePublicationOK(t, os.WriteFile(filepath.Join(path, "bog.wal"), wal, 0600))
			marker := append([]byte("BOGCMT01"), make([]byte, 16)...)
			binary.LittleEndian.PutUint64(marker[8:16], uint64(end))
			binary.LittleEndian.PutUint64(marker[16:24], ^uint64(end))
			requirePublicationOK(t, os.WriteFile(filepath.Join(path, "bog.commit"), marker, 0600))
			legacy, err := OpenLegacy(path)
			requirePublicationOK(t, err)
			closeOnCleanup(t, legacy)

			ctx := t.Context()
			old := newManifest(int64(cutoff), digest(wal[:cutoff]))
			old.NextOrdinal = MaxSegments
			pub := legacy.publication()
			input := bytes.NewReader(wal)
			requirePublicationOK(t, scanCapturedFrames(ctx, input, int64(len(magic)), int64(cutoff), 0,
				func(_, _ int64, first uint64, rows []store.Row) error {
					for i, row := range rows {
						ordinal := first + uint64(i)
						block, err := EncodeBlock([]BlockRow{{Row: row, Ordinal: ordinal}})
						if err != nil {
							return err
						}
						name := fmt.Sprintf("%s%032x", segmentPrefix, ordinal)
						if err := legacy.root.WriteFile(name, block, 0600); err != nil {
							return err
						}
						old.Segments = append(old.Segments, Segment{Name: name, Size: int64(len(block)), SHA256: digest(block)})
					}
					return nil
				}))
			payload, err := json.Marshal(old)
			requirePublicationOK(t, err)
			data, err := json.Marshal(manifestEnvelope{Payload: payload, SHA256: digest(payload)})
			requirePublicationOK(t, err)
			requirePublicationOK(t, pub.writeNew(manifestName, data))
			requirePublicationOK(t, legacy.dir.Sync())
			_, err = legacy.OpenPublished()
			requirePublicationOK(t, err)
			before := checkpointNames(t, path)

			budget := NewBudget(Limits{StagingBytes: 256 << 10})
			publisher := NewPublisher(legacy.root, legacy.dir, budget)
			pub.stage = &publisher.stage
			if won {
				pub.manifestDirSync = func() error { return syscall.EIO }
			} else {
				pub.manifestRename = func(_, _ string) error { return syscall.EIO }
			}
			candidate := newManifest(end, digest(wal))
			candidate.Segments = old.Segments
			candidate.NextOrdinal = old.NextOrdinal
			nextOrdinal := uint64(MaxSegments + 2)
			lock, err := LockPublication(legacy.dir, true)
			requirePublicationOK(t, err)
			got, publishErr := pub.publish(ctx, candidate, &nextOrdinal, func(visit func(uint64, []store.Row) error) error {
				return scanCapturedFrames(ctx, input, int64(cutoff), end, old.NextOrdinal,
					func(_, _ int64, first uint64, rows []store.Row) error { return visit(first, rows) })
			})
			requirePublicationOK(t, lock.Close())
			if got != nil || !errors.Is(publishErr, ErrPublicationUncertain) || !errors.Is(publishErr, syscall.EIO) {
				t.Fatalf("uncertain publication returned manifest %v and error %v", got != nil, publishErr)
			}
			if budget.Usage().InUse[StagingBytes] == 0 {
				t.Fatal("uncertain publication released the staging charge")
			}
			if len(candidate.Segments) >= MaxSegments {
				t.Fatal("failed attempt did not actually build a merged successor")
			}
			requirePublicationOK(t, publisher.RecoverCaptured(func() (*Manifest, error) {
				p, err := legacy.readPublished(false)
				if err != nil {
					return nil, err
				}
				m := p.Manifest()
				return &m, nil
			}))
			if budget.Usage().InUse[StagingBytes] != 0 {
				t.Fatal("recovery did not release the staging charge")
			}
			recovered, err := legacy.OpenPublished()
			requirePublicationOK(t, err)
			if won {
				if m := recovered.Manifest(); m.NextOrdinal != nextOrdinal || !slices.Equal(m.Segments, candidate.Segments) {
					t.Fatal("recovery did not select the winning merge")
				}
			} else {
				if !slices.Equal(checkpointNames(t, path), before) || !slices.Equal(recovered.Manifest().Segments, old.Segments) {
					t.Fatal("recovery changed the prior authority or left losing files")
				}
				retry, err := publisher.PublishCaptured(ctx, input, end, old, nextOrdinal, digest(wal))
				requirePublicationOK(t, err)
				if retry == nil || retry.NextOrdinal != nextOrdinal || len(retry.Segments) >= MaxSegments {
					t.Fatal("retry did not install merged authority")
				}
			}
			// Publication recovery owns only attempt outputs. The history
			// reclaimer is responsible for retiring the replaced inputs.
			for _, s := range old.Segments {
				_, err := legacy.root.Stat(s.Name)
				requirePublicationOK(t, err)
			}
		})
	}
}
