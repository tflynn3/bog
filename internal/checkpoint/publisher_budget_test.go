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
		name       string
		oldBlocks  int
		oldParts   []int
		extraParts []int
		tailParts  []int
		laterParts []int
		replaced   int
		rewritten  int
		reject     bool
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
			name: "reuse a coalesced output on the next flush", oldBlocks: 2,
			oldParts: []int{30}, tailParts: []int{80}, laterParts: []int{25}, replaced: 2, rewritten: 1,
		},
		{
			name: "partition suffix across staged and original slack", oldBlocks: 3,
			oldParts: []int{30}, extraParts: []int{83}, tailParts: []int{80},
			laterParts: []int{25, 19, 16}, replaced: 3, rewritten: 2,
		},
		{
			name: "refuse a row that cannot fit in a full generation", oldBlocks: 5,
			oldParts: []int{85}, tailParts: []int{30}, reject: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, l, _ := capturedPublicationFixture(t)
			budget := NewBudget(Limits{StagingBytes: uint64(tc.oldBlocks+tc.rewritten) * MaxBlockSize})
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
				parts := tc.oldParts
				if i == tc.oldBlocks-1 && tc.extraParts != nil {
					parts = tc.extraParts
				}
				rows := largeRows(parts, stamp)
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
			if tc.laterParts != nil {
				run = largeRows(tc.laterParts, stamp)
				expected = append(expected, run...)
				size = 0
				for _, row := range run {
					size += blockRowSize + len(row.Row.Line)
				}
				requirePublicationOK(t, b.flush(run, size))
			}
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
			rewritten := 0
			for _, s := range a.files {
				if s.rewritten {
					rewritten++
				}
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
			if rewritten != tc.rewritten {
				t.Fatalf("rewrote %d staged slots, want %d", rewritten, tc.rewritten)
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

func TestCeilingStagedReplacementSettlement(t *testing.T) {
	for _, mode := range []string{"overlap refused", "exact overlap", "cancel before publish", "rename lost"} {
		t.Run(mode, func(t *testing.T) {
			path, legacy, _ := capturedPublicationFixture(t)
			previous, err := legacy.Publish()
			requirePublicationOK(t, err)
			previousBytes := readFixture(t, path, manifestName)

			stamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			size := func(percent int) int { return percent * blockPayload / 100 }
			row := func(percent int, ts time.Time) store.Row {
				return store.Row{TS: ts, Line: strings.Repeat("a", size(percent)-blockRowSize)}
			}
			original := row(30, stamp)
			suffix := []store.Row{row(80, stamp), row(25, stamp.Add(-time.Second))}
			base := legacy.publication()
			old := []Segment{}
			for i := range 2 {
				data, err := EncodeBlock([]BlockRow{{Row: original, Ordinal: uint64(i)}})
				requirePublicationOK(t, err)
				name := fmt.Sprintf("%s%032x", segmentPrefix, i)
				requirePublicationOK(t, base.writeNew(name, data))
				old = append(old, Segment{Name: name, Size: int64(len(data)), SHA256: digest(data)})
			}
			// Model full, untouched segments by metadata. Publication's
			// attempt is the unit under test here. The public entry point
			// checks all referenced bytes before calling it.
			for i := len(old); i < MaxSegments; i++ {
				old = append(old, Segment{Name: fmt.Sprintf("%s%032x", segmentPrefix, i), Size: MaxBlockSize, SHA256: previous.SourceSHA256})
			}
			before := checkpointNames(t, path)
			liveBeforeReplace := uint64(2*blockHeaderSize + 2*size(30) + size(80))
			replacement := uint64(blockHeaderSize + 2*size(30) + size(25))
			overlap := liveBeforeReplace + replacement
			limit := overlap
			if mode == "overlap refused" {
				limit--
			}
			budget := NewBudget(Limits{StagingBytes: limit})
			publisher := NewPublisher(legacy.root, legacy.dir, budget)
			base.stage = &publisher.stage
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			scan := func(visit func(uint64, []store.Row) error) error { return visit(2, suffix) }
			attempt := func(p *publication, ctx context.Context) (*Manifest, error) {
				m := newManifest(previous.CoveredOffset+64, previous.SourceSHA256)
				m.Segments = old
				m.NextOrdinal = 2
				next := uint64(4)
				return p.publish(ctx, m, &next, scan)
			}

			if mode == "cancel before publish" {
				base.segmentDirSync = func() error { cancel(); return legacy.dir.Sync() }
			}
			var intent publicationIntent
			if mode == "rename lost" {
				base.manifestRename = func(_, _ string) error {
					stage, err := openStaging(legacy.dir)
					if err != nil {
						return err
					}
					intent, err = readIntent(stage)
					return errors.Join(err, stage.Close(), syscall.EIO)
				}
			}
			got, err := attempt(base, ctx)
			switch mode {
			case "overlap refused":
				requireExhaustion(t, err, StagingBytes, limit, liveBeforeReplace, replacement)
				if got != nil || budget.Usage().Peak[StagingBytes] != liveBeforeReplace {
					t.Fatalf("rejected replacement changed authority or peak: %v, %+v", got != nil, budget.Usage())
				}
			case "exact overlap":
				requirePublicationOK(t, err)
				assertCeilingReplacement(t, legacy, got, old, original, suffix)
			case "cancel before publish":
				if got != nil || !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled publication returned manifest %v and error %v", got != nil, err)
				}
			case "rename lost":
				if got != nil || !errors.Is(err, ErrPublicationUncertain) || !errors.Is(err, syscall.EIO) {
					t.Fatalf("uncertain publication returned manifest %v and error %v", got != nil, err)
				}
				if !intent.HadManifest || len(intent.Segments) != 2 {
					t.Fatalf("unexpected intent: %+v", intent)
				}
				intentData := readFixture(t, path, intentName)
				manifestData := readFixture(t, path, stagedManifest)
				var installedBytes uint64
				for _, file := range intent.Segments {
					data := readFixture(t, path, file.Name)
					if digest(data) != file.SHA256 {
						t.Fatalf("intent names wrong bytes for %s", file.Name)
					}
					installedBytes += uint64(len(data))
				}
				current := installedBytes + uint64(len(intentData)+len(manifestData))
				if installedBytes != replacement+uint64(blockHeaderSize+size(80)) ||
					budget.Usage().InUse[StagingBytes] != current {
					t.Fatalf("charges did not track live outputs and intent: %+v", budget.Usage())
				}
				requirePublicationOK(t, publisher.RecoverCaptured(func() (*Manifest, error) {
					return previous, base.checkPrevious(t.Context(), previous)
				}))
			}
			if mode != "overlap refused" && budget.Usage().Peak[StagingBytes] != overlap {
				t.Fatalf("replacement peak missed overlap: %+v, want %d", budget.Usage(), overlap)
			}
			if budget.Usage().InUse[StagingBytes] != 0 {
				t.Fatalf("staging leaked: %+v", budget.Usage())
			}
			requirePublicationOK(t, CheckPublication(legacy.dir))
			if got == nil {
				if !slices.Equal(before, checkpointNames(t, path)) || !bytes.Equal(previousBytes, readFixture(t, path, manifestName)) {
					t.Fatal("failed attempt changed authority or left files")
				}
			}
			if mode == "rename lost" {
				retry, err := attempt(&publication{root: legacy.root, dir: legacy.dir, stage: &publisher.stage}, t.Context())
				requirePublicationOK(t, err)
				assertCeilingReplacement(t, legacy, retry, old, original, suffix)
			}
		})
	}
}

func assertCeilingReplacement(t *testing.T, legacy *Legacy, m *Manifest, old []Segment, original store.Row, suffix []store.Row) {
	t.Helper()
	if m == nil || m.NextOrdinal != 4 || len(m.Segments) != MaxSegments || !slices.Equal(m.Segments[:MaxSegments-2], old[2:]) {
		t.Fatal("publication failed to pack the successor at the ceiling")
	}
	for _, s := range old[:2] {
		data, err := legacy.root.ReadFile(s.Name)
		requirePublicationOK(t, err)
		if digest(data) != s.SHA256 {
			t.Fatalf("publication changed immutable original %s", s.Name)
		}
	}
	var actual []BlockRow
	for _, s := range m.Segments[MaxSegments-2:] {
		data, err := legacy.root.ReadFile(s.Name)
		requirePublicationOK(t, err)
		rows, err := DecodeBlock(data)
		requirePublicationOK(t, err)
		if digest(data) != s.SHA256 || !slices.IsSortedFunc(rows, compareMergeRows) {
			t.Fatalf("invalid successor segment %s", s.Name)
		}
		actual = append(actual, rows...)
	}
	slices.SortFunc(actual, compareMergeRows)
	expected := []BlockRow{
		{Row: suffix[1], Ordinal: 3},
		{Row: original, Ordinal: 0},
		{Row: original, Ordinal: 1},
		{Row: suffix[0], Ordinal: 2},
	}
	if !slices.Equal(actual, expected) {
		t.Fatal("staged replacement changed backfill, duplicates or tie order")
	}
}

func TestStagedBlockSlotCapAndReuse(t *testing.T) {
	_, legacy, _ := capturedPublicationFixture(t)
	p := legacy.publication()
	budget := NewBudget(Limits{StagingBytes: 1 << 20})
	p.stage = &stagingAccount{budget: budget}
	requirePublicationOK(t, p.root.Mkdir(stageDir, 0700))
	a := publicationAttempt{pub: p, ctx: t.Context()}
	stamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	row := BlockRow{Row: store.Row{TS: stamp, Line: "tied duplicate"}}
	for i := range MaxSegments {
		row.Ordinal = uint64(i)
		requirePublicationOK(t, a.stageBlock([]BlockRow{row}))
	}
	old := a.files[0]
	replacement := []BlockRow{
		{Row: row.Row, Ordinal: 0},
		{Row: row.Row, Ordinal: MaxSegments},
	}
	used := budget.Usage().InUse[StagingBytes]
	if err := a.stageBlock(replacement); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("extra live slot returned %v", err)
	}
	requirePublicationOK(t, a.replaceBlock(0, replacement))
	current := a.files[0]
	if current.Name == old.Name || current.reusableRoom() != 0 || len(a.files) != MaxSegments {
		t.Fatal("replacement did not update and seal the staged slot")
	}
	if _, err := p.root.Lstat(stageDir + "/" + old.Name); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("superseded staging file still present: %v", err)
	}
	after := budget.Usage()
	if after.InUse[StagingBytes] != used-uint64(old.Size)+uint64(current.Size) ||
		after.Peak[StagingBytes] != used+uint64(current.Size) {
		t.Fatalf("unexpected staging usage after replacement: %+v", after)
	}
	if err := a.replaceBlock(0, replacement); !errors.Is(err, ErrIntegrity) || budget.Usage() != after {
		t.Fatalf("second replacement changed staging: %v, %+v", err, budget.Usage())
	}
	rows, err := a.readStaged(0, MaxSegments+1)
	requirePublicationOK(t, err)
	if !slices.Equal(rows, replacement) {
		t.Fatal("replacement lost rows")
	}
	if _, err := a.readStaged(0, MaxSegments); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("accepted a row outside the staged ordinal bound: %v", err)
	}
	data, err := p.root.ReadFile(stageDir + "/" + current.Name)
	requirePublicationOK(t, err)
	data[len(data)-1] ^= 1
	requirePublicationOK(t, p.root.WriteFile(stageDir+"/"+current.Name, data, 0600))
	if _, err := a.readStaged(0, MaxSegments+1); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("accepted corrupt staged bytes: %v", err)
	}

	// Simulate interruption after writing the single overlap file and
	// before unlinking the replaced slot or recording the intent.
	_, err = a.writeBlock(replacement)
	requirePublicationOK(t, err)
	err = recoverCapturedWithBudget(legacy.root, legacy.dir,
		func() (*Manifest, error) { return nil, os.ErrNotExist }, p.stage)
	if !errors.Is(err, os.ErrNotExist) || budget.Usage().InUse[StagingBytes] != 0 {
		t.Fatalf("interrupted overlap did not settle: %v, %+v", err, budget.Usage())
	}
	requirePublicationOK(t, CheckPublication(legacy.dir))
}
