package fuzz

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/history"
	"bog/internal/store"
	"bog/internal/wal"
)

const (
	ceilingWorkerEnv = "BOG_FUZZ_CHECKPOINT_CEILING"
	ceilingSeed      = "ceiling-v1"
	ceilingStageCap  = 256 << 10
	ceilingDiskCap   = 1 << 20
	ceilingFileCap   = checkpoint.MaxSegments + 12
	ceilingRewrites  = 8 << 10
	ceilingSegment   = "bog-segment-"
	ceilingPin       = ".bog-history-pin-"
)

func TestCheckpointSegmentCeiling(t *testing.T) {
	if os.Getenv(ceilingWorkerEnv) == ceilingSeed {
		setWorkerLimits(t)
		runCheckpointCeiling(t)
		return
	}

	ctx, cancel := context.WithTimeout(t.Context(), corpusDeadline)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/proc/self/exe", "-test.run=^TestCheckpointSegmentCeiling$", "-test.count=1", "-test.v")
	cmd.WaitDelay = time.Second
	// Own the fixture root outside the worker so timeout and SIGKILL still
	// leave a directory that testing can remove.
	cmd.Env = append(cmd.Environ(), ceilingWorkerEnv+"="+ceilingSeed, "TMPDIR="+t.TempDir())
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil || err != nil {
		t.Fatalf("%s worker: context=%v process=%v\n%s", ceilingSeed, ctx.Err(), err, output)
	}
	t.Logf("%s", output)
}

type ceilingCommit struct {
	input []byte
	rows  []store.Row // Append order is the independent source of ordinals.
}

func (c ceilingCommit) boundary() history.Boundary {
	return history.Boundary{
		CoveredOffset: int64(len(c.input)),
		NextOrdinal:   uint64(len(c.rows)),
		SourceSHA256:  sha256.Sum256(c.input),
	}
}

func (c ceilingCommit) checkManifest(m *checkpoint.Manifest) error {
	if m == nil {
		return errors.New("missing checkpoint manifest")
	}
	point := c.boundary()
	if m.Version != 1 || m.CoveredOffset != point.CoveredOffset ||
		m.NextOrdinal != point.NextOrdinal || m.SourceSHA256 != digest(c.input) ||
		len(m.Segments) == 0 || len(m.Segments) > checkpoint.MaxSegments {
		return fmt.Errorf("manifest offset=%d next=%d hash=%s segments=%d, want boundary %+v",
			m.CoveredOffset, m.NextOrdinal, m.SourceSHA256, len(m.Segments), point)
	}
	return nil
}

func appendCeilingRows(t *testing.T, path string, owner *wal.Log, before ceilingCommit, added []store.Row) ceilingCommit {
	t.Helper()
	appendRows(t, owner, added)
	c := ceilingCommit{
		input: readArtifact(t, filepath.Join(path, walFile)),
		rows:  append(slices.Clone(before.rows), added...),
	}
	if !bytesAdvanced(before.input, c.input) {
		t.Fatal("acknowledged append did not extend the WAL prefix")
	}
	assertWALPoint(t, owner, c.input, uint64(len(c.rows)))
	return c
}

func bytesAdvanced(before, after []byte) bool {
	return len(after) > len(before) && slices.Equal(after[:len(before)], before)
}

func seedCheckpointCeiling(t *testing.T, path string, owner *wal.Log, root *os.Root, dir *os.File) (ceilingCommit, *checkpoint.Manifest) {
	t.Helper()
	rows := make([]store.Row, checkpoint.MaxSegments)
	for i := range rows {
		rows[i] = store.Row{TS: start.Add(time.Duration(i%5) * time.Second), Line: fmt.Sprintf("old-%04d keep", i)}
	}
	point := appendCeilingRows(t, path, owner, ceilingCommit{}, rows)
	m := &checkpoint.Manifest{
		Version:       1,
		CoveredOffset: int64(len(point.input)),
		NextOrdinal:   uint64(len(rows)),
		SourceSHA256:  digest(point.input),
		Segments:      make([]checkpoint.Segment, 0, checkpoint.MaxSegments),
	}
	// Construct a valid but maximally fragmented manifest using the public
	// block encoder. The WAL comes from an acknowledged public append.
	for i, row := range rows {
		b, err := checkpoint.EncodeBlock([]checkpoint.BlockRow{{Row: row, Ordinal: uint64(i)}})
		must(t, err)
		name := fmt.Sprintf("%s%032x", ceilingSegment, i)
		must(t, root.WriteFile(name, b, 0600))
		m.Segments = append(m.Segments, checkpoint.Segment{Name: name, SHA256: digest(b), Size: int64(len(b))})
	}
	payload := manifestPayload(t, *m)
	b, err := json.Marshal(manifestEnvelope{Payload: payload, SHA256: digest(payload)})
	must(t, err)
	must(t, root.WriteFile(manifestFile, b, 0600))
	must(t, dir.Sync())
	return point, m
}

func ceilingBackfill(label string) []store.Row {
	tied := store.Row{TS: start.Add(2 * time.Second), Line: label + " keep"}
	return []store.Row{tied, {TS: start, Line: label + " backfill"}, tied}
}

func publishCeiling(t *testing.T, ctx context.Context, p *checkpoint.Publisher, owner *wal.Log, point ceilingCommit, previous *checkpoint.Manifest) (*checkpoint.Manifest, error) {
	t.Helper()
	c, err := owner.Capture(t.Context())
	must(t, err)
	defer func() { must(t, c.Release()) }()
	if c.CommittedEnd() != int64(len(point.input)) || c.NextOrdinal() != uint64(len(point.rows)) {
		t.Fatalf("captured offset=%d ordinal=%d, want %+v", c.CommittedEnd(), c.NextOrdinal(), point.boundary())
	}
	return p.PublishCaptured(ctx, c, c.CommittedEnd(), previous, c.NextOrdinal(), digest(point.input))
}

func recoverCeiling(t *testing.T, p *checkpoint.Publisher, dir *os.File, owner *wal.Log, predecessor, committed ceilingCommit, previous *checkpoint.Manifest) {
	t.Helper()
	assertWALPoint(t, owner, committed.input, uint64(len(committed.rows)))
	if !bytesAdvanced(predecessor.input, committed.input) {
		t.Fatal("recovery fixture does not contain a committed suffix")
	}
	must(t, p.RecoverCaptured(func() (*checkpoint.Manifest, error) {
		m, err := history.ValidatePinnedAuthority(t.Context(), dir)
		if err != nil {
			return nil, err
		}
		if err := predecessor.checkManifest(m); err != nil {
			return nil, err
		}
		if !slices.Equal(m.Segments, previous.Segments) {
			return nil, errors.New("recovery selected a different predecessor segment set")
		}
		return m, nil
	}))
	must(t, checkpoint.CheckPublication(dir))
}

type ceilingFile struct {
	size int64
	sum  string
}

// Inventory records exact content identities without retaining artifact bytes.
type ceilingInventory struct {
	root, stage map[string]ceilingFile
	bytes       int64
}

func inspectCeiling(t *testing.T, path string) ceilingInventory {
	t.Helper()
	inv := ceilingInventory{root: make(map[string]ceilingFile), stage: make(map[string]ceilingFile)}
	scratch := make([]byte, 32<<10)
	record := func(dir, name string, dest map[string]ceilingFile) {
		p := filepath.Join(dir, name)
		info, err := os.Lstat(p)
		must(t, err)
		if !info.Mode().IsRegular() || info.Size() > maxArtifactSize || info.Size() > ceilingDiskCap-inv.bytes {
			t.Fatalf("fixture file %s: mode=%s size=%d total=%d", p, info.Mode(), info.Size(), inv.bytes)
		}
		f, err := os.Open(p)
		must(t, err)
		h := sha256.New()
		n, err := io.CopyBuffer(h, io.LimitReader(f, info.Size()+1), scratch)
		must(t, errors.Join(err, f.Close()))
		if n != info.Size() {
			t.Fatalf("fixture file %s changed size: read %d, stat %d", p, n, info.Size())
		}
		dest[name] = ceilingFile{n, fmt.Sprintf("%x", h.Sum(nil))}
		inv.bytes += n
	}
	for _, name := range children(t, path, ceilingFileCap) {
		if name == checkpointStage {
			stagePath := filepath.Join(path, name)
			info, err := os.Lstat(stagePath)
			must(t, err)
			if !info.IsDir() {
				t.Fatalf("staging path %s has mode %s", stagePath, info.Mode())
			}
			for _, staged := range children(t, stagePath, 2) {
				record(stagePath, staged, inv.stage)
			}
			continue
		}
		record(path, name, inv.root)
	}
	if len(inv.root)+len(inv.stage) > ceilingFileCap {
		t.Fatalf("fixture has %d root files and %d staged files, cap %d", len(inv.root), len(inv.stage), ceilingFileCap)
	}
	return inv
}

func (inv ceilingInventory) report(t *testing.T, phase string, budget *checkpoint.Budget) {
	t.Helper()
	usage := budget.Usage()
	var staged int64
	for _, f := range inv.stage {
		staged += f.size
	}
	t.Logf("seed=%s phase=%s files=%d bytes=%d stage_files=%d stage_bytes=%d staging_in_use=%d staging_peak=%d pins=%d captures=%d",
		ceilingSeed, phase, len(inv.root)+len(inv.stage), inv.bytes, len(inv.stage), staged,
		usage.InUse[checkpoint.StagingBytes], usage.Peak[checkpoint.StagingBytes],
		usage.InUse[checkpoint.PinnedGenerations], usage.InUse[checkpoint.LiveCaptures])
}

func assertCeilingFiles(t *testing.T, got, want map[string]ceilingFile) {
	t.Helper()
	for _, name := range slices.Sorted(maps.Keys(want)) {
		if got[name] != want[name] {
			t.Fatalf("%s: got %+v, want %+v", name, got[name], want[name])
		}
	}
	for _, name := range slices.Sorted(maps.Keys(got)) {
		if _, ok := want[name]; !ok {
			t.Fatalf("unexpected file %s", name)
		}
	}
}

func checkCeilingInterruption(t *testing.T, inv, before ceilingInventory, withStagedManifest bool) int64 {
	t.Helper()
	wantStage := []string{"installed-v1"}
	if withStagedManifest {
		wantStage = append(wantStage, "manifest")
	}
	if got := slices.Sorted(maps.Keys(inv.stage)); !slices.Equal(got, wantStage) {
		t.Fatalf("pending staging files %v, want %v", got, wantStage)
	}
	loser := maps.Clone(inv.root)
	for name, f := range before.root {
		if inv.root[name] != f {
			t.Fatalf("pending publication changed prior file %s", name)
		}
		delete(loser, name)
	}
	if len(loser) != 1 {
		t.Fatalf("pending publication installed %d new files, want one coalesced segment", len(loser))
	}
	var total int64
	for name, f := range loser {
		if !strings.HasPrefix(name, ceilingSegment) || f.size <= 0 || f.size > ceilingRewrites {
			t.Fatalf("unexpected losing output %s (%d bytes)", name, f.size)
		}
		total += f.size
	}
	for _, f := range inv.stage {
		total += f.size
	}
	if total > ceilingStageCap {
		t.Fatalf("pending publication materialized %d bytes, stage cap %d", total, ceilingStageCap)
	}
	return total
}

func checkCeilingWhilePending(t *testing.T, p *checkpoint.Publisher, dir *os.File, owner *wal.Log, catalog *history.Catalog, initial history.Generation, before, committed ceilingCommit, previous *checkpoint.Manifest) {
	t.Helper()
	requireIntegrity(t, "pending checkpoint", checkpoint.CheckPublication(dir), checkpoint.ErrPublicationUncertain)
	m, err := publishCeiling(t, t.Context(), p, owner, committed, previous)
	requireIntegrity(t, "retry before recovery", err, checkpoint.ErrPublicationUncertain)
	if m != nil {
		t.Fatal("retry before recovery returned an authority")
	}
	got, err := catalog.ReplacePinned(t.Context(), dir)
	requireIntegrity(t, "load before recovery", err, checkpoint.ErrPublicationUncertain)
	if got != (history.Generation{}) {
		t.Fatalf("load before recovery returned generation %+v", got)
	}
	assertHistoryRows(t, catalog.AcquireCurrent, initial, checkpointRows(before.rows))
}

func cancelCeilingStaging(t *testing.T, path string, p *checkpoint.Publisher, owner *wal.Log, committed ceilingCommit, previous *checkpoint.Manifest, before ceilingInventory, budget *checkpoint.Budget) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	observed := false
	m, err := publishCeiling(t, phaseContext{ctx, func() {
		stage := filepath.Join(path, checkpointStage)
		_, err := os.Lstat(stage)
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		must(t, err)
		if len(children(t, stage, 2)) == 0 {
			return
		}
		inv := inspectCeiling(t, path)
		assertCeilingFiles(t, inv.root, before.root)
		if len(inv.stage) != 1 {
			t.Fatalf("cancellation saw %d staged files, want one staged segment", len(inv.stage))
		}
		for name, f := range inv.stage {
			if !strings.HasPrefix(name, ceilingSegment) || f.size <= 0 || f.size > ceilingRewrites {
				t.Fatalf("unexpected staged output %s (%d bytes)", name, f.size)
			}
		}
		observed = true
		cancel()
	}}, p, owner, committed, previous)
	if !observed || m != nil || !errors.Is(err, context.Canceled) || errors.Is(err, checkpoint.ErrPublicationUncertain) {
		t.Fatalf("cancel during staging: observed=%v published=%v error=%v", observed, m != nil, err)
	}
	if got := budget.Usage().InUse[checkpoint.StagingBytes]; got != 0 {
		t.Fatalf("cancellation retained %d staging bytes", got)
	}
}

func failCeilingRename(t *testing.T, path string, p *checkpoint.Publisher, owner *wal.Log, committed ceilingCommit, previous *checkpoint.Manifest, before ceilingInventory) int64 {
	t.Helper()
	var charged int64
	m, err := publishCeiling(t, phaseContext{t.Context(), func() {
		marker := filepath.Join(path, checkpointStage, "installed-v1")
		_, err := os.Lstat(marker)
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		must(t, err)
		inv := inspectCeiling(t, path)
		charged = checkCeilingInterruption(t, inv, before, true)
		checkCeilingStagedManifest(t, path, inv, committed, previous)
		// Force the real rename to fail with ENOENT without changing the
		// existing authority or the durable intent naming losing outputs.
		must(t, os.Remove(filepath.Join(path, checkpointStage, "manifest")))
	}}, p, owner, committed, previous)
	if charged == 0 || m != nil || !errors.Is(err, checkpoint.ErrPublicationUncertain) || !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("rename failure: charged=%d published=%v error=%v", charged, m != nil, err)
	}
	return charged
}

func runCheckpointCeiling(t *testing.T) {
	path := t.TempDir()
	owner := openOwner(t, path)
	root, dir := openPublicationDir(t, path)
	baseline, initial := seedCheckpointCeiling(t, path, owner, root, dir)
	budget := checkpoint.NewBudget(checkpoint.Limits{
		checkpoint.StagingBytes:      ceilingStageCap,
		checkpoint.PinnedGenerations: 3,
		checkpoint.LiveCaptures:      2,
	})
	p := checkpoint.NewPublisher(root, dir, budget)
	catalog := history.NewCatalog(budget)
	t.Cleanup(func() { must(t, catalog.Close()) })
	old, err := catalog.ReplacePinned(t.Context(), dir)
	must(t, err)
	if old.Boundary != baseline.boundary() || len(initial.Segments) != checkpoint.MaxSegments {
		t.Fatalf("invalid ceiling fixture: boundary=%+v segments=%d", old.Boundary, len(initial.Segments))
	}
	held, err := catalog.AcquireCurrent()
	must(t, err)
	t.Cleanup(func() { must(t, held.Close()) })
	checkHistoryRows(t, held, old, checkpointRows(baseline.rows))

	committed := appendCeilingRows(t, path, owner, baseline, ceilingBackfill("first"))
	before := inspectCeiling(t, path)
	before.report(t, "baseline", budget)
	assertSettled := func(t *testing.T) {
		t.Helper()
		must(t, checkpoint.CheckPublication(dir))
		now := inspectCeiling(t, path)
		assertCeilingFiles(t, now.root, before.root)
		if len(now.stage) != 0 || budget.Usage().InUse[checkpoint.StagingBytes] != 0 {
			t.Fatal("staging did not settle")
		}
		checkHistoryRows(t, held, old, checkpointRows(baseline.rows))
		now.report(t, "settled", budget)
	}

	suite := t
	for _, name := range []string{"cancel-staged-block", "manifest-budget", "sigkill-1", "rename-enoent", "sigkill-2"} {
		if !t.Run(name, func(t *testing.T) {
			switch name {
			case "cancel-staged-block":
				cancelCeilingStaging(t, path, p, owner, committed, initial, before, budget)
			case "manifest-budget":
				rejectCeilingManifestBudget(t, p, owner, committed, initial, budget)
			case "sigkill-1", "sigkill-2":
				must(t, owner.Close())
				stop := startInterrupted(t, "checkpoint", path, "")
				inv := inspectCeiling(t, path)
				checkCeilingInterruption(t, inv, before, true)
				checkCeilingStagedManifest(t, path, inv, committed, initial)
				inv.report(t, "installed-before-rename", budget)
				stop()
				owner = openCeilingOwner(t, suite, path, committed)
				p = checkpoint.NewPublisher(root, dir, budget)
				checkCeilingWhilePending(t, p, dir, owner, catalog, old, baseline, committed, initial)
				recoverCeiling(t, p, dir, owner, baseline, committed, initial)
			case "rename-enoent":
				charged := failCeilingRename(t, path, p, owner, committed, initial, before)
				inv := inspectCeiling(t, path)
				checkCeilingInterruption(t, inv, before, false)
				if got := budget.Usage().InUse[checkpoint.StagingBytes]; got != uint64(charged) {
					t.Fatalf("rename failure charged %d bytes, want %d", got, charged)
				}
				inv.report(t, "rename-failed", budget)
				checkCeilingWhilePending(t, p, dir, owner, catalog, old, baseline, committed, initial)
				recoverCeiling(t, p, dir, owner, baseline, committed, initial)
			}
			assertSettled(t)
		}) {
			t.FailNow()
		}
	}

	checkCeilingAdvance(t, path, dir, owner, catalog, p, budget, held, old, baseline, committed, initial, before)
}

func openCeilingOwner(t, suite *testing.T, path string, point ceilingCommit) *wal.Log {
	t.Helper()
	var indexed store.Store
	owner, err := wal.Open(path, &indexed)
	must(t, err)
	// Callers explicitly close the owner before another process takes the lock.
	// Keep the fallback registered through the rest of the suite.
	suite.Cleanup(func() { must(suite, owner.Close()) })
	assertWALPoint(t, owner, point.input, uint64(len(point.rows)))
	assertStoreRows(t, &indexed, point.rows)
	return owner
}

func checkCeilingAdvance(t *testing.T, path string, dir *os.File, owner *wal.Log, catalog *history.Catalog, p *checkpoint.Publisher, budget *checkpoint.Budget, held *history.Reader, old history.Generation, baseline, committed ceilingCommit, initial *checkpoint.Manifest, before ceilingInventory) {
	t.Helper()
	scanning, err := catalog.AcquireCurrent()
	must(t, err)
	t.Cleanup(func() { must(t, scanning.Close()) })
	resume := holdCeilingScan(t, scanning, baseline)

	previous, last := initial, old
	var rewritten int64
	var merges int
	for _, label := range []string{"first", "second", "third", "fourth", "fifth"} {
		if label != "first" {
			committed = appendCeilingRows(t, path, owner, committed, ceilingBackfill(label))
		}
		ticket, err := catalog.ReserveReplacement(t.Context())
		must(t, err)
		defer ticket.Close()
		m, err := publishCeiling(t, t.Context(), p, owner, committed, previous)
		must(t, err)
		must(t, committed.checkManifest(m))
		removed, written := ceilingRewrite(t, previous, m)
		rewritten += written
		if len(previous.Segments) == checkpoint.MaxSegments {
			if removed < 2 {
				t.Fatalf("round %s at the ceiling retired only %d inputs", label, removed)
			}
			merges++
		}
		if written <= 0 || rewritten > ceilingRewrites {
			t.Fatalf("round %s: retired=%d rewritten=%d total=%d", label, removed, written, rewritten)
		}
		g, err := ticket.ReplacePinned(t.Context(), dir)
		must(t, err)
		ticket.Close()
		if g.ID == last.ID || g.Boundary != committed.boundary() ||
			g.Boundary.CoveredOffset <= last.Boundary.CoveredOffset ||
			g.Boundary.NextOrdinal <= last.Boundary.NextOrdinal {
			t.Fatalf("round %s failed to advance: previous=%+v current=%+v", label, last, g)
		}
		checkCeilingCurrentRows(t, catalog, g, committed)
		checkHistoryRows(t, held, old, checkpointRows(baseline.rows))
		must(t, catalog.ReclaimPinned(t.Context(), dir))
		inv := inspectCeiling(t, path)
		assertCeilingLive(t, inv, before, initial, m)
		assertCeilingStagingReleased(t, dir, budget)
		inv.report(t, "published-"+label, budget)
		t.Logf("seed=%s round=%s segments=%d retired=%d rewritten_bytes=%d cutoff=%d next_ordinal=%d",
			ceilingSeed, label, len(m.Segments), removed, written, g.Boundary.CoveredOffset, g.Boundary.NextOrdinal)
		previous, last = m, g
	}

	if merges < 2 {
		t.Fatalf("covered %d repacks, want at least two at the segment ceiling", merges)
	}

	reader, err := catalog.Acquire(old.ID)
	if reader != nil {
		must(t, reader.Close())
	}
	var expired *history.ExpiredError
	if reader != nil || !errors.As(err, &expired) || expired.ID != old.ID {
		t.Fatalf("acquire retired authority: reader=%v err=%v", reader != nil, err)
	}
	must(t, held.Close())
	must(t, scanning.Close())
	select {
	case <-old.Drained:
		t.Fatal("predecessor drained while an admitted scan was paused")
	default:
	}
	must(t, catalog.ReclaimPinned(t.Context(), dir))
	assertCeilingLive(t, inspectCeiling(t, path), before, initial, previous)
	resume()
	select {
	case <-old.Drained:
	default:
		t.Fatal("predecessor did not drain after its last scan returned")
	}
	must(t, catalog.ReclaimPinned(t.Context(), dir))
	settled := inspectCeiling(t, path)
	assertCeilingSegments(t, settled, previous)
	assertCeilingPins(t, settled, 1)
	assertCeilingStagingReleased(t, dir, budget)
	settled.report(t, "pin-drained", budget)
	if usage := budget.Usage(); usage.InUse[checkpoint.PinnedGenerations] != 1 || usage.InUse[checkpoint.LiveCaptures] != 0 {
		t.Fatalf("drained predecessor retained budget: %+v", usage)
	}

	must(t, catalog.Close())
	must(t, owner.Close())
	clean := inspectCeiling(t, path)
	assertCeilingSegments(t, clean, previous)
	assertCeilingPins(t, clean, 0)
	reopenedOwner := openCeilingOwner(t, t, path, committed)
	must(t, reopenedOwner.Close())
	reopened := history.NewCatalog(budget)
	defer func() { must(t, reopened.Close()) }()
	g, err := reopened.ReplacePinned(t.Context(), dir)
	must(t, err)
	if g.ID != last.ID || g.Boundary != last.Boundary {
		t.Fatalf("restart selected generation %x %+v, want %x %+v", g.ID, g.Boundary, last.ID, last.Boundary)
	}
	checkCeilingCurrentRows(t, reopened, g, committed)
	must(t, reopened.Close())
	final := inspectCeiling(t, path)
	assertCeilingFiles(t, final.root, clean.root)
	assertCeilingStagingReleased(t, dir, budget)
	if usage := budget.Usage(); usage.InUse != (checkpoint.Limits{}) || usage.Peak[checkpoint.StagingBytes] == 0 {
		t.Fatalf("final budget usage: %+v", usage)
	}
	final.report(t, "restart-clean", budget)
}

func ceilingRewrite(t *testing.T, before, after *checkpoint.Manifest) (removed int, written int64) {
	t.Helper()
	old := make(map[string]checkpoint.Segment, len(before.Segments))
	for _, s := range before.Segments {
		old[s.Name] = s
	}
	added := 0
	for _, s := range after.Segments {
		if prior, ok := old[s.Name]; ok {
			if prior != s {
				t.Fatalf("successor changed immutable segment %s", s.Name)
			}
			delete(old, s.Name)
		} else {
			added++
			written += s.Size
		}
	}
	if added != 1 {
		t.Fatalf("backfill wrote %d segments, want a single repacked block", added)
	}
	return len(old), written
}

func assertCeilingSegments(t *testing.T, inv ceilingInventory, manifests ...*checkpoint.Manifest) {
	t.Helper()
	want := make(map[string]ceilingFile)
	for _, m := range manifests {
		for _, s := range m.Segments {
			want[s.Name] = ceilingFile{s.Size, s.SHA256}
		}
	}
	got := make(map[string]ceilingFile)
	for name, f := range inv.root {
		if strings.HasPrefix(name, ceilingSegment) {
			got[name] = f
		}
	}
	assertCeilingFiles(t, got, want)
}

func assertCeilingPins(t *testing.T, inv ceilingInventory, want int) {
	t.Helper()
	count := 0
	for name := range inv.root {
		if strings.HasPrefix(name, ceilingPin) && len(name) == len(ceilingPin)+32 {
			count++
			continue
		}
		if strings.HasPrefix(name, ceilingSegment) {
			continue
		}
		switch name {
		case walFile, commitFile, manifestFile, "bog.checkpoint.lock":
		default:
			t.Fatalf("unexpected file in checkpoint namespace: %s", name)
		}
	}
	if count != want {
		t.Fatalf("found %d history pin records, want %d", count, want)
	}
	if len(inv.stage) != 0 {
		t.Fatalf("found %d staged files after publication", len(inv.stage))
	}
}

func assertCeilingLive(t *testing.T, inv, before ceilingInventory, initial, current *checkpoint.Manifest) {
	t.Helper()
	assertCeilingSegments(t, inv, initial, current)
	assertCeilingPins(t, inv, 2)
	for name, f := range before.root {
		if strings.HasPrefix(name, ceilingPin) && inv.root[name] != f {
			t.Fatalf("replacement changed the predecessor pin file %s", name)
		}
	}
}

func assertCeilingStagingReleased(t *testing.T, dir *os.File, budget *checkpoint.Budget) {
	t.Helper()
	must(t, checkpoint.CheckPublication(dir))
	if usage := budget.Usage(); usage.InUse[checkpoint.StagingBytes] != 0 ||
		usage.Peak[checkpoint.StagingBytes] > ceilingStageCap {
		t.Fatalf("staging usage after settlement: %+v", usage)
	}
}

func checkCeilingCurrentRows(t *testing.T, catalog *history.Catalog, g history.Generation, point ceilingCommit) {
	t.Helper()
	assertHistoryRows(t, catalog.AcquireCurrent, g, checkpointRows(point.rows))
	reader, err := catalog.Acquire(g.ID)
	must(t, err)
	defer func() { must(t, reader.Close()) }()
	q := history.Query{Start: start.Add(time.Second), End: start.Add(4 * time.Second), Contains: "keep"}
	want := make([]checkpoint.BlockRow, 0, len(point.rows))
	for _, row := range checkpointRows(point.rows) {
		if !row.TS.Before(q.Start) && row.TS.Before(q.End) && strings.Contains(row.Line, q.Contains) {
			want = append(want, checkpoint.BlockRow{Row: row.Row, Ordinal: row.Ordinal})
		}
	}
	must(t, scanCeilingRows(t.Context(), reader, q, want, nil))
}

// Keep an admitted scan alive across replacement and Reader.Close. The scan
// compares every visited row after release, including rows in retired files.
func holdCeilingScan(t *testing.T, reader *history.Reader, baseline ceilingCommit) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	want := make([]checkpoint.BlockRow, 0, len(baseline.rows))
	for _, row := range checkpointRows(baseline.rows) {
		want = append(want, checkpoint.BlockRow{Row: row.Row, Ordinal: row.Ordinal})
	}
	var scanErr error
	go func() {
		defer close(done)
		scanErr = scanCeilingRows(ctx, reader, history.Query{Start: start, End: end}, want, func() error {
			close(entered)
			select {
			case <-resume:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	wait := func() {
		t.Helper()
		select {
		case <-done:
		case <-time.After(interruptionDeadline):
			t.Fatal("predecessor scan did not exit")
		}
	}
	t.Cleanup(func() {
		cancel()
		wait()
	})
	select {
	case <-entered:
	case <-done:
		t.Fatalf("predecessor scan exited before reaching barrier: %v", scanErr)
	case <-time.After(interruptionDeadline):
		t.Fatal("predecessor scan did not reach barrier")
	}
	return func() {
		t.Helper()
		close(resume)
		wait()
		must(t, scanErr)
	}
}

func scanCeilingRows(ctx context.Context, reader *history.Reader, q history.Query, want []checkpoint.BlockRow, first func() error) error {
	index := 0
	err := reader.Scan(ctx, q, func(row checkpoint.BlockRow) error {
		if index >= len(want) {
			return fmt.Errorf("extra row at index %d: %+v", index, row)
		}
		if row != want[index] {
			return fmt.Errorf("row %d: got %+v, want %+v", index, row, want[index])
		}
		index++
		if index == 1 && first != nil {
			return first()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if index != len(want) {
		return fmt.Errorf("scan returned %d rows, want %d", index, len(want))
	}
	return nil
}

func checkCeilingStagedManifest(t *testing.T, path string, inv ceilingInventory, committed ceilingCommit, previous *checkpoint.Manifest) {
	t.Helper()
	b := readArtifact(t, filepath.Join(path, checkpointStage, "manifest"))
	staged := decodeManifest(t, decodeEnvelope(t, b).Payload)
	must(t, committed.checkManifest(&staged))
	removed, written := ceilingRewrite(t, previous, &staged)
	if removed < 2 || written <= 0 || written > ceilingRewrites {
		t.Fatalf("staged successor retired %d inputs and wrote %d bytes", removed, written)
	}
	for _, s := range staged.Segments {
		if inv.root[s.Name] != (ceilingFile{s.Size, s.SHA256}) {
			t.Fatalf("staged manifest references an absent or changed segment %s", s.Name)
		}
	}
}

func rejectCeilingManifestBudget(t *testing.T, p *checkpoint.Publisher, owner *wal.Log, committed ceilingCommit, previous *checkpoint.Manifest, budget *checkpoint.Budget) {
	t.Helper()
	// Leave room for the coalesced block and force admission to fail when
	// staging the successor manifest.
	const available = 2 << 10
	const blocked = ceilingStageCap - available
	hold, err := budget.Reserve(t.Context(), checkpoint.StagingBytes, blocked)
	must(t, err)
	defer hold.Release()
	m, err := publishCeiling(t, t.Context(), p, owner, committed, previous)
	var rejected *checkpoint.ExhaustedError
	if m != nil || !errors.As(err, &rejected) || errors.Is(err, checkpoint.ErrPublicationUncertain) {
		t.Fatalf("stage admission: published=%v error=%v", m != nil, err)
	}
	if rejected.Resource != checkpoint.StagingBytes || rejected.Limit != ceilingStageCap ||
		rejected.InUse <= blocked || rejected.InUse > ceilingStageCap || rejected.Need <= available {
		t.Fatalf("successor manifest admission failed at unexpected usage: %+v", rejected)
	}
	usage := budget.Usage()
	if usage.InUse[checkpoint.StagingBytes] != blocked || usage.Peak[checkpoint.StagingBytes] != rejected.InUse {
		t.Fatalf("stage admission left attempt charges: %+v", usage)
	}
	t.Logf("seed=%s phase=manifest-budget staged_bytes=%d rejected_bytes=%d staging_peak=%d",
		ceilingSeed, rejected.InUse-blocked, rejected.Need, usage.Peak[checkpoint.StagingBytes])
}
