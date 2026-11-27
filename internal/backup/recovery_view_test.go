package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"bog/internal/checkpoint"
	"bog/internal/store"
)

// fixtureAuthority is the test owner's durable selector. The fixture keeps
// older files available to held views and models the command's sync ordering.
type fixtureAuthority struct {
	RecoveryPoint
	Manifest string
	Tail     string
}

const fixtureSelector = "fixture-authority"

type fileRecoveryView struct {
	authority fixtureAuthority
	root      *os.Root
	tail      *os.File
}

func (v *fileRecoveryView) Point() RecoveryPoint { return v.authority.RecoveryPoint }
func (v *fileRecoveryView) OpenManifest() (io.ReadCloser, error) {
	return v.root.Open(v.authority.Manifest)
}
func (v *fileRecoveryView) OpenSegment(name string) (io.ReadCloser, error) { return v.root.Open(name) }
func (v *fileRecoveryView) OpenSuffix() (io.ReadCloser, error) {
	return io.NopCloser(io.NewSectionReader(v.tail, 0, v.authority.CommittedEnd-v.authority.CoveredOffset)), nil
}

type checkpointView struct{ *fileRecoveryView }

func (v checkpointView) Point() checkpoint.RecoveryPoint {
	return checkpoint.RecoveryPoint(v.fileRecoveryView.Point())
}

func fixturePin(t testing.TB, root *os.Root, a fixtureAuthority) *fileRecoveryView {
	t.Helper()
	tail, err := root.Open(a.Tail)
	must(t, err)
	t.Cleanup(func() { must(t, tail.Close()) })
	return &fileRecoveryView{authority: a, root: root, tail: tail}
}

func fixtureSelect(root *os.Root, a fixtureAuthority) error {
	b, err := json.Marshal(a)
	if err != nil {
		return err
	}
	if err := writeBytes(root, fixtureSelector+".tmp", b); err != nil {
		return err
	}
	if err := root.Rename(fixtureSelector+".tmp", fixtureSelector); err != nil {
		return err
	}
	return syncDirectory(root)
}

func fixtureRestart(t *testing.T, p *checkpoint.Publisher, root *os.Root) *fileRecoveryView {
	t.Helper()
	var view *fileRecoveryView
	must(t, p.RecoverRecoveryView(t.Context(), func() (checkpoint.RecoveryView, error) {
		b, err := root.ReadFile(fixtureSelector)
		if err != nil {
			return nil, err
		}
		var a fixtureAuthority
		if err := json.Unmarshal(b, &a); err != nil {
			return nil, err
		}
		view = fixturePin(t, root, a)
		return checkpointView{view}, nil
	}))
	return view
}

func assertRecoveryWalk(t *testing.T, path string, point RecoveryPoint, rows []store.Row) {
	t.Helper()
	r, err := OpenRestored(t.Context(), path)
	must(t, err)
	defer func() { must(t, r.Close()) }()
	if got := r.RecoveryPoint(); got != point {
		t.Fatalf("verified point = %+v, want %+v", got, point)
	}
	checkWalk(t, r, rows)
	q := Query{Start: minTime, End: maxTime}
	walk, err := OpenReferenceWalk(t.Context(), path, q, 2)
	must(t, err)
	defer func() { must(t, walk.Close()) }()
	var got []Row
	for i := 0; i <= len(rows); i++ {
		page, done, err := walk.NextPage(t.Context())
		must(t, err)
		got = append(got, page...)
		if done {
			if want := oracle(rows, q); !slices.Equal(got, want) {
				t.Fatalf("reference walk = %+v, want %+v", got, want)
			}
			return
		}
	}
	t.Fatal("reference walk did not terminate")
}

func TestPinnedTailRecoveryTwoCycles(t *testing.T) {
	const maxSuffix = 1 << 16
	ctx := t.Context()
	parent := t.TempDir()
	f := newFixture(t)
	bundle := filepath.Join(parent, "v1")
	must(t, Export(ctx, f, bundle))
	wantRows := slices.Clone(f.rows)
	sourcePoint := RecoveryPoint{f.point.Generation, f.point.CoveredOffset, f.point.NextOrdinal,
		f.point.CommittedEnd, uint64(len(wantRows)), sha256.Sum256(f.wal[f.point.CoveredOffset:f.point.CommittedEnd])}
	base := wantRows[1].TS
	for cycle := 1; cycle <= 2; cycle++ {
		work := filepath.Join(parent, "work-"+strconv.Itoa(cycle))
		must(t, Restore(ctx, bundle, work))
		removed := bundle + ".out-of-reach"
		must(t, os.Rename(bundle, removed))
		assertRecoveryWalk(t, work, sourcePoint, wantRows)

		root, err := os.OpenRoot(work)
		must(t, err)
		t.Cleanup(func() { must(t, root.Close()) })
		dir, err := root.Open(".")
		must(t, err)
		t.Cleanup(func() { must(t, dir.Close()) })
		budget := checkpoint.NewBudget(checkpoint.Limits{checkpoint.StagingBytes: 1 << 20, checkpoint.ReplayTailBytes: maxSuffix})
		pub := checkpoint.NewPublisher(root, dir, budget)
		point, initial, err := VerifyPinnedRecovery(ctx, root, func(start, end int64) error {
			if start != sourcePoint.CoveredOffset || end != sourcePoint.CommittedEnd {
				t.Fatalf("replay admission got [%d,%d)", start, end)
			}
			return nil
		})
		must(t, err)
		tail := make([]byte, point.CommittedEnd-point.CoveredOffset)
		_, err = initial.ReadAt(tail, point.CoveredOffset)
		must(t, errors.Join(err, initial.Close()))
		a := fixtureAuthority{point, manifestName, "fixture-tail"}
		must(t, writeBytes(root, a.Tail, tail))
		must(t, fixtureSelect(root, a))
		held := fixtureRestart(t, pub, root)
		heldRows := slices.Clone(wantRows)

		// Append to the rebased tail and sync before moving the commit bound.
		add := []store.Row{
			wantRows[1],
			{TS: base.Add(-time.Duration(cycle) * time.Second), Line: "new backfill [x]"},
			{TS: base, Line: "new tie"},
		}
		appended := frameRows(t, add)
		w, err := root.OpenFile(a.Tail, os.O_WRONLY|os.O_APPEND, 0)
		must(t, err)
		n, err := w.Write(appended)
		must(t, errors.Join(err, w.Sync(), w.Close()))
		if n != len(appended) {
			t.Fatalf("wrote %d bytes, want %d", n, len(appended))
		}
		tail = append(tail, appended...)
		a.CommittedEnd += int64(len(appended))
		a.CommittedOrdinal += uint64(len(add))
		a.SuffixSHA256 = sha256.Sum256(tail)
		must(t, fixtureSelect(root, a))
		wantRows = append(wantRows, add...)
		captured := fixtureRestart(t, pub, root)
		if captured.Point() != a.RecoveryPoint || held.Point() != point {
			t.Fatal("restart or held capture changed committed bounds")
		}

		// Cover the restored frames and retain the freshly appended batch.
		used := int(point.CommittedEnd - point.CoveredOffset)
		next := point.CommittedOrdinal
		cutoff := point.CoveredOffset + int64(used)
		want := RecoveryPoint{CoveredOffset: cutoff, NextOrdinal: next, CommittedEnd: a.CommittedEnd,
			CommittedOrdinal: a.CommittedOrdinal, SuffixSHA256: sha256.Sum256(tail[used:])}
		var selected fixtureAuthority
		m, err := pub.PublishRecoveryView(ctx, checkpointView{captured}, cutoff, next,
			func(_ context.Context, candidate checkpoint.RecoveryPoint) error {
				want.Generation = candidate.Generation
				if RecoveryPoint(candidate) != want || candidate.Generation == point.Generation {
					t.Fatalf("candidate = %+v, want %+v", candidate, want)
				}
				selected = fixtureAuthority{want, checkpoint.GenerationManifestName(candidate.Generation), "fixture-rebased-tail"}
				if err := writeBytes(root, selected.Tail, tail[used:]); err != nil {
					return err
				}
				return fixtureSelect(root, selected)
			})
		must(t, err)
		if m.Version != 2 || m.Advance.Generation != hex.EncodeToString(point.Generation[:]) ||
			m.Advance.CoveredOffset != point.CoveredOffset || m.Advance.NextOrdinal != point.NextOrdinal ||
			m.Advance.ConsumedSHA256 != digest(tail[:used]) {
			t.Fatalf("wrong predecessor: %+v", m)
		}
		live := fixtureRestart(t, checkpoint.NewPublisher(root, dir, budget), root)
		if live.Point() != selected.RecoveryPoint {
			t.Fatalf("wrong selected point after restart: %+v", live.Point())
		}
		bundle = work + ".export"
		heldBundle := work + ".held"
		var wg sync.WaitGroup
		results := make(chan error, 3)
		for _, job := range []struct {
			path string
			view *fileRecoveryView
		}{{bundle, live}, {heldBundle, held}, {work + ".held-2", held}} {
			wg.Go(func() { results <- ExportRecoveryView(ctx, job.view, job.path, maxSuffix) })
		}
		wg.Wait()
		close(results)
		for err := range results {
			must(t, err)
		}
		assertRecoveryWalk(t, bundle, want, wantRows)
		assertRecoveryWalk(t, heldBundle, point, heldRows)
		assertRecoveryWalk(t, removed, sourcePoint, heldRows)
		sourcePoint = want
	}
	last := filepath.Join(parent, "last-restore")
	must(t, Restore(ctx, bundle, last))
	must(t, os.Rename(bundle, bundle+".out-of-reach"))
	assertRecoveryWalk(t, last, sourcePoint, wantRows)
	if sourcePoint.CommittedOrdinal != uint64(len(wantRows)) {
		t.Fatalf("next ordinal = %d, want %d", sourcePoint.CommittedOrdinal, len(wantRows))
	}
}

type memoryRecoveryView struct {
	*fixture
	point RecoveryPoint
	tail  []byte
	open  func() (io.ReadCloser, error)
}

func (v memoryRecoveryView) Point() RecoveryPoint { return v.point }
func (v memoryRecoveryView) OpenSuffix() (io.ReadCloser, error) {
	if v.open != nil {
		return v.open()
	}
	return io.NopCloser(bytes.NewReader(v.tail)), nil
}

type errorCloser struct {
	io.Reader
	err error
}

func (r errorCloser) Close() error { return r.err }

func TestExportRecoveryViewRejectsInvalidSuffix(t *testing.T) {
	f := newFixture(t)
	valid := bytes.Clone(f.wal[f.point.CoveredOffset:f.point.CommittedEnd])
	injected := errors.New("reader close")
	for _, tc := range []struct {
		name   string
		change func(*memoryRecoveryView)
		want   error
	}{
		{"frame checksum", func(v *memoryRecoveryView) {
			v.tail[16] ^= 1
			v.point.SuffixSHA256 = sha256.Sum256(v.tail)
		}, ErrIntegrity},
		{"extra frame byte", func(v *memoryRecoveryView) { v.tail = append(v.tail, 0) }, ErrIntegrity},
		{"short reader", func(v *memoryRecoveryView) { v.tail = v.tail[:len(v.tail)-1] }, io.ErrUnexpectedEOF},
		{"wrong ordinal", func(v *memoryRecoveryView) { v.point.CommittedOrdinal++ }, ErrIntegrity},
		{"wrong digest", func(v *memoryRecoveryView) { v.point.SuffixSHA256[0] ^= 1 }, ErrIntegrity},
		{"wrong generation", func(v *memoryRecoveryView) { v.point.Generation[0] ^= 1 }, ErrIntegrity},
		{"close", func(v *memoryRecoveryView) {
			v.open = func() (io.ReadCloser, error) { return errorCloser{bytes.NewReader(valid), injected}, nil }
		}, injected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			dest := filepath.Join(t.TempDir(), "bundle")
			v := memoryRecoveryView{fixture: f, tail: bytes.Clone(valid),
				point: RecoveryPoint{f.point.Generation, f.point.CoveredOffset, f.point.NextOrdinal,
					f.point.CommittedEnd, uint64(len(f.rows)), sha256.Sum256(valid)}}
			tc.change(&v)
			if err := ExportRecoveryView(ctx, v, dest, 1<<16); !errors.Is(err, tc.want) {
				t.Fatalf("export error = %v, want %v", err, tc.want)
			}
			if _, err := os.Stat(dest); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed export left destination: %v", err)
			}
		})
	}
}

func TestRecoveryViewEmptySuffixAndAdmission(t *testing.T) {
	f := newFixture(t)
	v := memoryRecoveryView{fixture: f, point: RecoveryPoint{
		Generation: f.point.Generation, CoveredOffset: f.point.CoveredOffset, NextOrdinal: f.point.NextOrdinal,
		CommittedEnd: f.point.CoveredOffset, CommittedOrdinal: f.point.NextOrdinal, SuffixSHA256: sha256.Sum256(nil),
	}}
	dest := filepath.Join(t.TempDir(), "empty")
	must(t, ExportRecoveryView(t.Context(), v, dest, 0))
	assertRecoveryWalk(t, dest, v.point, f.rows[:f.point.NextOrdinal])
	root, err := os.OpenRoot(dest)
	must(t, err)
	defer func() { must(t, root.Close()) }()
	refused := errors.New("refused")
	// Replace the suffix after publishing. Admission must run before opening it.
	must(t, root.Remove(tailName))
	must(t, root.Mkdir(tailName, 0700))
	p, tail, err := VerifyPinnedRecovery(t.Context(), root, func(start, end int64) error {
		if start != v.point.CoveredOffset || start != end {
			t.Fatalf("unexpected empty bounds [%d,%d)", start, end)
		}
		return refused
	})
	if p != (RecoveryPoint{}) || tail != nil || !errors.Is(err, refused) {
		t.Fatalf("admission returned %+v, %v, %v", p, tail, err)
	}
}

func TestVerifyV2BundleRejectsCorruptMetadata(t *testing.T) {
	f := newFixture(t)
	suffix := f.wal[f.point.CoveredOffset:f.point.CommittedEnd]
	v := memoryRecoveryView{fixture: f, tail: suffix, point: RecoveryPoint{
		f.point.Generation, f.point.CoveredOffset, f.point.NextOrdinal, f.point.CommittedEnd,
		uint64(len(f.rows)), sha256.Sum256(suffix),
	}}
	for _, tc := range []struct {
		name   string
		change func([]byte)
	}{
		{"unsupported version", func(b []byte) { b[7] = '3' }},
		{"generation", func(b []byte) { b[8] ^= 1 }},
		{"covered ordinal", func(b []byte) { little.PutUint64(b[48:], 1) }},
		{"logical offset overflow", func(b []byte) { little.PutUint64(b[56:], ^uint64(0)) }},
		{"committed ordinal", func(b []byte) { little.PutUint64(b[64:], v.point.CommittedOrdinal+1) }},
		{"suffix digest", func(b []byte) { b[72] ^= 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := filepath.Join(t.TempDir(), "bundle")
			must(t, ExportRecoveryView(t.Context(), v, d, 1<<16))
			name := filepath.Join(d, metadataName)
			b, err := os.ReadFile(name)
			must(t, err)
			if len(b) != 136 || string(b[:8]) != "BOGBAK02" {
				t.Fatalf("invalid v2 encoding: %x", b)
			}
			tc.change(b)
			sum := sha256.Sum256(b[:104])
			copy(b[104:], sum[:])
			must(t, os.WriteFile(name, b, 0600))
			if _, err := Verify(t.Context(), d); !errors.Is(err, ErrIntegrity) {
				t.Fatalf("verify = %v, want integrity failure", err)
			}
		})
	}
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(b []byte) (int, error) { return f(b) }

func TestExportRecoveryViewCancellationClosesSuffix(t *testing.T) {
	f := newFixture(t)
	tail := f.wal[f.point.CoveredOffset:f.point.CommittedEnd]
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	closed := errors.New("suffix close observed")
	view := memoryRecoveryView{fixture: f, point: RecoveryPoint{
		f.point.Generation, f.point.CoveredOffset, f.point.NextOrdinal, f.point.CommittedEnd,
		uint64(len(f.rows)), sha256.Sum256(tail),
	}, open: func() (io.ReadCloser, error) {
		r := bytes.NewReader(tail)
		return errorCloser{readerFunc(func(b []byte) (int, error) {
			cancel()
			return r.Read(b)
		}), closed}, nil
	}}
	dest := filepath.Join(t.TempDir(), "canceled")
	err := ExportRecoveryView(ctx, view, dest, 1<<16)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, closed) {
		t.Fatalf("export = %v, want cancellation and suffix close", err)
	}
	if _, err := os.Stat(dest); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed export left destination: %v", err)
	}
	view.open = nil
	view.tail = tail
	must(t, ExportRecoveryView(t.Context(), view, dest, 1<<16))
}
