package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"slices"
	"testing"
)

type testRecoveryView struct {
	point    RecoveryPoint
	root     *os.Root
	manifest string
	suffix   []byte
	open     func() (io.ReadCloser, error)
}

func (v testRecoveryView) Point() RecoveryPoint { return v.point }
func (v testRecoveryView) OpenManifest() (io.ReadCloser, error) {
	return v.root.Open(v.manifest)
}
func (v testRecoveryView) OpenSegment(name string) (io.ReadCloser, error) {
	return v.root.Open(name)
}
func (v testRecoveryView) OpenSuffix() (io.ReadCloser, error) {
	if v.open != nil {
		return v.open()
	}
	return io.NopCloser(bytes.NewReader(v.suffix)), nil
}

func newRecoveryFixture(t *testing.T, budget Limits) (string, *Legacy, *Publisher, testRecoveryView) {
	t.Helper()
	dir, l, wal := capturedPublicationFixture(t)
	_, err := l.Publish()
	requirePublicationOK(t, err)
	envelope := readFixture(t, dir, manifestName)
	m, id, err := DecodeManifest(envelope)
	requirePublicationOK(t, err)
	frame := wal[8:]
	tail := slices.Concat(frame, frame)
	point := RecoveryPoint{Generation: id, CoveredOffset: m.CoveredOffset, NextOrdinal: m.NextOrdinal,
		CommittedEnd: m.CoveredOffset + int64(len(tail)), CommittedOrdinal: m.NextOrdinal * 3, SuffixSHA256: sha256.Sum256(tail)}
	return dir, l, NewPublisher(l.root, l.dir, NewBudget(budget)), testRecoveryView{point, l.root, manifestName, tail, nil}
}

func TestRecoveryViewRejectsInvalidCapture(t *testing.T) {
	const stagingBytes = 1 << 20
	for _, tc := range []struct {
		name     string
		limit    uint64
		change   func(*testRecoveryView, *int64, *uint64)
		expected error
	}{
		{"split frame", 1 << 20, func(_ *testRecoveryView, cutoff *int64, _ *uint64) { *cutoff-- }, ErrIntegrity},
		{"cutoff ordinal", 1 << 20, func(_ *testRecoveryView, _ *int64, ordinal *uint64) { *ordinal++ }, ErrIntegrity},
		{"committed ordinal", 1 << 20, func(v *testRecoveryView, _ *int64, _ *uint64) { v.point.CommittedOrdinal++ }, ErrIntegrity},
		{"digest", 1 << 20, func(v *testRecoveryView, _ *int64, _ *uint64) { v.point.SuffixSHA256[0] ^= 1 }, ErrIntegrity},
		{"short retained remainder", 1 << 20, func(v *testRecoveryView, _ *int64, _ *uint64) { v.suffix = v.suffix[:len(v.suffix)-1] }, io.ErrUnexpectedEOF},
		{"extra byte", 1 << 20, func(v *testRecoveryView, _ *int64, _ *uint64) { v.suffix = append(v.suffix, 1) }, ErrIntegrity},
		{"suffix limit", 1, func(v *testRecoveryView, _ *int64, _ *uint64) {
			v.open = func() (io.ReadCloser, error) {
				t.Error("suffix opened before admission")
				return nil, errors.New("unexpected open")
			}
		}, ErrExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, l, pub, view := newRecoveryFixture(t, Limits{StagingBytes: stagingBytes, ReplayTailBytes: tc.limit})
			before := checkpointNames(t, dir)
			cutoff, ordinal := view.point.CoveredOffset+int64(len(view.suffix)/2), view.point.NextOrdinal*2
			tc.change(&view, &cutoff, &ordinal)
			m, err := pub.PublishRecoveryView(t.Context(), view, cutoff, ordinal, func(context.Context, RecoveryPoint) error {
				t.Error("selected an invalid capture")
				return nil
			})
			if m != nil || !errors.Is(err, tc.expected) {
				t.Fatalf("publish = %v, %v, want %v", m, err, tc.expected)
			}
			if got := checkpointNames(t, dir); !slices.Equal(got, before) {
				t.Fatalf("failed publication changed files: %v", got)
			}
			requirePublicationOK(t, CheckPublication(l.dir))
			if used := pub.stage.budget.Usage().InUse[StagingBytes]; used != 0 {
				t.Fatalf("failed publication kept %d staged bytes charged", used)
			}
		})
	}
}

func TestRecoveryViewInterruptedCleanup(t *testing.T) {
	for _, successorWon := range []bool{false, true} {
		name := "predecessor selected"
		if successorWon {
			name = "successor selected"
		}
		t.Run(name, func(t *testing.T) {
			dir, l, pub, initial := newRecoveryFixture(t, Limits{StagingBytes: 1 << 20, ReplayTailBytes: 1 << 20})
			originalName := initial.manifest
			original := readFixture(t, dir, originalName)
			before := checkpointNames(t, dir)
			view := initial
			cutoff := view.point.CoveredOffset + int64(len(view.suffix)/2)
			ordinal := view.point.NextOrdinal * 2
			interrupted := errors.New("interrupted")
			m, err := pub.PublishRecoveryView(t.Context(), view, cutoff, ordinal, func(_ context.Context, candidate RecoveryPoint) error {
				if successorWon {
					view.point = candidate
					view.manifest = GenerationManifestName(candidate.Generation)
					view.suffix = view.suffix[len(view.suffix)/2:]
				}
				return interrupted
			})
			if m != nil || !errors.Is(err, ErrPublicationUncertain) || !errors.Is(err, interrupted) {
				t.Fatalf("publication = %v, %v", m, err)
			}
			want := readFixture(t, dir, view.manifest)
			after := checkpointNames(t, dir)
			if len(after) <= len(before) {
				t.Fatal("uncertain publication reclaimed outputs")
			}
			load := func() (RecoveryView, error) { return view, nil }
			// Interrupt after one losing final unlink. The previous authority
			// must survive and the remaining owned names must still be known.
			if !successorWon {
				unlinks := 0
				ctx := context.WithValue(t.Context(), stagingFaultKey{}, func(step stagingStep) error {
					if step == removeFinal {
						unlinks++
						if unlinks == 2 {
							return interrupted
						}
					}
					return nil
				})
				err := pub.RecoverRecoveryView(ctx, load)
				if !errors.Is(err, interrupted) || !errors.Is(err, ErrPublicationUncertain) {
					t.Fatalf("interrupted final cleanup = %v", err)
				}
				if got := checkpointNames(t, dir); len(got) != len(after)-1 {
					t.Fatalf("wanted one completed removal, got %v", got)
				}
			}
			ctx := context.WithValue(t.Context(), stagingFaultKey{}, func(step stagingStep) error {
				if step == removeStageEntry {
					return interrupted
				}
				return nil
			})
			if err := pub.RecoverRecoveryView(ctx, load); !errors.Is(err, interrupted) {
				t.Fatalf("interrupted staging cleanup = %v", err)
			}
			if used := pub.stage.budget.Usage().InUse[StagingBytes]; used == 0 {
				t.Fatal("uncertain cleanup released staging charges")
			}
			for range 2 {
				requirePublicationOK(t, pub.RecoverRecoveryView(t.Context(), load))
				requirePublicationOK(t, CheckPublication(l.dir))
				if got := readFixture(t, dir, view.manifest); !bytes.Equal(got, want) {
					t.Fatal("recovery changed selected manifest")
				}
				if got := readFixture(t, dir, originalName); !bytes.Equal(got, original) {
					t.Fatal("recovery changed original held manifest")
				}
				if used := pub.stage.budget.Usage().InUse[StagingBytes]; used != 0 {
					t.Fatalf("recovery kept %d bytes charged", used)
				}
			}
			if got := checkpointNames(t, dir); !successorWon && !slices.Equal(got, before) {
				t.Fatalf("owned leftovers: %v, want %v", got, before)
			}
		})
	}
}

func TestRecoveryViewConsumesEntireSuffix(t *testing.T) {
	_, _, pub, view := newRecoveryFixture(t, Limits{StagingBytes: 1 << 20, ReplayTailBytes: 1 << 20})
	var selected RecoveryPoint
	m, err := pub.PublishRecoveryView(t.Context(), view, view.point.CommittedEnd, view.point.CommittedOrdinal,
		func(_ context.Context, point RecoveryPoint) error { selected = point; return nil })
	requirePublicationOK(t, err)
	if m == nil || selected.CommittedEnd != selected.CoveredOffset ||
		selected.CommittedOrdinal != selected.NextOrdinal || selected.SuffixSHA256 != sha256.Sum256(nil) {
		t.Fatalf("wrong empty suffix: %+v, %+v", m, selected)
	}
}
