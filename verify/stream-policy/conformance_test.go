package streampolicy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

type syntheticControls struct {
	*syntheticTarget
	stopped bool
}

func newConformanceFixture() *syntheticControls {
	return &syntheticControls{syntheticTarget: newSynthetic()}
}

func (s *syntheticControls) conformanceAdapters() ConformanceAdapters {
	return ConformanceAdapters{s.adapters(), s, s}
}

func (s *syntheticControls) SetGrant(_ context.Context, grant Grant) error {
	at := slices.IndexFunc(s.fixture.Grants, func(existing Grant) bool { return existing.Target == grant.Target })
	if at < 0 {
		return errors.New("unknown synthetic grant")
	}
	s.fixture.Grants[at] = grant
	return nil
}

func (s *syntheticControls) SetMasking(_ context.Context, update MaskingChange) error {
	s.masking[update.Scope] = update
	return nil
}

func (s *syntheticControls) SetRedaction(_ context.Context, update RedactionChange) error {
	s.redaction[update.Scope] = update
	return nil
}

func (s *syntheticControls) InterruptAppend(_ context.Context, r IngestRequest, boundary CommitBoundary) (InterruptedAppend, error) {
	if s.stopped {
		return InterruptedAppend{}, errors.New("synthetic fixture is already stopped")
	}
	prepared := false
	_, err := s.submit(r, func(frame syntheticFrame, encoded []byte) {
		prepared = true
		if boundary == AfterCommit {
			s.publish(frame, encoded)
		}
	})
	if err != nil {
		return InterruptedAppend{}, err
	}
	if !prepared {
		return InterruptedAppend{}, errors.New("append did not reach the synthetic preparation boundary")
	}
	s.stopped = true
	s.frames = nil
	clear(s.cursors)
	return InterruptedAppend{Stopped: true, Boundary: boundary}, nil
}

func (s *syntheticControls) Reopen(ctx context.Context) (CommitEvidence, error) {
	state, err := s.RestartAndReplay(ctx)
	if err != nil {
		return CommitEvidence{}, err
	}
	s.stopped = false
	return s.evidence(state)
}

func (s *syntheticControls) Inspect(context.Context) (CommitEvidence, error) {
	if s.stopped {
		return CommitEvidence{}, errors.New("synthetic fixture is stopped")
	}
	return s.evidence(Committed{Complete: true, Bytes: bytes.Clone(s.committed), Records: s.records()})
}

func (s *syntheticControls) evidence(state Committed) (CommitEvidence, error) {
	evidence := CommitEvidence{Committed: state, RetryRecords: []RetryRecord{}}
	err := decodeFixture(bytes.NewReader(state.Bytes), func(frame syntheticFrame) error {
		evidence.RetryRecords = append(evidence.RetryRecords,
			RetryRecord{frame.Scope, frame.Principal, frame.Key, frame.PolicyVersion, frame.Status})
		return nil
	})
	return evidence, err
}

func conformanceContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestExtendedReferenceScenario(t *testing.T) {
	t.Parallel()
	for i := range 8 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			fixture := newConformanceFixture()
			report, err := RunConformance(conformanceContext(t), fixture.conformanceAdapters())
			if err != nil {
				t.Fatal(err)
			}
			if !report.SyntheticPassed || report.Qualified != nil {
				t.Fatalf("unexpected synthetic report: %+v", report)
			}
			if got := len(fixture.frames); got != 8 {
				t.Fatalf("committed %d frames, want 8", got)
			}
		})
	}
}

type faultyControls struct {
	*syntheticControls
	grant     func(Grant) bool
	mask      func(MaskingChange) MaskingChange
	reopen    func(*CommitEvidence)
	interrupt func(*InterruptedAppend)
	inspect   func(*CommitEvidence)
	last      CommitBoundary
}

func (f *faultyControls) SetGrant(ctx context.Context, grant Grant) error {
	if f.grant == nil || f.grant(grant) {
		return f.syntheticControls.SetGrant(ctx, grant)
	}
	return nil
}

func (f *faultyControls) SetMasking(ctx context.Context, mask MaskingChange) error {
	if f.mask != nil {
		mask = f.mask(mask)
	}
	return f.syntheticControls.SetMasking(ctx, mask)
}

func (f *faultyControls) Reopen(ctx context.Context) (CommitEvidence, error) {
	got, err := f.syntheticControls.Reopen(ctx)
	if err == nil && f.reopen != nil {
		f.reopen(&got)
	}
	return got, err
}

func (f *faultyControls) InterruptAppend(ctx context.Context, request IngestRequest, boundary CommitBoundary) (InterruptedAppend, error) {
	f.last = boundary
	got, err := f.syntheticControls.InterruptAppend(ctx, request, boundary)
	if err == nil && f.interrupt != nil {
		f.interrupt(&got)
	}
	return got, err
}

func (f *faultyControls) Inspect(ctx context.Context) (CommitEvidence, error) {
	got, err := f.syntheticControls.Inspect(ctx)
	if err == nil && f.inspect != nil {
		f.inspect(&got)
	}
	return got, err
}

func TestConformanceRegressions(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		phase string
		want  error
		apply func(*faultyControls, *observedTarget)
	}{
		{"revoke query ignored", "revoked query", ErrViolation, func(f *faultyControls, _ *observedTarget) {
			f.grant = func(g Grant) bool { return g.Query }
		}},
		{"revoke export ignored", "create after export revocation", ErrViolation, func(f *faultyControls, _ *observedTarget) {
			f.grant = func(g Grant) bool { return g.Export }
		}},
		{"artifact disclosed after grant revocation", "download after export revocation", ErrViolation, func(f *faultyControls, o *observedTarget) {
			o.fetch = func(target Target, id string, artifact *Artifact) {
				if target == (Target{alice, "fixture-a"}) && len(f.frames) == 5 && id != missingArtifact &&
					artifact.Status == http.StatusForbidden {
					artifact.Bytes = []byte("disclosed artifact")
				}
			}
		}},
		{"revoke ingest ignored", "ingest revoked before outcome lookup", ErrViolation, func(f *faultyControls, _ *observedTarget) {
			f.grant = func(g Grant) bool { return g.Ingest }
		}},
		{"revision reused when disabling mask", "restoring masking rules", ErrViolation, func(f *faultyControls, _ *observedTarget) {
			f.mask = func(update MaskingChange) MaskingChange {
				if update.Replacement.Forbidden == "" {
					update.Revision = ""
				}
				return update
			}
		}},
		{"forged query rows", "resume held snapshot", ErrViolation, func(f *faultyControls, o *observedTarget) {
			o.query = func(_ QueryRequest, page *Page) error {
				if page.Status == http.StatusOK && len(f.frames) == 5 && len(page.Rows) > 0 {
					page.Body = append(page.Body, page.Body...)
				}
				return nil
			}
		}},
		{"held export leaks later row", "held export", ErrViolation, func(f *faultyControls, o *observedTarget) {
			o.fetch = func(_ Target, _ string, artifact *Artifact) {
				if artifact.Verified && len(f.frames) == 5 {
					artifact.Bytes = append(artifact.Bytes, backfillMarker...)
				}
			}
		}},
		{"held export leaks retry key", "held export", ErrViolation, func(f *faultyControls, o *observedTarget) {
			o.fetch = func(_ Target, _ string, artifact *Artifact) {
				if artifact.Verified && len(f.frames) == 5 {
					artifact.Manifest.Version = backfillKey
				}
			}
		}},
		{"fresh export leaks another stream's retry key", "interleaved snapshot query and export: authorized export download", ErrViolation, func(f *faultyControls, o *observedTarget) {
			o.fetch = func(target Target, _ string, artifact *Artifact) {
				if artifact.Verified && len(f.frames) == 5 && target.Selector == "fixture-b" {
					artifact.Bytes = append(artifact.Bytes, backfillKey...)
				}
			}
		}},
		{"masked walk leaks hidden value in header", "fresh query under current masking policy", ErrViolation, func(f *faultyControls, o *observedTarget) {
			o.query = func(request QueryRequest, page *Page) error {
				if page.Status == http.StatusOK && request.Contains == maskedMarker && f.masking[f.fixture.Streams[0].Scope].Revision == "fixture-mask-r2" {
					page.Headers = map[string][]string{"X-Fixture-Hint": {maskedMarker}}
				}
				return nil
			}
		}},
		{"masked cursor opening leaks hidden value", "policy changes between pages", ErrViolation, func(f *faultyControls, o *observedTarget) {
			openings := 0
			o.query = func(request QueryRequest, page *Page) error {
				if page.NextCursor != "" && request.Cursor == "" && request.Contains == "" && f.masking[f.fixture.Streams[0].Scope].Revision == "fixture-mask-r2" {
					openings++
					if openings == 2 {
						page.NextCursor = maskedMarker + page.NextCursor
						page.Headers["Bog-Next-Cursor"] = []string{page.NextCursor}
					}
				}
				return nil
			}
		}},
		{"boundary missing", "interrupt before commit", ErrIncomplete, func(f *faultyControls, _ *observedTarget) {
			f.interrupt = func(got *InterruptedAppend) { got.Stopped = false }
		}},
		{"response escaped", "interrupt before commit", ErrViolation, func(f *faultyControls, _ *observedTarget) {
			f.interrupt = func(got *InterruptedAppend) { got.ResponseStarted = true }
		}},
		{"retry survives uncommitted batch", "recovery before commit", ErrViolation, func(f *faultyControls, _ *observedTarget) {
			f.reopen = func(e *CommitEvidence) {
				record := e.RetryRecords[0]
				record.Key = rollbackKey
				e.RetryRecords = append(e.RetryRecords, record)
			}
		}},
		{"uncommitted row hidden from decoded evidence", "recovery before commit", ErrViolation, func(f *faultyControls, _ *observedTarget) {
			f.reopen = func(e *CommitEvidence) {
				e.Bytes = append(e.Bytes, beforeCommitMarker...)
			}
		}},
		{"rows recovered without their retry", "recovery after commit", ErrViolation, func(f *faultyControls, _ *observedTarget) {
			f.reopen = func(e *CommitEvidence) {
				if f.last == AfterCommit {
					e.RetryRecords = e.RetryRecords[:len(e.RetryRecords)-1]
				}
			}
		}},
		{"retry recovered without its rows", "recovery after commit", ErrViolation, func(f *faultyControls, _ *observedTarget) {
			f.reopen = func(e *CommitEvidence) {
				if f.last == AfterCommit {
					e.Records = e.Records[:len(e.Records)-2]
				}
			}
		}},
		{"missing retry evidence", "running committed authority", ErrIncomplete, func(f *faultyControls, _ *observedTarget) {
			f.inspect = func(e *CommitEvidence) { e.RetryRecords = nil }
		}},
		{"redaction policy version mutated", "recovery after commit", ErrViolation, func(f *faultyControls, _ *observedTarget) {
			f.reopen = func(e *CommitEvidence) {
				if f.last == AfterCommit {
					e.RetryRecords[len(e.RetryRecords)-1].PolicyVersion = changedRedactionVersion
				}
			}
		}},
		{"unredacted replay bytes", "recovery after commit", ErrViolation, func(f *faultyControls, _ *observedTarget) {
			f.reopen = func(e *CommitEvidence) {
				if f.last == AfterCommit {
					e.Bytes = append(e.Bytes, ndjson("2026-01-01T00:00:02Z", escaped)...)
				}
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newConformanceFixture()
			fault := &faultyControls{syntheticControls: fixture}
			observed := &observedTarget{syntheticTarget: fixture.syntheticTarget}
			test.apply(fault, observed)
			adapters := ConformanceAdapters{Adapters{observed, observed, observed, observed}, fault, fault}
			report, err := RunConformance(conformanceContext(t), adapters)
			if !errors.Is(err, test.want) || !strings.Contains(err.Error(), test.phase) || report != (Report{}) {
				t.Fatalf("report %+v, error %v, want %v in phase %q", report, err, test.want, test.phase)
			}
			if strings.Contains(err.Error(), escaped) || strings.Contains(err.Error(), interruptedKey) {
				t.Fatalf("diagnostic echoed fixture inputs: %v", err)
			}
		})
	}
}

type candidateProbe func(context.Context) (Candidate, error)

func (p candidateProbe) ObserveCandidate(ctx context.Context) (Candidate, error) { return p(ctx) }

func TestQualificationUsesSeparateCandidateEvidence(t *testing.T) {
	t.Parallel()
	identity := Candidate{strings.Repeat("a", 40), strings.Repeat("b", 64)}
	for _, tc := range []struct {
		name  string
		at    int
		calls int
		want  error
	}{
		{name: "verified identity at every reopen", calls: 7},
		{name: "first reference restart has wrong identity", at: 2, calls: 2, want: ErrViolation},
		{name: "atomic recovery starts a different process", at: 5, calls: 5, want: ErrViolation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newConformanceFixture()
			calls := 0
			probe := candidateProbe(func(context.Context) (Candidate, error) {
				calls++
				got := identity
				if calls == tc.at {
					got.BinarySHA256 = strings.Repeat("c", 64)
				}
				return got, nil
			})
			report, err := QualifyShippedAdapter(conformanceContext(t), fixture.conformanceAdapters(), identity, probe)
			if !errors.Is(err, tc.want) || calls != tc.calls {
				t.Fatalf("qualification used %d identity observations, error %v", calls, err)
			}
			if tc.want == nil {
				if report.SyntheticPassed || report.Qualified == nil || *report.Qualified != identity {
					t.Fatalf("unexpected qualification report: %+v", report)
				}
			} else if report != (Report{}) {
				t.Fatalf("failed run generated a positive report: %+v", report)
			}
		})
	}
}

func TestConformanceMissingEvidence(t *testing.T) {
	t.Parallel()
	ctx := conformanceContext(t)
	fixture := newConformanceFixture()
	adapters := fixture.conformanceAdapters()
	for _, missing := range []ConformanceAdapters{
		{Adapters: adapters.Adapters, Policy: fixture},
		{Adapters: adapters.Adapters, Commit: fixture},
	} {
		if report, err := RunConformance(ctx, missing); !errors.Is(err, ErrIncomplete) || report != (Report{}) {
			t.Fatalf("missing adapter yielded report %+v, error %v", report, err)
		}
	}
	probe := candidateProbe(func(context.Context) (Candidate, error) {
		t.Fatal("identity called for malformed candidate")
		return Candidate{}, nil
	})
	if report, err := QualifyShippedAdapter(ctx, adapters, Candidate{}, probe); !errors.Is(err, ErrIncomplete) || report != (Report{}) {
		t.Fatalf("malformed candidate yielded report %+v, error %v", report, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if report, err := RunConformance(canceled, adapters); !errors.Is(err, context.Canceled) || !errors.Is(err, ErrIncomplete) || report != (Report{}) {
		t.Fatalf("canceled scenario yielded report %+v, error %v", report, err)
	}
}

func TestReferenceModelBackfill(t *testing.T) {
	t.Parallel()
	fixture := ReferenceFixture()
	model := newReferenceModel(fixture, fixtureAppends(fixture))
	batch := backfillBatch(fixture)
	model.append(batch)
	request := QueryRequest{
		Target: batch.request.Target, Start: fixture.QueryStart, End: fixture.QueryEnd,
	}
	before := []Row{
		{fixture.QueryStart, maskedMarker + " " + replacement},
		{"2026-01-01T00:00:01Z", "retained-alice-a " + replacement},
		{"2026-01-01T00:00:01Z", backfillMarker + " " + replacement},
		{"2026-01-01T00:00:01Z", backfillMarker + " " + replacement},
		{"2026-01-01T00:00:04Z", "retained-bob-a " + replacement},
	}
	if got := model.query(request); !slices.Equal(got, before) {
		t.Fatalf("reference query got %v, want %v", got, before)
	}
	scope := fixture.Streams[0].Scope
	model.masking[scope] = MaskingChange{scope, "fixture-test-revision", Replacement{maskedMarker, maskResult}}
	for _, tc := range []struct {
		filter string
		want   []Row
	}{
		{maskedMarker, nil},
		{maskResult, []Row{{fixture.QueryStart, maskResult + " " + replacement}}},
		{strings.ToUpper(backfillMarker), nil},
	} {
		request.Contains = tc.filter
		if got := model.query(request); !slices.Equal(got, tc.want) {
			t.Fatalf("reference query with %q got %v, want %v", tc.filter, got, tc.want)
		}
	}
	request.Target.Principal = exporter
	if got := model.query(request); len(got) != 0 {
		t.Fatalf("export-only principal received query rows: %v", got)
	}
	records, versions := model.export(request.Target)
	if len(records) != 6 || records[5].Row.Timestamp != fixture.QueryEnd ||
		!slices.Equal(versions, []string{fixture.Streams[0].PolicyVersion}) {
		t.Fatalf("unexpected reference export: %v, %v", records, versions)
	}
}
