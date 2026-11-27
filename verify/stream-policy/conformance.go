package streampolicy

import (
	"context"
	"encoding/hex"
	"fmt"
)

// PolicyObserver applies changes to the trusted policy authority and returns
// only after the change governs new admissions. These are fixture controls,
// never policy fields on a data-plane request.
type PolicyObserver interface {
	SetGrant(context.Context, Grant) error
	SetMasking(context.Context, MaskingChange) error
	SetRedaction(context.Context, RedactionChange) error
}

type MaskingChange struct {
	Scope       Scope
	Revision    string
	Replacement Replacement // Zero value disables masking.
}

type RedactionChange struct {
	Scope        Scope
	Version      string
	Replacements []Replacement
}

type CommitBoundary string

const (
	BeforeCommit CommitBoundary = "prepared-before-durable-commit"
	AfterCommit  CommitBoundary = "durable-before-response"
)

// InterruptedAppend attests to an observed boundary and a stopped target.
// ResponseStarted records whether any application response reached the client.
type InterruptedAppend struct {
	Boundary        CommitBoundary
	Stopped         bool
	ResponseStarted bool
}

// RetryRecord is the observable committed retry outcome. These fields come
// from independently decoded committed authority, never a retry request.
type RetryRecord struct {
	Scope         Scope
	Principal     string
	Key           string
	PolicyVersion string
	Status        int
}

// CommitEvidence describes a single complete view of committed rows and retry
// outcomes. Both must come from the same durable boundary. A nil RetryRecords
// means retry evidence was unavailable.
type CommitEvidence struct {
	Committed
	RetryRecords []RetryRecord
}

// CommitObserver provides bounded fixture fault injection and independent
// inspection. InterruptAppend must submit exactly once, stop the target at
// the observed boundary, and leave its durable directory intact. Reopen must
// start a new target using that directory, with no inherited in-memory rows
// or retry index, and route subsequent requests to it. Inspect works on the
// running target and must not restart it.
type CommitObserver interface {
	InterruptAppend(context.Context, IngestRequest, CommitBoundary) (InterruptedAppend, error)
	Reopen(context.Context) (CommitEvidence, error)
	Inspect(context.Context) (CommitEvidence, error)
}

// ConformanceAdapters share the target described by Adapters. Ingest must
// additionally support two overlapping requests during the same-key probe.
type ConformanceAdapters struct {
	Adapters
	Policy PolicyObserver
	Commit CommitObserver
}

// Report keeps synthetic results and candidate-specific qualification distinct.
// A zero value records that neither gate has passed.
type Report struct {
	SyntheticPassed bool
	Qualified       *Candidate
}

// Candidate binds qualification to a shipped binary. CandidateObserver checks
// the process serving the fixture against this identity, including after a
// restart. Release orchestration owns the binary and the native runner.
type Candidate struct {
	Revision     string
	BinarySHA256 string
}

type CandidateObserver interface {
	ObserveCandidate(context.Context) (Candidate, error)
}

// RunConformance exercises the extended reference scenario with synthetic
// adapters. Use a disposable ReferenceFixture target and a deadline.
func RunConformance(ctx context.Context, adapters ConformanceAdapters) (Report, error) {
	if err := runConformance(ctx, adapters, nil); err != nil {
		return Report{}, err
	}
	return Report{SyntheticPassed: true}, nil
}

// QualifyShippedAdapter runs the same checks against a candidate. It requires
// independent process identity observations at startup, after reopen, and
// at completion. The synthetic result is recorded only by RunConformance.
func QualifyShippedAdapter(ctx context.Context, adapters ConformanceAdapters, want Candidate, observer CandidateObserver) (Report, error) {
	if observer == nil || !validHex(want.Revision, 20) || !validHex(want.BinarySHA256, 32) {
		return Report{}, fmt.Errorf("%w: candidate identity required", ErrIncomplete)
	}
	check := func(ctx context.Context) error {
		got, err := observer.ObserveCandidate(ctx)
		if err := observation(ctx, err); err != nil {
			return err
		}
		if got != want {
			return fmt.Errorf("%w: candidate process identity differs", ErrViolation)
		}
		return nil
	}
	if err := runConformance(ctx, adapters, check); err != nil {
		return Report{}, err
	}
	return Report{Qualified: &want}, nil
}

func validHex(s string, size int) bool {
	if len(s) != 2*size {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

type conformance struct {
	*scenario
	policy   PolicyObserver
	commit   CommitObserver
	identity func(context.Context) error
	model    referenceModel
}

func runConformance(ctx context.Context, adapters ConformanceAdapters, identity func(context.Context) error) error {
	if adapters.Policy == nil || adapters.Commit == nil {
		return fmt.Errorf("%w: policy and commit observers required", ErrIncomplete)
	}
	c := conformance{policy: adapters.Policy, commit: adapters.Commit, identity: identity}
	if err := c.checkIdentity(ctx); err != nil {
		return err
	}
	// Preserve the static matrix and check process identity after its reopens.
	base, err := runReference(ctx, adapters.Adapters, identity)
	if err != nil {
		return err
	}
	c.scenario = base
	c.model = newReferenceModel(base.fixture, base.appends)
	c.forbidden = append(c.forbidden, rawBackfill, maskInputSecret, rawInterrupted)
	c.denialMarkers = append(c.rejectionMarkers(), backfillKey, rollbackKey, interruptedKey, changedRedactionVersion,
		freshPolicyKey, beforeCommitMarker, afterCommitMarker, freshPolicyMarker)
	for _, step := range []struct {
		name string
		run  func(context.Context) error
	}{
		{"interleaved snapshot query and export", c.checkInterleaved},
		{"policy changes between pages", c.checkPagePolicy},
		{"atomic append and retry recovery", c.checkAtomicRetry},
		{"candidate identity", c.checkIdentity},
	} {
		if err := observation(ctx, nil); err != nil {
			return err
		}
		if err := step.run(ctx); err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
	}
	return observation(ctx, nil)
}

func (c *conformance) checkIdentity(ctx context.Context) error {
	if err := observation(ctx, nil); err != nil {
		return err
	}
	if c.identity != nil {
		return c.identity(ctx)
	}
	return nil
}
