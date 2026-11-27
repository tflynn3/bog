package streampolicy

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

// StreamPolicy describes independent ingest redaction and query output masking.
// The fixture does not promise output masking for exports. Even disabled masking
// has a version. Changing the effective rules always assigns a new version.
type StreamPolicy struct {
	Scope            Scope
	RedactionVersion string
	Redactions       []Replacement
	FailOn           string
	MaskingVersion   string
	Masks            []Replacement
}

// PolicyRevision is a full desired policy snapshot. Number is a fixture
// sequence, starting at one. An adapter can map it to native authority versions.
type PolicyRevision struct {
	Number  uint64
	Grants  []Grant
	Streams []StreamPolicy
}

type PolicyAck struct {
	Complete bool
	Number   uint64
}

// PolicyObserver applies a revision to the same target as the other observers.
// A complete acknowledgment attests that subsequent admissions use this
// revision, including after RestartAndReplay. Merely queuing a change or
// updating an adapter-side access filter is insufficient.
type PolicyObserver interface {
	ApplyPolicy(context.Context, PolicyRevision) (PolicyAck, error)
}

// ExportPublication observes the final create response and the IDs made
// retrievable by that attempt, including publication invisible to its caller.
// Observe publication at the target boundary, independently of the response.
type ExportPublication struct {
	Complete bool
	ExportStart
	Published []string
}

// ExportPublicationObserver starts a real authorized export. It synchronously
// calls beforePublish exactly once, after admission and before the final policy
// check and publication. The callback may update policy and must finish before
// the target can publish. The observer propagates callback errors and owns
// releasing or canceling its barrier on every return path. The callback must
// finish before the observer returns, including on cancellation.
type ExportPublicationObserver interface {
	ObserveExportPublication(ctx context.Context, target Target, beforePublish func(context.Context) error) (ExportPublication, error)
}

// RetryCommit describes one atomic persistence unit recovered from the target.
// Records and the retry disposition must be decoded from the same authoritative
// unit. Do not pair independent reads of the rows and retry index by inference.
type RetryCommit struct {
	Records          []Record
	Scope            Scope
	Principal, Key   string
	RedactionVersion string
	Outcome          Response
}

type RetryRecovery struct {
	Complete bool
	Commits  []RetryCommit // Complete list, in durable append order.
}

// RetryRecoveryObserver independently reads recovered persistence units after
// RestartAndReplay. Report incomplete evidence if the target cannot establish
// that a batch and its retry disposition share a durable commit boundary.
type RetryRecoveryObserver interface {
	ObserveRetryRecovery(context.Context) (RetryRecovery, error)
}

// RevisionAdapters share the fresh ReferenceFixture target required by
// Adapters. Policy updates, the publication barrier, and retry recovery must
// reach that target through its actual policy, export, and storage boundaries.
type RevisionAdapters struct {
	Adapters
	Policy      PolicyObserver
	Publication ExportPublicationObserver
	Recovery    RetryRecoveryObserver
}

type policyModel struct {
	current PolicyRevision
}

func newPolicyModel(f Fixture) policyModel {
	revision := PolicyRevision{Grants: slices.Clone(f.Grants)}
	for _, stream := range f.Streams {
		revision.Streams = append(revision.Streams, StreamPolicy{
			Scope: stream.Scope, RedactionVersion: stream.PolicyVersion,
			Redactions: slices.Clone(f.Replacements), FailOn: f.FailOn,
			MaskingVersion: "fixture-masking-v1",
		})
	}
	return policyModel{current: revision}
}

func (revision PolicyRevision) clone() PolicyRevision {
	cloned := PolicyRevision{Number: revision.Number, Grants: slices.Clone(revision.Grants)}
	for _, stream := range revision.Streams {
		stream.Redactions = slices.Clone(stream.Redactions)
		stream.Masks = slices.Clone(stream.Masks)
		cloned.Streams = append(cloned.Streams, stream)
	}
	return cloned
}

func (m policyModel) resolve(request Target, scope Scope) (Grant, StreamPolicy) {
	var grant Grant
	for _, candidate := range m.current.Grants {
		if candidate.Target == request {
			grant = candidate
			break
		}
	}
	for _, stream := range m.current.Streams {
		if stream.Scope == scope {
			return grant, stream
		}
	}
	return Grant{}, StreamPolicy{}
}

func replaceLine(line string, rules []Replacement) string {
	for _, rule := range rules {
		line = strings.ReplaceAll(line, rule.Forbidden, rule.Replacement)
	}
	return line
}

type revisionScenario struct {
	scenario
	policy      PolicyObserver
	publication ExportPublicationObserver
	recovery    RetryRecoveryObserver
	model       policyModel
}

func (s *revisionScenario) update(ctx context.Context, change func(*PolicyRevision)) error {
	next := s.model.current.clone()
	next.Number++
	if change != nil {
		change(&next)
	}
	// Give the adapter its own copy. Expectations remain verifier-owned.
	ack, err := s.policy.ApplyPolicy(ctx, next.clone())
	if err := observation(ctx, err); err != nil {
		return err
	}
	if !ack.Complete {
		return fmt.Errorf("%w: policy update lacks acknowledgment", ErrIncomplete)
	}
	if ack.Number != next.Number {
		return fmt.Errorf("%w: acknowledged policy revision differs from requested revision", ErrViolation)
	}
	s.model.current = next
	return nil
}

func setGrant(revision *PolicyRevision, target Target, change func(*Grant)) {
	for i := range revision.Grants {
		if revision.Grants[i].Target == target {
			change(&revision.Grants[i])
			return
		}
	}
	grant := Grant{Target: target}
	change(&grant)
	revision.Grants = append(revision.Grants, grant)
}

func (s *revisionScenario) setQuery(ctx context.Context, target Target, allowed bool) error {
	return s.update(ctx, func(revision *PolicyRevision) {
		setGrant(revision, target, func(grant *Grant) { grant.Query = allowed })
	})
}

// RunPolicyRevisions checks acknowledged policy changes across held queries,
// export publication and retrieval, and committed retries after restart.
func RunPolicyRevisions(ctx context.Context, adapters RevisionAdapters) error {
	if adapters.Query == nil || adapters.Retry == nil || adapters.Replay == nil || adapters.Export == nil ||
		adapters.Policy == nil || adapters.Publication == nil || adapters.Recovery == nil {
		return fmt.Errorf("%w: policy, publication, retry recovery, and all four base observers are required", ErrIncomplete)
	}
	base := newScenario(adapters.Adapters)
	s := revisionScenario{
		scenario: base, model: newPolicyModel(base.fixture),
		policy: adapters.Policy, publication: adapters.Publication, recovery: adapters.Recovery,
	}
	for _, step := range []struct {
		name string
		run  func(context.Context) error
	}{
		{"initial policy acknowledgment", func(ctx context.Context) error { return s.update(ctx, nil) }},
		{"mixed-stream seed and retry outcomes", s.checkIngest},
		{"query grants between pages", s.checkGrantRevisions},
		{"masking revisions between pages", s.checkMaskingRevisions},
		{"export publication and retrieval revocation", s.checkExportRevocation},
		{"retries across policy change and restart", s.checkRetryRevisions},
	} {
		if err := observation(ctx, nil); err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
		if err := step.run(ctx); err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
	}
	return observation(ctx, nil)
}
