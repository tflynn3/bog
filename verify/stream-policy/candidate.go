package streampolicy

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

// CandidateOperation identifies the candidate route to bind.
type CandidateOperation string

const (
	CandidateIngest       CandidateOperation = "ingest"
	CandidateQuery        CandidateOperation = "query"
	CandidateCreateExport CandidateOperation = "create export"
	CandidateFetchExport  CandidateOperation = "fetch export"
)

// CandidateRequest carries fixture inputs to the candidate's public interface.
// Query is populated only for CandidateQuery, and ArtifactID only for
// CandidateFetchExport. The adapter installs the exact body for CandidateIngest.
type CandidateRequest struct {
	Operation  CandidateOperation
	Target     Target
	Query      QueryRequest
	ArtifactID string
}

// CandidateEvidence supplies the candidate's native durable evidence. These
// calls are serial and must honor the context, including read and close.
type CandidateEvidence interface {
	// OpenCommitted opens the exact committed byte range of the disposable
	// target while it is serving. Determine the range from its authoritative
	// commit metadata. Include all committed batches in order, without staging,
	// rebuilding, filtering, or reading beyond the committed boundary.
	OpenCommitted(context.Context) (io.ReadCloser, error)

	// DecodeCommitted independently validates the native framing, integrity,
	// and record format across the supplied bytes. Decode every record in
	// append order, including one that disagrees with policy. Use an
	// evidence-side decoder, independent of the server and its indexes.
	DecodeCommitted(context.Context, []byte) ([]Record, error)

	// Restart reopens the same durable state with the same policy authority.
	// Return after readiness, and arrange for subsequent Request bindings to
	// reach the reopened target. The caller owns process cleanup.
	Restart(context.Context) error
}

// Candidate binds one running, exclusively owned target provisioned with
// ReferenceFixture's principals and stream identities. Bind credentials and
// selectors using the candidate's native protocol. Policy updates must go to
// the candidate's policy authority. None of the hooks should implement policy
// or synthesize request outcomes on the verifier side.
type Candidate struct {
	// Request binds native routes, headers and query parameters. An empty
	// principal means no credentials. Ingest requests are unkeyed and the
	// binding must leave their body unset. Return an error for an unavailable
	// route.
	Request  func(context.Context, CandidateRequest) (*http.Request, error)
	Policy   PolicyObserver
	Evidence CandidateEvidence

	// ExportID extracts any artifact reference exposed by the application,
	// including on denial. Return an empty ID when none was exposed.
	ExportID func(Response) (string, error)

	// VerifyArtifact validates the original artifact body independently,
	// including its versioned manifest authentication and payload integrity.
	// It returns all decoded records in durable append order.
	VerifyArtifact func(context.Context, []byte) (Manifest, []Record, error)
}

// RunCandidate exercises server-side grants, scope binding, and redaction on a
// fresh target. A deadline bounds network, policy, and storage observations.
// The caller owns provisioning and cleanup.
func RunCandidate(ctx context.Context, candidate Candidate) error {
	if err := observation(ctx, nil); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		return fmt.Errorf("%w: candidate run requires a context deadline", ErrIncomplete)
	}
	if candidate.Request == nil || candidate.Policy == nil || candidate.Evidence == nil ||
		candidate.ExportID == nil || candidate.VerifyArtifact == nil {
		return fmt.Errorf("%w: candidate requires HTTP, policy, committed evidence, and export bindings", ErrIncomplete)
	}

	target := newCandidateAdapter(candidate)
	defer target.client.CloseIdleConnections()
	base := newScenario(Adapters{Query: target, Retry: target, Replay: target, Export: target})
	s := revisionScenario{scenario: base, policy: candidate.Policy, model: newPolicyModel(base.fixture)}
	for _, step := range []struct {
		name string
		run  func(context.Context) error
	}{
		{"initial policy acknowledgment", func(ctx context.Context) error { return s.update(ctx, nil) }},
		{"unauthenticated operations", s.checkCandidateAuthentication},
		{"ingest grants and pre-append redaction", s.checkCandidateIngest},
		{"authorization and cursor scope", s.checkQueries},
		{"query grant revocation and regrant", s.checkGrantRevisions},
		{"committed bytes and restart", s.checkCandidateReplay},
		{"scoped exports after restart", s.checkExports},
		{"export grant revocation and regrant", s.checkCandidateExportGrant},
		{"final committed bytes", func(ctx context.Context) error {
			return target.checkCommitted(ctx, s.committed, s.forbidden)
		}},
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

func (s *revisionScenario) checkCandidateAuthentication(ctx context.Context) error {
	target := Target{Selector: s.fixture.Streams[0].Selector}
	unauthenticated := rejection{status: http.StatusUnauthorized}
	request := IngestRequest{Target: target, Body: ndjson("2026-01-01T00:00:05Z", uncommitted)}
	if _, err := s.ingest(ctx, request, unauthenticated.status); err != nil {
		return fmt.Errorf("ingest: %w", err)
	}
	page, err := s.query(ctx, QueryRequest{Target: target, Limit: 1}, unauthenticated.status)
	if err == nil {
		err = inspectResponse(page.Response, s.denialMarkers)
	}
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	if err := s.deniedCreate(ctx, target, &unauthenticated, nil); err != nil {
		return fmt.Errorf("export create: %w", err)
	}
	unauthenticated = rejection{status: http.StatusUnauthorized}
	if err := s.deniedDownload(ctx, target, missingArtifact, &unauthenticated, []string{missingArtifact}); err != nil {
		return fmt.Errorf("export retrieval: %w", err)
	}
	return nil
}

func (s *revisionScenario) checkCandidateIngest(ctx context.Context) error {
	for i, entry := range s.appends {
		request := entry.request
		request.Key = ""
		if _, err := s.ingest(ctx, request, http.StatusNoContent); err != nil {
			return fmt.Errorf("authorized batch %d: %w", i+1, err)
		}
	}

	denied := rejection{status: http.StatusForbidden}
	for _, target := range s.targets {
		if target.grant.Ingest {
			continue
		}
		request := IngestRequest{Target: target.grant.Target, Body: ndjson("2026-01-01T00:00:05Z", uncommitted)}
		got, err := s.Retry.Ingest(ctx, request)
		if err := observation(ctx, err); err != nil {
			return err
		}
		if err := s.checkRejection(got, &denied, nil); err != nil {
			return fmt.Errorf("denied ingest (%s): %w", target.label(), err)
		}
	}

	request := s.appends[0].request
	request.Key = ""
	const ts = "2026-01-01T00:00:06Z"
	request.Body = ndjson(ts, uncommitted+" "+plain) + ndjson(ts, failed)
	if _, err := s.ingest(ctx, request, http.StatusServiceUnavailable); err != nil {
		return fmt.Errorf("failed redaction must leave the whole batch uncommitted: %w", err)
	}
	return nil
}

func (s *revisionScenario) checkCandidateReplay(ctx context.Context) error {
	owner := Target{alice, s.fixture.Streams[0].Selector}
	held, err := s.open(ctx, owner)
	if err != nil {
		return err
	}
	if err := s.checkReplay(ctx); err != nil {
		return err
	}
	modified := modifyToken(held.token, []string{held.token})
	refs := []string{held.token, modified}
	invalid := rejection{status: http.StatusBadRequest}
	if err := s.rejectRevisionQueries(ctx, owner, &invalid, refs, []string{modified, held.token}); err != nil {
		return fmt.Errorf("previous process cursor: %w", err)
	}
	return s.checkFullQueries(ctx)
}

func (s *revisionScenario) checkCandidateExportGrant(ctx context.Context) error {
	stream := s.fixture.Streams[0]
	target := Target{exporter, stream.Selector}
	owner := matrixTarget{grant: Grant{Target: target}, stream: stream}
	id, err := s.createArtifact(ctx, owner)
	if err != nil {
		return err
	}
	if id == missingArtifact {
		return fmt.Errorf("%w: absent artifact probe names a real artifact", ErrIncomplete)
	}
	if err := s.checkArtifact(ctx, owner, id); err != nil {
		return err
	}

	refs := []string{id, missingArtifact}
	createDenial := rejection{status: http.StatusForbidden}
	downloadDenial := rejection{status: http.StatusForbidden}
	withoutGrant := Target{bob, stream.Selector}
	if err := s.deniedCreate(ctx, withoutGrant, &createDenial, refs); err != nil {
		return err
	}
	if err := s.deniedDownload(ctx, withoutGrant, id, &downloadDenial, refs); err != nil {
		return err
	}
	change := func(allowed bool) error {
		return s.update(ctx, func(revision *PolicyRevision) {
			setGrant(revision, target, func(grant *Grant) { grant.Export = allowed })
		})
	}
	if err := change(false); err != nil {
		return err
	}
	if err := s.deniedCreate(ctx, target, &createDenial, refs); err != nil {
		return fmt.Errorf("revoked create: %w", err)
	}
	for _, probe := range refs {
		if err := s.deniedDownload(ctx, target, probe, &downloadDenial, refs); err != nil {
			return fmt.Errorf("revoked retrieval: %w", err)
		}
	}
	if err := change(true); err != nil {
		return err
	}
	return s.checkArtifact(ctx, owner, id)
}
