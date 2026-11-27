package streampolicy

import (
	"context"
	"fmt"
	"net/http"
)

func (s *revisionScenario) exportAtBarrier(ctx context.Context, target Target, change func(context.Context) error) (ExportPublication, error) {
	calls := 0
	var callbackErr error
	result, err := s.publication.ObserveExportPublication(ctx, target, func(callbackCtx context.Context) error {
		calls++
		if calls != 1 {
			callbackErr = fmt.Errorf("%w: publication barrier ran more than once", ErrIncomplete)
		} else {
			callbackErr = change(callbackCtx)
		}
		return callbackErr
	})
	if callbackErr != nil {
		return ExportPublication{}, callbackErr
	}
	if err := observation(ctx, err); err != nil {
		return ExportPublication{}, err
	}
	if !result.Complete || calls != 1 {
		return ExportPublication{}, fmt.Errorf("%w: export lacks admission or publication evidence", ErrIncomplete)
	}
	return result, nil
}

func (s *revisionScenario) checkExportRevocation(ctx context.Context) error {
	a, b := s.fixture.Streams[0], s.fixture.Streams[1]
	owner := matrixTarget{grant: Grant{Target: Target{exporter, a.Selector}}, stream: a}
	other := matrixTarget{grant: Grant{Target: Target{bob, b.Selector}}, stream: b}
	open, err := s.exportAtBarrier(ctx, owner.grant.Target, func(ctx context.Context) error { return observation(ctx, nil) })
	if err != nil {
		return fmt.Errorf("authorized publication control: %w", err)
	}
	if err := requireResponse(open.Response, anySuccessStatus); err != nil {
		return err
	}
	id := open.ArtifactID
	if id == "" {
		return fmt.Errorf("%w: authorized publication lacks an artifact ID", ErrIncomplete)
	}
	if len(id) > MaxEvidenceBytes {
		return ErrIncomplete
	}
	if len(open.Published) != 1 || open.Published[0] != id || id == missingArtifact {
		return fmt.Errorf("%w: publication differs from acknowledged artifact", ErrViolation)
	}
	if err := inspectResponse(open.Response, s.scopedMarkers(a.Scope)); err != nil {
		return err
	}
	if err := checkMarkers([]byte(id), s.scopedMarkers(a.Scope)); err != nil {
		return err
	}
	if err := s.checkArtifact(ctx, owner, id); err != nil {
		return fmt.Errorf("authorized scoped artifact: %w", err)
	}
	otherID, err := s.createArtifact(ctx, other)
	if err != nil {
		return err
	}
	if otherID == id || otherID == missingArtifact {
		return fmt.Errorf("%w: export reused an ID across scopes or for an absent artifact", ErrViolation)
	}
	if err := s.checkArtifact(ctx, other, otherID); err != nil {
		return fmt.Errorf("independent scoped artifact: %w", err)
	}

	refs := []string{id, otherID, missingArtifact}
	createDenial := rejection{status: http.StatusForbidden}
	ungranted := Target{bob, a.Selector}
	if err := s.deniedCreate(ctx, ungranted, &createDenial, refs); err != nil {
		return err
	}
	downloadDenial := rejection{status: http.StatusForbidden}
	for _, target := range []Target{ungranted, other.grant.Target} {
		if err := s.deniedDownload(ctx, target, id, &downloadDenial, refs); err != nil {
			return fmt.Errorf("cross-scope download control: %w", err)
		}
	}
	aborted, err := s.exportAtBarrier(ctx, owner.grant.Target, func(ctx context.Context) error {
		return s.update(ctx, func(revision *PolicyRevision) {
			setGrant(revision, owner.grant.Target, func(grant *Grant) { grant.Export = false })
		})
	})
	if err != nil {
		return fmt.Errorf("revocation at publication barrier: %w", err)
	}
	if aborted.ArtifactID != "" || len(aborted.Published) != 0 {
		return fmt.Errorf("%w: export revoked before publication disclosed or published an artifact", ErrViolation)
	}
	if err := s.checkRejection(aborted.Response, &createDenial, refs); err != nil {
		return fmt.Errorf("revoked publication must return uniform denial: %w", err)
	}
	if err := s.deniedCreate(ctx, owner.grant.Target, &createDenial, refs); err != nil {
		return fmt.Errorf("revoked create must deny: %w", err)
	}
	for _, probe := range []string{id, otherID, missingArtifact} {
		if err := s.deniedDownload(ctx, owner.grant.Target, probe, &downloadDenial, refs); err != nil {
			return fmt.Errorf("revoked retrieval must return uniform denial: %w", err)
		}
	}
	if err := s.checkArtifact(ctx, other, otherID); err != nil {
		return fmt.Errorf("other scope must retain export access: %w", err)
	}
	if err := s.update(ctx, func(revision *PolicyRevision) {
		setGrant(revision, owner.grant.Target, func(grant *Grant) { grant.Export = true })
	}); err != nil {
		return err
	}
	return s.checkArtifact(ctx, owner, id)
}
