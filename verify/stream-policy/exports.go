package streampolicy

import (
	"context"
	"fmt"
	"net/http"
	"slices"
)

func (s *scenario) checkExports(ctx context.Context) error {
	type scopedArtifact struct {
		id    string
		scope Scope
	}
	const missing = "fixture-missing-artifact"
	artifacts := make([]scopedArtifact, 0, len(s.fixture.Streams))
	markers := []string{missing}
	// Keep a positive control in each scope, including the export-only grant.
	creators := [...]string{exporter, bob, alice}
	for i, stream := range s.fixture.Streams {
		target := Target{creators[i], stream.Selector}
		id, err := s.checkExport(ctx, target, stream)
		if err != nil {
			return fmt.Errorf("authorized export in stream %d: %w", i+1, err)
		}
		if id == missing {
			return fmt.Errorf("%w: missing-artifact probe does not name an absent artifact", ErrIncomplete)
		}
		artifacts = append(artifacts, scopedArtifact{id, stream.Scope})
		markers = append(markers, id)
	}

	exportGrants := make(map[Target]bool, len(s.fixture.Grants))
	var principals []string
	for _, grant := range s.fixture.Grants {
		exportGrants[grant.Target] = grant.Export
		if !slices.Contains(principals, grant.Principal) {
			principals = append(principals, grant.Principal)
		}
	}
	streams := append(slices.Clone(s.fixture.Streams[:]), Stream{Selector: unknown})
	var createBaseline, downloadBaseline Response
	for p, principal := range principals {
		for j, stream := range streams {
			target := Target{principal, stream.Selector}
			allowed := exportGrants[target]
			phase := "artifact requested in another authorized stream"
			if !allowed {
				phase = "artifact requested without an export grant"
				if err := s.deniedCreate(ctx, target, &createBaseline, markers); err != nil {
					return fmt.Errorf("denied export create by principal %d through selector %d: %w", p+1, j+1, err)
				}
			}

			for i, artifact := range artifacts {
				if allowed && stream.Scope == artifact.scope {
					continue
				}
				for _, id := range []string{artifact.id, missing} {
					if err := s.deniedDownload(ctx, target, id, &downloadBaseline, markers); err != nil {
						return fmt.Errorf("%s (source stream %d, principal %d, selector %d): %w", phase, i+1, p+1, j+1, err)
					}
				}
			}
		}
	}
	return nil
}

func (s *scenario) deniedCreate(ctx context.Context, target Target, baseline *Response, markers []string) error {
	start, err := s.Export.CreateExport(ctx, target)
	if err := observation(ctx, err); err != nil {
		return err
	}
	if err := requireResponse(start.Response, http.StatusForbidden); err != nil {
		return err
	}
	if start.ArtifactID != "" {
		return fmt.Errorf("%w: rejected export disclosed an artifact ID", ErrViolation)
	}
	return s.compareExportDenial(start.Response, baseline, markers)
}

func (s *scenario) deniedDownload(ctx context.Context, target Target, id string, baseline *Response, markers []string) error {
	artifact, err := s.Export.FetchExport(ctx, target, id)
	if err := observation(ctx, err); err != nil {
		return err
	}
	if err := requireResponse(artifact.Response, http.StatusForbidden); err != nil {
		return err
	}
	if len(artifact.Bytes) != 0 || len(artifact.Records) != 0 || artifact.Verified ||
		artifact.Manifest.Scope != (Scope{}) || artifact.Manifest.Version != "" || len(artifact.Manifest.PolicyVersions) != 0 {
		return fmt.Errorf("%w: rejected download disclosed artifact evidence", ErrViolation)
	}
	return s.compareExportDenial(artifact.Response, baseline, markers)
}

func (s *scenario) compareExportDenial(response Response, baseline *Response, markers []string) error {
	if !baseline.Complete {
		*baseline = response
	}
	return s.deniedResponse(response, *baseline, markers...)
}

func (s *scenario) checkExport(ctx context.Context, target Target, stream Stream) (string, error) {
	start, err := s.Export.CreateExport(ctx, target)
	if err := observation(ctx, err); err != nil {
		return "", err
	}
	forbidden := s.scopedMarkers(stream.Scope)
	if err := requireResponse(start.Response, anySuccessStatus); err != nil {
		return "", err
	}
	if err := inspectResponse(start.Response, forbidden); err != nil {
		return "", err
	}
	if start.ArtifactID == "" {
		return "", fmt.Errorf("%w: authorized export has no artifact ID", ErrIncomplete)
	}
	if len(start.ArtifactID) > MaxEvidenceBytes {
		return "", ErrIncomplete
	}
	if err := checkMarkers([]byte(start.ArtifactID), forbidden); err != nil {
		return "", err
	}
	artifact, err := s.Export.FetchExport(ctx, target, start.ArtifactID)
	if err := observation(ctx, err); err != nil {
		return "", err
	}
	want, _ := s.forScope(stream.Scope)
	if err := requireResponse(artifact.Response, anySuccessStatus); err != nil {
		return "", err
	}
	if err := inspectResponse(artifact.Response, forbidden); err != nil {
		return "", err
	}
	manifest := artifact.Manifest
	if !artifact.Verified || manifest.Version == "" || manifest.Scope == (Scope{}) ||
		len(manifest.PolicyVersions) == 0 || artifact.Records == nil {
		return "", ErrIncomplete
	}
	if manifest.Scope != stream.Scope || !slices.Equal(manifest.PolicyVersions, []string{stream.PolicyVersion}) {
		return "", fmt.Errorf("%w: export manifest scope or policy version differs", ErrViolation)
	}
	return start.ArtifactID, inspectRecords(artifact.Bytes, artifact.Records, want, forbidden)
}
