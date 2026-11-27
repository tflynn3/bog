package streampolicy

import (
	"context"
	"fmt"
	"net/http"
	"slices"
)

const missingArtifact = "fixture-missing-artifact"

type scopedArtifact struct {
	id     string
	source matrixTarget
}

func (s *scenario) createArtifacts(ctx context.Context) ([]scopedArtifact, []string, error) {
	var artifacts []scopedArtifact
	references := []string{missingArtifact}
	scopes := make(map[string]Scope)
	for _, target := range s.targets {
		if !target.grant.Export {
			continue
		}
		id, err := s.createArtifact(ctx, target)
		if err != nil {
			return nil, nil, fmt.Errorf("authorized export in stream %d (principal %d): %w", target.selectorIndex, target.principalIndex, err)
		}
		if id == missingArtifact {
			return nil, nil, fmt.Errorf("%w: missing-artifact probe does not name an absent artifact", ErrIncomplete)
		}
		if previous, ok := scopes[id]; ok {
			// A content-addressed artifact can be reused within the same stream.
			if previous != target.stream.Scope {
				return nil, nil, fmt.Errorf("%w: artifact ID reused across streams", ErrViolation)
			}
			continue
		}
		scopes[id] = target.stream.Scope
		artifacts = append(artifacts, scopedArtifact{id, target})
		references = append(references, id)
	}

	return artifacts, references, nil
}

func (s *scenario) checkExports(ctx context.Context) error {
	return s.checkExportMatrix(ctx, s.checkArtifact)
}

type artifactCheck func(context.Context, matrixTarget, string) error

func (s *scenario) checkExportMatrix(ctx context.Context, check artifactCheck) error {
	artifacts, references, err := s.createArtifacts(ctx)
	if err != nil {
		return err
	}
	createDenial := rejection{status: http.StatusForbidden}
	downloadDenial := rejection{status: http.StatusForbidden}
	for _, target := range s.targets {
		if !target.grant.Export {
			if err := s.deniedCreate(ctx, target.grant.Target, &createDenial, references); err != nil {
				return fmt.Errorf("denied export create (%s): %w", target.label(), err)
			}
		}
		for _, artifact := range artifacts {
			if err := s.checkDownloadPair(ctx, target, artifact, &downloadDenial, references, check); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *scenario) checkDownloadPair(ctx context.Context, target matrixTarget, artifact scopedArtifact, denial *rejection, references []string, check artifactCheck) error {
	phase := "artifact requested without an export grant"
	if target.grant.Export {
		phase = "artifact requested in another authorized stream"
		if target.stream.Scope == artifact.source.stream.Scope {
			if err := check(ctx, target, artifact.id); err != nil {
				return fmt.Errorf("authorized export download in stream %d (principal %d, source principal %d): %w",
					target.selectorIndex, target.principalIndex, artifact.source.principalIndex, err)
			}
			phase = "absent artifact requested in its paired authorized stream"
		}
	}
	if !target.grant.Export || target.stream.Scope != artifact.source.stream.Scope {
		if err := s.deniedDownload(ctx, target.grant.Target, artifact.id, denial, references); err != nil {
			return fmt.Errorf("%s (source %s, target %s, existing ID): %w", phase, artifact.source.label(), target.label(), err)
		}
	}
	if err := s.deniedDownload(ctx, target.grant.Target, missingArtifact, denial, references); err != nil {
		return fmt.Errorf("%s (source %s, target %s, absent ID): %w", phase, artifact.source.label(), target.label(), err)
	}
	return nil
}

func (s *scenario) deniedCreate(ctx context.Context, target Target, denial *rejection, references []string) error {
	start, err := s.Export.CreateExport(ctx, target)
	if err := observation(ctx, err); err != nil {
		return err
	}
	if start.ArtifactID != "" {
		return fmt.Errorf("%w: rejected export disclosed an artifact ID", ErrViolation)
	}
	return s.checkRejection(start.Response, denial, references)
}

func (s *scenario) deniedDownload(ctx context.Context, target Target, id string, denial *rejection, references []string) error {
	artifact, err := s.Export.FetchExport(ctx, target, id)
	if err := observation(ctx, err); err != nil {
		return err
	}
	if err := requireResponse(artifact.Response, denial.status); err != nil {
		return err
	}
	if len(artifact.Bytes) != 0 || len(artifact.Records) != 0 || artifact.Verified ||
		artifact.Manifest.Scope != (Scope{}) || artifact.Manifest.Version != "" || len(artifact.Manifest.PolicyVersions) != 0 {
		return fmt.Errorf("%w: rejected download disclosed artifact evidence", ErrViolation)
	}
	return s.checkRejection(artifact.Response, denial, references)
}

func (s *scenario) createArtifact(ctx context.Context, target matrixTarget) (string, error) {
	start, err := s.Export.CreateExport(ctx, target.grant.Target)
	if err := observation(ctx, err); err != nil {
		return "", err
	}
	forbidden := s.scopedMarkers(target.stream.Scope)
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
	return start.ArtifactID, nil
}

func (s *scenario) checkArtifact(ctx context.Context, target matrixTarget, id string) error {
	want, _ := s.forScope(target.stream.Scope)
	return s.checkArtifactSnapshot(ctx, target, id, want, []string{target.stream.PolicyVersion}, nil)
}

func (s *scenario) checkArtifactSnapshot(ctx context.Context, target matrixTarget, id string, want []Record, versions, extraForbidden []string) error {
	artifact, err := s.Export.FetchExport(ctx, target.grant.Target, id)
	if err := observation(ctx, err); err != nil {
		return err
	}
	if err := requireResponse(artifact.Response, anySuccessStatus); err != nil {
		return err
	}
	forbidden := append(s.scopedMarkers(target.stream.Scope), key)
	forbidden = append(forbidden, extraForbidden...)
	if err := inspectResponse(artifact.Response, forbidden); err != nil {
		return err
	}
	manifest := artifact.Manifest
	if !artifact.Verified || manifest.Version == "" || manifest.Scope == (Scope{}) ||
		len(manifest.PolicyVersions) == 0 || artifact.Records == nil {
		return ErrIncomplete
	}
	if manifest.Scope != target.stream.Scope || len(manifest.PolicyVersions) != len(versions) ||
		!slices.Equal(slices.Sorted(slices.Values(manifest.PolicyVersions)), versions) {
		return fmt.Errorf("%w: export manifest scope or policy version differs", ErrViolation)
	}
	if err := checkMarkers([]byte(manifest.Version), forbidden); err != nil {
		return err
	}
	return inspectRecords(artifact.Bytes, artifact.Records, want, forbidden)
}
