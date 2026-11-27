package streampolicy

import (
	"context"
	"fmt"
	"net/http"
	"slices"
)

func (s *scenario) checkExports(ctx context.Context) error {
	a, b, c := s.fixture.Streams[0], s.fixture.Streams[1], s.fixture.Streams[2]
	const missing = "fixture-missing-artifact"
	var known string
	for i, stream := range s.fixture.Streams {
		target := Target{alice, stream.Selector}
		if stream == a {
			target.Principal = exporter
		} else if stream == b {
			target.Principal = bob
		}
		id, err := s.checkExport(ctx, target, stream)
		if err != nil {
			return fmt.Errorf("authorized export in stream %d: %w", i+1, err)
		}
		if id == missing {
			return fmt.Errorf("%w: missing-artifact probe does not name an absent artifact", ErrIncomplete)
		}
		if stream == a {
			known = id
		}
	}
	markers := []string{known, missing}

	probes := []struct {
		target        Target
		phase         string
		createAllowed bool
	}{
		{target: Target{bob, a.Selector}, phase: "artifact requested without an export grant"},
		{target: Target{bob, c.Selector}, phase: "artifact requested in another tenant"},
		{target: Target{bob, unknown}, phase: "artifact requested through an unknown selector"},
		{target: Target{bob, b.Selector}, phase: "artifact requested in another authorized stream", createAllowed: true},
	}
	var baseline Response
	for _, probe := range probes {
		if probe.createAllowed {
			continue
		}
		start, err := s.Export.CreateExport(ctx, probe.target)
		if err := observation(ctx, err); err != nil {
			return err
		}
		if err := requireResponse(start.Response, http.StatusForbidden); err != nil {
			return err
		}
		if start.ArtifactID != "" {
			return fmt.Errorf("%w: rejected export disclosed an artifact ID", ErrViolation)
		}
		if !baseline.Complete {
			baseline = start.Response
		}
		if err := s.deniedResponse(start.Response, baseline, markers...); err != nil {
			return err
		}
	}

	// Pair the existing artifact with an absent one under every selector,
	// including the selector where Bob has an export grant.
	baseline = Response{}
	for _, probe := range probes {
		for _, id := range []string{known, missing} {
			resp, err := s.deniedDownload(ctx, probe.target, id)
			if err != nil {
				return fmt.Errorf("%s: %w", probe.phase, err)
			}
			if !baseline.Complete {
				baseline = resp
			}
			if err := s.deniedResponse(resp, baseline, markers...); err != nil {
				return fmt.Errorf("%s: %w", probe.phase, err)
			}
		}
	}
	return nil
}

func (s *scenario) deniedDownload(ctx context.Context, target Target, id string) (Response, error) {
	artifact, err := s.Export.FetchExport(ctx, target, id)
	if err := observation(ctx, err); err != nil {
		return Response{}, err
	}
	if err := requireResponse(artifact.Response, http.StatusForbidden); err != nil {
		return Response{}, err
	}
	if len(artifact.Bytes) != 0 || len(artifact.Records) != 0 || artifact.Verified ||
		artifact.Manifest.Scope != (Scope{}) || artifact.Manifest.Version != "" || len(artifact.Manifest.PolicyVersions) != 0 {
		return Response{}, fmt.Errorf("%w: rejected download disclosed artifact evidence", ErrViolation)
	}
	return artifact.Response, nil
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
