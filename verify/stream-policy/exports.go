package streampolicy

import (
	"context"
	"crypto/sha256"
	"fmt"
	"maps"
	"net/http"
	"slices"
)

type scopedArtifact struct {
	id     string
	stream Stream
	digest [sha256.Size]byte
}

func (s *scenario) prepareExports(ctx context.Context) ([]scopedArtifact, ExportState, error) {
	empty, err := s.observeExports(ctx)
	if err != nil {
		return nil, ExportState{}, err
	}
	if len(empty.Artifacts) != 0 || empty.Pending != 0 {
		return nil, ExportState{}, fmt.Errorf("%w: target has preexisting export state", ErrIncomplete)
	}

	artifacts := make([]scopedArtifact, 0, len(s.fixture.Streams))
	expected := make(map[string][sha256.Size]byte, len(s.fixture.Streams))
	for i, stream := range s.fixture.Streams {
		cell, ok := exportControl(s.matrix, stream)
		if !ok {
			return nil, ExportState{}, fmt.Errorf("%w: stream %d has no export control", ErrIncomplete, i+1)
		}
		artifact, err := s.createControl(ctx, cell)
		if err != nil {
			return nil, ExportState{}, fmt.Errorf("authorized export in stream %d: %w", i+1, err)
		}
		if artifact.id == missingArtifact {
			return nil, ExportState{}, fmt.Errorf("%w: missing-artifact probe does not name an absent artifact", ErrIncomplete)
		}
		if _, exists := expected[artifact.id]; exists {
			return nil, ExportState{}, fmt.Errorf("%w: different scopes returned the same artifact ID", ErrViolation)
		}
		artifacts = append(artifacts, artifact)
		expected[artifact.id] = artifact.digest
	}

	before, err := s.observeExports(ctx)
	if err != nil {
		return nil, ExportState{}, err
	}
	if before.Pending != 0 || !maps.Equal(before.Artifacts, expected) {
		return nil, ExportState{}, fmt.Errorf("%w: export inventory differs from authorized controls", ErrViolation)
	}
	return artifacts, before, nil
}

func exportControl(matrix []policyCell, stream Stream) (policyCell, bool) {
	var control policyCell
	for _, cell := range matrix {
		if cell.stream == stream && cell.Export {
			control = cell
			// Use a least-privilege creation control where the fixture has one.
			if !cell.Query && !cell.Ingest {
				break
			}
		}
	}
	return control, control.Export
}

func (s *scenario) observeExports(ctx context.Context) (ExportState, error) {
	state, err := s.exportState.ObserveExportState(ctx)
	if err := observation(ctx, err); err != nil {
		return ExportState{}, err
	}
	if !state.Complete || state.Pending < 0 || state.Artifacts == nil {
		return ExportState{}, fmt.Errorf("%w: export inventory unavailable", ErrIncomplete)
	}
	size := 0
	for id := range state.Artifacts {
		const maxIDBytes = MaxEvidenceBytes - sha256.Size
		if len(id) == 0 || len(id) > maxIDBytes || size > MaxEvidenceBytes-len(id)-sha256.Size {
			return ExportState{}, fmt.Errorf("%w: invalid export inventory", ErrIncomplete)
		}
		size += len(id) + sha256.Size
	}
	state.Artifacts = maps.Clone(state.Artifacts)
	return state, nil
}

func (s *scenario) checkExportsUnchanged(ctx context.Context, before ExportState) error {
	after, err := s.observeExports(ctx)
	if err != nil {
		return err
	}
	if after.Pending != before.Pending || !maps.Equal(after.Artifacts, before.Artifacts) {
		return fmt.Errorf("%w: export state changed after denied operations and read-only controls", ErrViolation)
	}
	return nil
}

func (s *scenario) exerciseExports(ctx context.Context, cell policyCell, evidence matrixEvidence, samples *denialSamples) error {
	if !cell.Export {
		if err := s.deniedCreate(ctx, cell.Target, evidence, samples); err != nil {
			return fmt.Errorf("denied export create: %w", err)
		}
	}
	for i, artifact := range evidence.artifacts {
		if cell.Export && cell.stream.Scope == artifact.stream.Scope {
			digest, err := s.fetchControl(ctx, cell.Target, artifact.stream, artifact.id)
			if err != nil {
				return fmt.Errorf("authorized same-stream download: %w", err)
			}
			if digest != artifact.digest {
				return fmt.Errorf("%w: authorized same-stream download returned different artifact bytes", ErrViolation)
			}
			continue
		}
		phase := "artifact requested in another authorized stream"
		if !cell.Export {
			phase = "artifact requested without an export grant"
		}
		for _, id := range []string{artifact.id, missingArtifact} {
			if err := s.deniedDownload(ctx, cell.Target, id, evidence, samples); err != nil {
				return fmt.Errorf("%s (source stream %d): %w", phase, i+1, err)
			}
		}
	}
	return nil
}

func (s *scenario) deniedCreate(ctx context.Context, target Target, evidence matrixEvidence, samples *denialSamples) error {
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
	return evidence.checkDenial(createForbidden, start.Response, samples)
}

func (s *scenario) deniedDownload(ctx context.Context, target Target, id string, evidence matrixEvidence, samples *denialSamples) error {
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
	return evidence.checkDenial(downloadForbidden, artifact.Response, samples)
}

func (s *scenario) createControl(ctx context.Context, cell policyCell) (scopedArtifact, error) {
	start, err := s.Export.CreateExport(ctx, cell.Target)
	if err := observation(ctx, err); err != nil {
		return scopedArtifact{}, err
	}
	if err := requireResponse(start.Response, anySuccessStatus); err != nil {
		return scopedArtifact{}, err
	}
	forbidden := s.scopedMarkers(cell.stream.Scope)
	if err := inspectResponse(start.Response, forbidden); err != nil {
		return scopedArtifact{}, err
	}
	if start.ArtifactID == "" || len(start.ArtifactID) > MaxEvidenceBytes {
		return scopedArtifact{}, fmt.Errorf("%w: authorized export lacks a bounded artifact ID", ErrIncomplete)
	}
	if err := checkMarkers([]byte(start.ArtifactID), forbidden); err != nil {
		return scopedArtifact{}, err
	}
	digest, err := s.fetchControl(ctx, cell.Target, cell.stream, start.ArtifactID)
	if err != nil {
		return scopedArtifact{}, err
	}
	return scopedArtifact{start.ArtifactID, cell.stream, digest}, nil
}

func (s *scenario) fetchControl(ctx context.Context, target Target, stream Stream, id string) ([sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	artifact, err := s.Export.FetchExport(ctx, target, id)
	if err := observation(ctx, err); err != nil {
		return zero, err
	}
	if err := requireResponse(artifact.Response, anySuccessStatus); err != nil {
		return zero, err
	}
	forbidden := s.scopedMarkers(stream.Scope)
	if err := inspectResponse(artifact.Response, forbidden); err != nil {
		return zero, err
	}
	manifest := artifact.Manifest
	if !artifact.Verified || manifest.Version == "" || manifest.Scope == (Scope{}) ||
		len(manifest.PolicyVersions) == 0 || artifact.Records == nil {
		return zero, ErrIncomplete
	}
	if manifest.Scope != stream.Scope || !slices.Equal(manifest.PolicyVersions, []string{stream.PolicyVersion}) {
		return zero, fmt.Errorf("%w: export manifest scope or policy version differs", ErrViolation)
	}
	want, _ := s.forScope(stream.Scope)
	if err := inspectRecords(artifact.Bytes, artifact.Records, want, forbidden); err != nil {
		return zero, err
	}
	return sha256.Sum256(artifact.Bytes), nil
}
