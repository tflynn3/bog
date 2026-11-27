package streampolicy

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"sync"
)

const (
	fixtureQueryLimit = 10
	freshRetryKey     = "fixture-unseen-retry-key"
	missingArtifact   = "fixture-missing-artifact"
)

type policyCell struct {
	Grant
	stream      Stream
	streamIndex int
}

func policyMatrix(fixture Fixture) []policyCell {
	grants := make(map[Target]Grant, len(fixture.Grants))
	var principals []string
	for _, grant := range fixture.Grants {
		grants[grant.Target] = grant
		if !slices.Contains(principals, grant.Principal) {
			principals = append(principals, grant.Principal)
		}
	}
	streams := append(slices.Clone(fixture.Streams[:]), Stream{Selector: unknown})
	cells := make([]policyCell, 0, len(principals)*len(streams))
	for _, principal := range principals {
		for i, stream := range streams {
			target := Target{principal, stream.Selector}
			grant := grants[target]
			grant.Target = target
			cells = append(cells, policyCell{grant, stream, i})
		}
	}
	return cells
}

type cursorControl struct {
	request QueryRequest
	invalid string
	last    []Row
}

func (s *scenario) openCursors(ctx context.Context) ([]cursorControl, error) {
	var controls []cursorControl
	var issued []string
	for _, cell := range s.matrix {
		if !cell.Query {
			continue
		}
		_, rows := s.forScope(cell.stream.Scope)
		if len(rows) != 2 {
			return nil, fmt.Errorf("%w: cursor control needs two rows", ErrIncomplete)
		}
		request := QueryRequest{Target: cell.Target, Limit: 1}
		page, err := s.query(ctx, request, http.StatusOK)
		if err != nil {
			return nil, fmt.Errorf("issue cursor: %w", err)
		}
		if err := compareRows(page.Rows, rows[:1]); err != nil {
			return nil, fmt.Errorf("opening page: %w", err)
		}
		if page.NextCursor == "" {
			return nil, fmt.Errorf("%w: opening page has no continuation", ErrIncomplete)
		}
		request.Cursor = page.NextCursor
		issued = append(issued, page.NextCursor)
		controls = append(controls, cursorControl{request: request, last: rows[1:]})
	}
	for i := range controls {
		token := controls[i].request.Cursor
		mid := len(token) / 2
		for _, ch := range "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789" {
			candidate := token[:mid] + string(ch) + token[mid+1:]
			if !slices.Contains(issued, candidate) {
				controls[i].invalid = candidate
				break
			}
		}
		if controls[i].invalid == "" {
			return nil, fmt.Errorf("%w: could not construct a distinct modified token", ErrIncomplete)
		}
	}
	return controls, nil
}

type denialKind uint8

const (
	queryForbidden denialKind = iota
	invalidCursor
	ingestForbidden
	createForbidden
	downloadForbidden
	denialKindCount
)

var denialNames = [denialKindCount]string{
	queryForbidden:    "query authorization",
	invalidCursor:     "invalid cursor",
	ingestForbidden:   "keyed ingest authorization",
	createForbidden:   "export creation authorization",
	downloadForbidden: "export download authorization",
}

// Each worker compares its own observations. The caller then compares one
// exemplar per operation across workers in fixture order.
type denialSamples [denialKindCount]Response

func (s *denialSamples) add(kind denialKind, got Response) error {
	want := &s[kind]
	if !want.Complete {
		*want = got
		return nil
	}
	if !sameResponse(got, *want) {
		return fmt.Errorf("%w: rejection exposes different response evidence", ErrViolation)
	}
	return nil
}

type matrixEvidence struct {
	cursors    []cursorControl
	artifacts  []scopedArtifact
	markers    []string
	references []string
}

func (s *scenario) checkMatrix(ctx context.Context) error {
	artifacts, before, err := s.prepareExports(ctx)
	if err != nil {
		return err
	}
	cursors, err := s.openCursors(ctx)
	if err != nil {
		return err
	}
	evidence := matrixEvidence{
		cursors: cursors, artifacts: artifacts,
		markers: s.denialMarkers(), references: []string{missingArtifact},
	}
	for _, cursor := range cursors {
		evidence.references = append(evidence.references, cursor.request.Cursor, cursor.invalid)
	}
	for _, artifact := range artifacts {
		evidence.references = append(evidence.references, artifact.id)
	}
	if err := s.exerciseMatrix(ctx, evidence); err != nil {
		return err
	}
	// Prove the tokens supplied to the probes were usable throughout the wave.
	for _, cursor := range cursors {
		page, err := s.query(ctx, cursor.request, http.StatusOK)
		if err == nil {
			err = terminalRows(page, cursor.last)
		}
		if err != nil {
			return fmt.Errorf("issuer resumes cursor: %w", err)
		}
	}
	return s.checkExportsUnchanged(ctx, before)
}

func (s *scenario) exerciseMatrix(ctx context.Context, evidence matrixEvidence) error {
	type result struct {
		samples denialSamples
		err     error
	}
	results := make([]result, len(s.matrix))
	var workers sync.WaitGroup
	for i, cell := range s.matrix {
		workers.Go(func() {
			results[i].err = s.exerciseCell(ctx, cell, evidence, &results[i].samples)
		})
	}
	workers.Wait()

	var combined denialSamples
	for i, result := range results {
		if result.err != nil {
			return fmt.Errorf("authorization and cursor scope, policy cell %d: %w", i+1, result.err)
		}
		for kind, response := range result.samples {
			if !response.Complete {
				continue
			}
			if err := combined.add(denialKind(kind), response); err != nil {
				return fmt.Errorf("authorization and cursor scope, policy cell %d, %s: %w", i+1, denialNames[kind], err)
			}
		}
	}
	return observation(ctx, nil)
}

func (s *scenario) exerciseCell(ctx context.Context, cell policyCell, evidence matrixEvidence, samples *denialSamples) error {
	if err := observation(ctx, nil); err != nil {
		return err
	}
	if err := s.exerciseQueries(ctx, cell, evidence, samples); err != nil {
		return err
	}
	if err := s.exerciseRetries(ctx, cell, evidence, samples); err != nil {
		return err
	}
	return s.exerciseExports(ctx, cell, evidence, samples)
}

func (s *scenario) exerciseQueries(ctx context.Context, cell policyCell, evidence matrixEvidence, samples *denialSamples) error {
	if cell.Query {
		if err := s.checkFullQuery(ctx, cell); err != nil {
			return fmt.Errorf("authorized same-stream query: %w", err)
		}
	} else {
		if err := s.checkRejectedQuery(ctx, cell.Target, "", queryForbidden, evidence, samples); err != nil {
			return fmt.Errorf("query authorization before validation: %w", err)
		}
	}
	for _, cursor := range evidence.cursors {
		if !cell.Query {
			for _, token := range []string{cursor.request.Cursor, cursor.invalid} {
				if err := s.checkRejectedQuery(ctx, cell.Target, token, queryForbidden, evidence, samples); err != nil {
					return fmt.Errorf("query authorization before validation: %w", err)
				}
			}
			continue
		}
		token := cursor.request.Cursor
		if cell.Target == cursor.request.Target {
			token = cursor.invalid
		}
		if err := s.checkRejectedQuery(ctx, cell.Target, token, invalidCursor, evidence, samples); err != nil {
			return fmt.Errorf("authorized cursor scope mismatch or modified cursor: %w", err)
		}
	}
	return nil
}

func (s *scenario) checkRejectedQuery(ctx context.Context, target Target, cursor string, kind denialKind, evidence matrixEvidence, samples *denialSamples) error {
	request := QueryRequest{Target: target, Cursor: cursor, Limit: 1}
	status := http.StatusForbidden
	if kind == invalidCursor {
		status = http.StatusBadRequest
	}
	page, err := s.query(ctx, request, status)
	if err != nil {
		return err
	}
	return evidence.checkDenial(kind, page.Response, samples)
}

func (s *scenario) exerciseRetries(ctx context.Context, cell policyCell, evidence matrixEvidence, samples *denialSamples) error {
	for i, entry := range s.appends {
		request := entry.request
		if cell.Ingest {
			if cell.Target != request.Target {
				continue
			}
			got, err := s.ingest(ctx, request, http.StatusNoContent)
			if err != nil {
				return fmt.Errorf("authorized keyed retry: %w", err)
			}
			if !sameResponse(got, s.outcomes[i]) {
				return fmt.Errorf("%w: authorized keyed retry differs from committed outcome", ErrViolation)
			}
			if _, err := s.ingest(ctx, conflictingRetry(request), http.StatusConflict); err != nil {
				return fmt.Errorf("authorized changed retry: %w", err)
			}
			continue
		}
		request.Target = cell.Target
		changed := conflictingRetry(request)
		fresh := changed
		fresh.Key = freshRetryKey
		for j, probe := range []IngestRequest{request, changed, fresh} {
			got, err := s.ingest(ctx, probe, http.StatusForbidden)
			if err == nil {
				err = evidence.checkDenial(ingestForbidden, got, samples)
			}
			if err != nil {
				return fmt.Errorf("ingest denied, source namespace %d, retry variant %d: %w", i+1, j+1, err)
			}
		}
	}
	return nil
}

func (e matrixEvidence) checkDenial(kind denialKind, response Response, samples *denialSamples) error {
	if err := inspectResponse(response, e.markers); err != nil {
		return err
	}
	if err := inspectDenialReferences(response, e.references); err != nil {
		return err
	}
	return samples.add(kind, response)
}
