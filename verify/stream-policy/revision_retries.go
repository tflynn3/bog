package streampolicy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
)

const (
	newRedactionVersion = "fixture-policy-a-v2"
	newReplacement      = "[fixture-redacted-v2]"
	postChangeKey       = "fixture-post-change-key"
	postChangeRetained  = "retained-after-policy-change"
)

func (s *revisionScenario) verifyRecoveredRetries(ctx context.Context, want []RetryCommit) error {
	if err := s.checkReplay(ctx); err != nil {
		return err
	}
	recovery, err := s.recovery.ObserveRetryRecovery(ctx)
	if err := observation(ctx, err); err != nil {
		return err
	}
	if !recovery.Complete || recovery.Commits == nil {
		return fmt.Errorf("%w: target lacks atomic retry recovery evidence", ErrIncomplete)
	}
	if len(recovery.Commits) != len(want) {
		return fmt.Errorf("%w: recovered atomic commit count differs", ErrViolation)
	}
	for i, expected := range want {
		got := recovery.Commits[i]
		if err := requireResponse(got.Outcome, http.StatusNoContent); err != nil {
			return fmt.Errorf("recovered retry %d: %w", i+1, err)
		}
		if err := inspectResponse(got.Outcome, s.scopedMarkers(expected.Scope)); err != nil {
			return fmt.Errorf("recovered retry %d: %w", i+1, err)
		}
		if !slices.Equal(got.Records, expected.Records) || got.Scope != expected.Scope ||
			got.Principal != expected.Principal || got.Key != expected.Key ||
			got.RedactionVersion != expected.RedactionVersion || !sameResponse(got.Outcome, expected.Outcome) {
			return fmt.Errorf("%w: recovered batch and retry disposition differ in commit %d", ErrViolation, i+1)
		}
	}
	return nil
}

func retryCommit(request IngestRequest, policy StreamPolicy, rows []Row, outcome Response) RetryCommit {
	commit := RetryCommit{
		Scope: policy.Scope, Principal: request.Principal, Key: request.Key,
		RedactionVersion: policy.RedactionVersion, Outcome: outcome,
	}
	for _, row := range rows {
		commit.Records = append(commit.Records, Record{policy.Scope, row})
	}
	return commit
}

func redactedFixtureRows(request IngestRequest, policy StreamPolicy) ([]Row, error) {
	decoder := json.NewDecoder(strings.NewReader(request.Body))
	var rows []Row
	for {
		var row Row
		err := decoder.Decode(&row)
		if err == io.EOF {
			return rows, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%w: malformed fixture append", ErrIncomplete)
		}
		if policy.FailOn != "" && strings.Contains(row.Line, policy.FailOn) {
			return nil, fmt.Errorf("%w: fixture append unexpectedly rejected by its redactor", ErrIncomplete)
		}
		row.Line = replaceLine(row.Line, policy.Redactions)
		rows = append(rows, row)
	}
}

func (s *revisionScenario) denyRetry(ctx context.Context, request IngestRequest, denial *rejection) error {
	response, err := s.Retry.Ingest(ctx, request)
	if err := observation(ctx, err); err != nil {
		return err
	}
	return s.checkRejection(response, denial, nil)
}

func (s *revisionScenario) checkRetryRevisions(ctx context.Context) error {
	var expected []RetryCommit
	for i, entry := range s.appends {
		scope := s.streamForSelector(entry.request.Selector).Scope
		_, policy := s.model.resolve(entry.request.Target, scope)
		expected = append(expected, retryCommit(entry.request, policy, entry.rows, s.outcomes[i]))
	}

	authorized := s.appends[0].request
	revoked := s.appends[len(s.appends)-1].request
	s.denialMarkers = append(s.denialMarkers, newRedactionVersion, newReplacement, postChangeKey, postChangeRetained)
	denied := rejection{status: http.StatusForbidden}
	control := revoked
	control.Principal = exporter
	if err := s.denyRetry(ctx, control, &denied); err != nil {
		return err
	}
	if err := s.update(ctx, func(revision *PolicyRevision) {
		setGrant(revision, revoked.Target, func(grant *Grant) { grant.Ingest = false })
		stream := &revision.Streams[0]
		stream.RedactionVersion = newRedactionVersion
		for i := range stream.Redactions {
			stream.Redactions[i].Replacement = newReplacement
		}
	}); err != nil {
		return err
	}

	if err := s.verifyRecoveredRetries(ctx, expected); err != nil {
		return fmt.Errorf("initial restart: %w", err)
	}
	if err := s.checkRecordedRetry(ctx, authorized, expected[0].Outcome); err != nil {
		return fmt.Errorf("authorized retry must retain its original disposition: %w", err)
	}
	if err := s.checkConflictingRetry(ctx, authorized); err != nil {
		return err
	}
	freshDenied := revoked
	freshDenied.Key = unusedKey
	for _, probe := range []IngestRequest{revoked, changedRetry(revoked), freshDenied} {
		if err := s.denyRetry(ctx, probe, &denied); err != nil {
			return fmt.Errorf("revoked retry must deny without revealing a recorded disposition: %w", err)
		}
	}

	fresh := authorized
	fresh.Key = postChangeKey
	fresh.Body = ndjson("2026-01-01T00:00:03Z", postChangeRetained+" "+escaped)
	_, active := s.model.resolve(fresh.Target, expected[0].Scope)
	rows, err := redactedFixtureRows(fresh, active)
	if err != nil {
		return err
	}
	outcome, err := s.ingest(ctx, fresh, http.StatusNoContent)
	if err != nil {
		return fmt.Errorf("fresh append under revised redaction: %w", err)
	}
	newCommit := retryCommit(fresh, active, rows, outcome)
	expected = append(expected, newCommit)
	s.committed = append(s.committed, newCommit.Records...)
	if err := s.verifyRecoveredRetries(ctx, expected); err != nil {
		return fmt.Errorf("restart after retry and fresh append: %w", err)
	}
	if err := s.denyRetry(ctx, revoked, &denied); err != nil {
		return fmt.Errorf("ingest revocation must survive another restart: %w", err)
	}
	for _, target := range s.targets {
		request := s.queryRequest(target.grant.Target, 10)
		grant, _ := s.model.resolve(request.Target, target.stream.Scope)
		if grant.Query {
			if err := s.endPage(ctx, request, s.expectedRows(request)); err != nil {
				return fmt.Errorf("reopened scoped rows (%s): %w", target.label(), err)
			}
		}
	}
	return nil
}
