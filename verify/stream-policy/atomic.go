package streampolicy

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"sync"
)

const freshPolicyKey = "fixture-fresh-policy-key"

func (c *conformance) record(batch appendCase) {
	c.model.append(batch)
	c.committed = c.model.records()
	c.denialMarkers = append(c.denialMarkers, rowMarkers(batch.rows)...)
}

func (c *conformance) checkCommitEvidence(evidence CommitEvidence) error {
	if !evidence.Complete || evidence.Records == nil || evidence.RetryRecords == nil {
		return ErrIncomplete
	}
	forbidden := append(slices.Clone(c.forbidden), unusedKey, failedKey)
	for _, pending := range []struct{ key, marker string }{
		{rollbackKey, beforeCommitMarker},
		{interruptedKey, afterCommitMarker},
		{freshPolicyKey, freshPolicyMarker},
	} {
		if !slices.ContainsFunc(c.model.retries, func(record RetryRecord) bool { return record.Key == pending.key }) {
			forbidden = append(forbidden, pending.key, pending.marker)
		}
	}
	if err := inspectRecords(evidence.Bytes, evidence.Records, c.model.records(), forbidden); err != nil {
		return err
	}
	if len(evidence.RetryRecords) != len(c.model.retries) {
		return fmt.Errorf("%w: committed retry outcome count differs", ErrViolation)
	}
	got := slices.SortedFunc(slices.Values(evidence.RetryRecords), compareRetryRecord)
	want := slices.SortedFunc(slices.Values(c.model.retries), compareRetryRecord)
	if !slices.Equal(got, want) {
		return fmt.Errorf("%w: committed retry outcomes differ from the expected scoped outcomes", ErrViolation)
	}
	decoded := jsonUnescaped(evidence.Bytes)
	for _, retry := range want {
		if !containsMarker(evidence.Bytes, decoded, retry.Key) ||
			!containsMarker(evidence.Bytes, decoded, retry.PolicyVersion) {
			return fmt.Errorf("%w: committed bytes lack retry key or policy witness", ErrIncomplete)
		}
	}
	return nil
}

func (c *conformance) inspectCommit(ctx context.Context) error {
	evidence, err := c.commit.Inspect(ctx)
	if err := observation(ctx, err); err != nil {
		return err
	}
	return c.checkCommitEvidence(evidence)
}

func (c *conformance) reopenCommit(ctx context.Context) error {
	evidence, err := c.commit.Reopen(ctx)
	if err := observation(ctx, err); err != nil {
		return err
	}
	if err := c.checkIdentity(ctx); err != nil {
		return err
	}
	return c.checkCommitEvidence(evidence)
}

func (c *conformance) interrupt(ctx context.Context, request IngestRequest, boundary CommitBoundary) error {
	observed, err := c.commit.InterruptAppend(ctx, request, boundary)
	if err := observation(ctx, err); err != nil {
		return err
	}
	if !observed.Stopped || observed.Boundary != boundary {
		return fmt.Errorf("%w: requested interruption boundary was not observed", ErrIncomplete)
	}
	if observed.ResponseStarted {
		return fmt.Errorf("%w: interrupted append started an application response", ErrViolation)
	}
	return nil
}

func (c *conformance) checkAtomicRetry(ctx context.Context) error {
	target := c.targets[0]
	before := additionalBatch(target.grant.Target, rollbackKey, replacement,
		rowInput{"2026-01-01T00:00:02Z", beforeCommitMarker, rawInterrupted + " " + maskInputSecret},
		rowInput{"2026-01-01T00:00:02Z", beforeCommitMarker, escaped})
	after := additionalBatch(target.grant.Target, interruptedKey, replacement,
		rowInput{"2026-01-01T00:00:02Z", afterCommitMarker, maskInputSecret},
		rowInput{"2026-01-01T00:00:02Z", afterCommitMarker, maskInputSecret})
	if err := c.inspectCommit(ctx); err != nil {
		return fmt.Errorf("running committed authority before interruption: %w", err)
	}
	request := c.request(target.grant.Target, "", 1)
	rows := c.model.query(request)
	token, err := c.openCursor(ctx, request, rows)
	if err != nil {
		return err
	}
	if err := c.interrupt(ctx, before.request, BeforeCommit); err != nil {
		return fmt.Errorf("interrupt before commit: %w", err)
	}
	if err := c.reopenCommit(ctx); err != nil {
		return fmt.Errorf("recovery before commit: %w", err)
	}
	invalid := rejection{status: http.StatusBadRequest}
	if err := c.rejectQuery(ctx, target, token, &invalid, []string{token}); err != nil {
		return fmt.Errorf("cursor survived restart: %w", err)
	}
	if err := c.checkMixedQueries(ctx); err != nil {
		return err
	}
	if err := c.checkConcurrentRetry(ctx, before.request); err != nil {
		return fmt.Errorf("first success after interrupted uncommitted append: %w", err)
	}
	c.record(before)
	if err := c.inspectCommit(ctx); err != nil {
		return fmt.Errorf("committed authority after first success: %w", err)
	}
	if err := c.interrupt(ctx, after.request, AfterCommit); err != nil {
		return fmt.Errorf("interrupt after commit: %w", err)
	}
	c.record(after)
	if err := c.reopenCommit(ctx); err != nil {
		return fmt.Errorf("recovery after commit and before client response: %w", err)
	}
	if err := c.checkMixedQueries(ctx); err != nil {
		return err
	}
	return c.retryUnderNewPolicy(ctx, target, after)
}

func (c *conformance) checkConcurrentRetry(ctx context.Context, request IngestRequest) error {
	var responses [2]Response
	var failures [2]error
	var workers sync.WaitGroup
	start := make(chan struct{})
	for i := range responses {
		workers.Go(func() {
			<-start
			responses[i], failures[i] = c.ingest(ctx, request, http.StatusNoContent)
		})
	}
	close(start)
	workers.Wait()
	for _, err := range failures {
		if err != nil {
			return err
		}
	}
	if !sameResponse(responses[0], responses[1]) {
		return fmt.Errorf("%w: concurrent retry responses differ", ErrViolation)
	}
	return nil
}

func (c *conformance) retryUnderNewPolicy(ctx context.Context, target matrixTarget, committed appendCase) error {
	update := RedactionChange{target.stream.Scope, changedRedactionVersion,
		[]Replacement{{maskInputSecret, changedReplacement}, {plain, replacement}, {escaped, replacement}}}
	if err := observation(ctx, c.policy.SetRedaction(ctx, update)); err != nil {
		return err
	}
	c.model.versions[update.Scope] = update.Version
	revoked := target.grant
	revoked.Ingest = false
	if err := c.setGrant(ctx, revoked); err != nil {
		return err
	}
	unused := committed.request
	unused.Key = unusedKey
	denied := rejection{status: http.StatusForbidden}
	for _, request := range []IngestRequest{unused, committed.request, changedRetry(committed.request)} {
		response, err := c.Retry.Ingest(ctx, request)
		if err := observation(ctx, err); err != nil {
			return err
		}
		if err := c.checkRejection(response, &denied, nil); err != nil {
			return fmt.Errorf("ingest revoked before outcome lookup: %w", err)
		}
	}
	if err := c.setGrant(ctx, target.grant); err != nil {
		return err
	}
	first, err := c.ingest(ctx, committed.request, http.StatusNoContent)
	if err != nil {
		return fmt.Errorf("recover committed outcome under a different policy: %w", err)
	}
	if err := c.checkRecordedRetry(ctx, committed.request, first); err != nil {
		return err
	}
	// Changing only raw bytes while retaining the same decoded row content
	// must conflict. This catches hashing the decoded or redacted payload.
	conflict := committed.request
	conflict.Body = " " + conflict.Body
	if _, err := c.ingest(ctx, conflict, http.StatusConflict); err != nil {
		return fmt.Errorf("raw-body fingerprint conflict: %w", err)
	}
	if err := c.inspectCommit(ctx); err != nil {
		return fmt.Errorf("denials and recovered retries must leave committed authority unchanged: %w", err)
	}
	fresh := additionalBatch(target.grant.Target, freshPolicyKey, changedReplacement,
		rowInput{"2026-01-01T00:00:03Z", freshPolicyMarker, maskInputSecret})
	if _, err := c.ingest(ctx, fresh.request, http.StatusNoContent); err != nil {
		return fmt.Errorf("fresh append under new policy: %w", err)
	}
	c.record(fresh)
	if err := c.reopenCommit(ctx); err != nil {
		return fmt.Errorf("recover both redaction policy versions: %w", err)
	}
	if err := c.checkRecordedRetry(ctx, committed.request, first); err != nil {
		return fmt.Errorf("retry after reopening updated policy: %w", err)
	}
	if err := c.checkMixedQueries(ctx); err != nil {
		return err
	}
	err = c.checkExportMatrix(ctx, func(ctx context.Context, downloader matrixTarget, id string) error {
		records, versions := c.model.export(downloader.grant.Target)
		forbidden := []string{backfillKey, rollbackKey, interruptedKey, freshPolicyKey}
		if downloader.stream.Scope != target.stream.Scope {
			forbidden = append(forbidden, changedRedactionVersion)
		}
		return c.checkArtifactSnapshot(ctx, downloader, id, records, versions, forbidden)
	})
	if err != nil {
		return fmt.Errorf("export across redaction policy versions: %w", err)
	}
	return c.inspectCommit(ctx)
}
