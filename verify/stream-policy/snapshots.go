package streampolicy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
)

func (c *conformance) request(target Target, filter string, limit int) QueryRequest {
	return QueryRequest{Target: target, Start: c.fixture.QueryStart, End: c.fixture.QueryEnd, Contains: filter, Limit: limit}
}

func (c *conformance) querySnapshotPage(ctx context.Context, request QueryRequest) (Page, error) {
	page, err := c.query(ctx, request, http.StatusOK)
	if err != nil {
		return Page{}, err
	}
	scope := c.model.streams[request.Selector]
	if hidden := c.model.masking[scope].Replacement.Forbidden; hidden != "" {
		if err := inspectResponse(page.Response, []string{hidden}); err != nil {
			return Page{}, err
		}
	}
	return page, nil
}

// checkWireRows independently reads the response, including terminal empty
// pages. A retained-field substring alone cannot establish order or multiplicity.
func checkWireRows(page Page, want []Row) error {
	if err := compareRows(page.Rows, want); err != nil {
		return err
	}
	if len(want) != 0 && len(page.Body) == 0 {
		return fmt.Errorf("%w: query rows lack response bytes", ErrIncomplete)
	}
	decoder := json.NewDecoder(bytes.NewReader(page.Body))
	decoder.DisallowUnknownFields()
	for _, expected := range want {
		var row Row
		if err := decoder.Decode(&row); err != nil || row != expected {
			return fmt.Errorf("%w: query body differs from exact expected rows", ErrViolation)
		}
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: unexpected data following the expected query rows", ErrViolation)
	}
	return nil
}

func (c *conformance) openCursor(ctx context.Context, request QueryRequest, want []Row) (string, error) {
	page, err := c.querySnapshotPage(ctx, request)
	if err != nil {
		return "", err
	}
	if err := checkWireRows(page, want[:1]); err != nil {
		return "", err
	}
	if page.NextCursor == "" {
		return "", fmt.Errorf("%w: opening page omitted continuation", ErrViolation)
	}
	return page.NextCursor, nil
}

// checkWalk consumes a fresh or held view. Trimmed pages may contain fewer
// rows than the request limit, but each continuation must progress.
func (c *conformance) checkWalk(ctx context.Context, request QueryRequest, want []Row) error {
	seen := make(map[string]bool)
	for {
		seen[request.Cursor] = true
		page, err := c.querySnapshotPage(ctx, request)
		if err != nil {
			return err
		}
		n := len(page.Rows)
		if n > request.Limit || n > len(want) {
			return fmt.Errorf("%w: query page exceeded its expected rows", ErrViolation)
		}
		if err := checkWireRows(page, want[:n]); err != nil {
			return err
		}
		want = want[n:]
		if page.NextCursor == "" && len(want) == 0 {
			return nil
		}
		if page.NextCursor == "" || len(want) == 0 || n == 0 || seen[page.NextCursor] {
			return fmt.Errorf("%w: query continuation did not match snapshot progress", ErrViolation)
		}
		request.Cursor = page.NextCursor
	}
}

func (c *conformance) checkMixedQueries(ctx context.Context) error {
	for _, target := range c.targets {
		if !c.model.grants[target.grant.Target].Query {
			continue
		}
		for _, filter := range []string{"", "retained-", "[redacted", rawBackfill, rawInterrupted, maskInputSecret, escaped} {
			request := c.request(target.grant.Target, filter, 2)
			if err := c.checkWalk(ctx, request, c.model.query(request)); err != nil {
				return fmt.Errorf("fresh walk (%s): %w", target.label(), err)
			}
		}
	}
	return nil
}

func (c *conformance) checkInterleaved(ctx context.Context) error {
	target := c.targets[0]
	request := c.request(target.grant.Target, "", 1)
	oldRows := c.model.query(request)
	token, err := c.openCursor(ctx, request, oldRows)
	if err != nil {
		return fmt.Errorf("opening snapshot: %w", err)
	}
	oldArtifact, err := c.createArtifact(ctx, target)
	if err != nil {
		return fmt.Errorf("export before backfill: %w", err)
	}
	oldRecords, oldVersions := c.model.export(target.grant.Target)
	batch := backfillBatch(c.fixture)
	if _, err := c.ingest(ctx, batch.request, http.StatusNoContent); err != nil {
		return fmt.Errorf("backfill: %w", err)
	}
	c.record(batch)
	request.Cursor, request.Limit = token, 2
	if err := c.checkWalk(ctx, request, oldRows[1:]); err != nil {
		return fmt.Errorf("resume held snapshot: %w", err)
	}
	if err := c.checkMixedQueries(ctx); err != nil {
		return err
	}
	grant := target.grant
	grant.Export = false
	if err := c.setGrant(ctx, grant); err != nil {
		return err
	}
	denial := rejection{status: http.StatusForbidden}
	references := []string{oldArtifact, missingArtifact}
	createDenial := rejection{status: http.StatusForbidden}
	if err := c.deniedCreate(ctx, grant.Target, &createDenial, references); err != nil {
		return fmt.Errorf("create after export revocation: %w", err)
	}
	for _, id := range references {
		if err := c.deniedDownload(ctx, grant.Target, id, &denial, references); err != nil {
			return fmt.Errorf("download after export revocation: %w", err)
		}
	}
	if err := c.setGrant(ctx, target.grant); err != nil {
		return err
	}
	if err := c.checkArtifactSnapshot(ctx, target, oldArtifact, oldRecords, oldVersions, rowMarkers(batch.rows)); err != nil {
		return fmt.Errorf("held export after backfill and regrant: %w", err)
	}
	return c.checkExports(ctx)
}

func rowMarkers(rows []Row) []string {
	markers := make([]string, 0, len(rows))
	for _, row := range rows {
		marker, _, _ := strings.Cut(row.Line, " ")
		markers = append(markers, marker)
	}
	return slices.Compact(markers)
}

func (c *conformance) setGrant(ctx context.Context, grant Grant) error {
	if err := observation(ctx, c.policy.SetGrant(ctx, grant)); err != nil {
		return err
	}
	c.model.grants[grant.Target] = grant
	return nil
}

func (c *conformance) setMasking(ctx context.Context, update MaskingChange) error {
	if err := observation(ctx, c.policy.SetMasking(ctx, update)); err != nil {
		return err
	}
	c.model.masking[update.Scope] = update
	return nil
}

func (c *conformance) checkPagePolicy(ctx context.Context) error {
	target := c.targets[0]
	request := c.request(target.grant.Target, "", 1)
	rows := c.model.query(request)
	token, err := c.openCursor(ctx, request, rows)
	if err != nil {
		return err
	}
	request.Cursor = token
	modified := modifyToken(token, []string{token})
	references := []string{token, modified}
	denied, invalid := rejection{status: http.StatusForbidden}, rejection{status: http.StatusBadRequest}
	ungranted := target.grant
	ungranted.Query = false
	if err := c.setGrant(ctx, ungranted); err != nil {
		return err
	}
	for _, probe := range []string{"", token, modified} {
		if err := c.rejectQuery(ctx, target, probe, &denied, references); err != nil {
			return fmt.Errorf("revoked query before cursor validation: %w", err)
		}
	}
	if err := c.setGrant(ctx, target.grant); err != nil {
		return err
	}
	if err := c.checkWalk(ctx, request, rows[1:]); err != nil {
		return fmt.Errorf("query resumed after regrant: %w", err)
	}
	if err := c.rejectQuery(ctx, target, modified, &invalid, references); err != nil {
		return fmt.Errorf("invalid cursor control: %w", err)
	}
	masking := MaskingChange{target.stream.Scope, "fixture-mask-r2", Replacement{maskedMarker, maskResult}}
	if err := c.setMasking(ctx, masking); err != nil {
		return err
	}
	if err := c.rejectQuery(ctx, target, token, &invalid, references); err != nil {
		return fmt.Errorf("stale cursor after masking enabled: %w", err)
	}
	for _, filter := range []string{maskedMarker, maskResult, ""} {
		fresh := c.request(target.grant.Target, filter, 1)
		if err := c.checkWalk(ctx, fresh, c.model.query(fresh)); err != nil {
			return fmt.Errorf("fresh query under current masking policy: %w", err)
		}
	}
	fresh := c.request(target.grant.Target, "", 1)
	maskedToken, err := c.openCursor(ctx, fresh, c.model.query(fresh))
	if err != nil {
		return err
	}
	references = append(references, maskedToken)
	if err := c.setMasking(ctx, MaskingChange{Scope: target.stream.Scope, Revision: "fixture-mask-r3"}); err != nil {
		return err
	}
	if err := c.setGrant(ctx, ungranted); err != nil {
		return err
	}
	if err := c.rejectQuery(ctx, target, maskedToken, &denied, references); err != nil {
		return fmt.Errorf("revocation takes precedence over stale revision: %w", err)
	}
	if err := c.setGrant(ctx, target.grant); err != nil {
		return err
	}
	for _, stale := range []string{token, maskedToken} {
		if err := c.rejectQuery(ctx, target, stale, &invalid, references); err != nil {
			return fmt.Errorf("restoring masking rules must keep old revisions invalid: %w", err)
		}
	}
	return c.checkWalk(ctx, fresh, c.model.query(fresh))
}
