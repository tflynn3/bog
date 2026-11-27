package streampolicy

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
)

const (
	maskedInput = "retained-alice-a"
	maskedLine  = "fixture-masked-first-a"
)

func (s *revisionScenario) queryRequest(target Target, limit int) QueryRequest {
	return QueryRequest{Target: target, Start: s.fixture.QueryStart, End: s.fixture.QueryEnd, Limit: limit}
}

func (s *revisionScenario) expectedRows(request QueryRequest) []Row {
	scope := s.streamForSelector(request.Selector).Scope
	grant, policy := s.model.resolve(request.Target, scope)
	if !grant.Query {
		return nil
	}
	var rows []Row
	for _, record := range s.committed {
		row := record.Row
		if record.Scope == scope && request.Start <= row.Timestamp && row.Timestamp < request.End {
			row.Line = replaceLine(row.Line, policy.Masks)
			if strings.Contains(row.Line, request.Contains) {
				rows = append(rows, row)
			}
		}
	}
	slices.SortStableFunc(rows, func(a, b Row) int { return strings.Compare(a.Timestamp, b.Timestamp) })
	return rows
}

func (s *revisionScenario) revisionPage(ctx context.Context, request QueryRequest, rows []Row) (Page, error) {
	page, err := s.query(ctx, request, http.StatusOK)
	if err != nil {
		return Page{}, err
	}
	if err := compareRows(page.Rows, rows); err != nil {
		return Page{}, err
	}
	if err := compareQueryBody(page.Body, rows); err != nil {
		return Page{}, err
	}
	scope := s.streamForSelector(request.Selector).Scope
	_, policy := s.model.resolve(request.Target, scope)
	for _, mask := range policy.Masks {
		if err := inspectResponse(page.Response, []string{mask.Forbidden}); err != nil {
			return Page{}, err
		}
	}
	return page, nil
}

func compareQueryBody(body []byte, want []Row) error {
	if len(body) == 0 && len(want) != 0 {
		return fmt.Errorf("%w: query rows lack wire evidence", ErrIncomplete)
	}
	rows, err := decodeQueryRows(body)
	if err != nil || !slices.Equal(rows, want) {
		return fmt.Errorf("%w: wire page differs from expected rows", ErrViolation)
	}
	return nil
}

func (s *revisionScenario) open(ctx context.Context, target Target) (cursorCase, error) {
	request := s.queryRequest(target, 1)
	rows := s.expectedRows(request)
	if len(rows) < 2 {
		return cursorCase{}, fmt.Errorf("%w: fixture needs at least two scoped rows", ErrIncomplete)
	}
	page, err := s.revisionPage(ctx, request, rows[:1])
	if err != nil {
		return cursorCase{}, err
	}
	if page.NextCursor == "" {
		return cursorCase{}, fmt.Errorf("%w: opening page lacks cursor evidence", ErrIncomplete)
	}
	return cursorCase{token: page.NextCursor, remaining: rows[1:]}, nil
}

func (s *revisionScenario) endPage(ctx context.Context, request QueryRequest, rows []Row) error {
	page, err := s.revisionPage(ctx, request, rows)
	if err == nil && page.NextCursor != "" {
		return fmt.Errorf("%w: terminal query exposed another cursor", ErrViolation)
	}
	return err
}

func (s *revisionScenario) resume(ctx context.Context, target Target, held cursorCase) error {
	request := s.queryRequest(target, 10)
	request.Cursor = held.token
	return s.endPage(ctx, request, held.remaining)
}

func (s *revisionScenario) rejectRevisionQueries(ctx context.Context, target Target, baseline *rejection, references, tokens []string) error {
	for _, token := range tokens {
		request := s.queryRequest(target, 10)
		request.Cursor = token
		page, err := s.query(ctx, request, baseline.status)
		if err != nil {
			return err
		}
		if err := s.checkRejection(page.Response, baseline, references); err != nil {
			return err
		}
	}
	return nil
}

type revisionCursorObserver struct {
	QueryObserver
	issued []string
}

func (o *revisionCursorObserver) Query(ctx context.Context, request QueryRequest) (Page, error) {
	page, err := o.QueryObserver.Query(ctx, request)
	if err == nil && page.NextCursor != "" {
		o.issued = append(o.issued, page.NextCursor)
	}
	return page, err
}

func (s *revisionScenario) recordRevisionCursors() *revisionCursorObserver {
	if observer, ok := s.Query.(*revisionCursorObserver); ok {
		return observer
	}
	observer := &revisionCursorObserver{QueryObserver: s.Query}
	s.Query = observer
	return observer
}

func (s *revisionScenario) checkQueries(ctx context.Context) error {
	// The candidate checks the static scope matrix before the revision probes.
	s.recordRevisionCursors()
	return s.scenario.checkQueries(ctx)
}

func (s *revisionScenario) checkGrantRevisions(ctx context.Context) error {
	s.recordRevisionCursors()
	a, b := s.fixture.Streams[0].Selector, s.fixture.Streams[1].Selector
	owner, expansion := Target{alice, a}, Target{bob, b}
	held, err := s.open(ctx, owner)
	if err != nil {
		return err
	}
	modified := modifyToken(held.token, []string{held.token})
	refs := []string{held.token, modified}
	denied := rejection{status: http.StatusForbidden}
	if err := s.rejectRevisionQueries(ctx, expansion, &denied, refs, []string{""}); err != nil {
		return fmt.Errorf("pre-expansion baseline: %w", err)
	}
	if err := s.update(ctx, func(revision *PolicyRevision) {
		setGrant(revision, owner, func(grant *Grant) { grant.Query = false })
		setGrant(revision, expansion, func(grant *Grant) { grant.Query = true })
	}); err != nil {
		return err
	}
	if err := s.rejectRevisionQueries(ctx, owner, &denied, refs, []string{"", held.token, modified}); err != nil {
		return fmt.Errorf("revoked query must deny before cursor validation: %w", err)
	}
	invalid := rejection{status: http.StatusBadRequest}
	if err := s.rejectRevisionQueries(ctx, expansion, &invalid, refs, []string{modified, held.token}); err != nil {
		return fmt.Errorf("expanded grant must preserve cursor scope: %w", err)
	}
	if err := s.rejectRevisionQueries(ctx, Target{bob, a}, &invalid, refs, []string{held.token}); err != nil {
		return fmt.Errorf("expanded grant must preserve principal binding: %w", err)
	}
	expanded, err := s.open(ctx, expansion)
	if err != nil {
		return fmt.Errorf("expanded scope first page: %w", err)
	}
	if err := s.setQuery(ctx, owner, true); err != nil {
		return err
	}
	if err := s.resume(ctx, owner, held); err != nil {
		return fmt.Errorf("regranted owner must resume original cursor: %w", err)
	}
	if err := s.resume(ctx, expansion, expanded); err != nil {
		return fmt.Errorf("expanded scope must resume only its own rows: %w", err)
	}
	return nil
}

func (s *revisionScenario) setMask(ctx context.Context, version string, rules []Replacement) error {
	return s.update(ctx, func(revision *PolicyRevision) {
		revision.Streams[0].MaskingVersion = version
		revision.Streams[0].Masks = rules
	})
}

func (s *revisionScenario) checkMaskingFilters(ctx context.Context, target Target) error {
	for _, filter := range []string{"", maskedInput, maskedLine} {
		request := s.queryRequest(target, 10)
		request.Contains = filter
		if err := s.endPage(ctx, request, s.expectedRows(request)); err != nil {
			return err
		}
	}
	return nil
}

func (s *revisionScenario) checkMaskingRevisions(ctx context.Context) error {
	a := Target{alice, s.fixture.Streams[0].Selector}
	b := Target{bob, s.fixture.Streams[1].Selector}
	refs := slices.Clone(s.recordRevisionCursors().issued)
	// Reserve each issued token for one probe. A previous presentation can
	// consume a cursor and make a later rejection ambiguous.
	open := func(target Target, witnesses ...*cursorCase) error {
		for _, witness := range witnesses {
			held, err := s.open(ctx, target)
			if err != nil {
				return err
			}
			if slices.Contains(refs, held.token) {
				return fmt.Errorf("%w: masking scenario requires distinct cursors", ErrIncomplete)
			}
			refs = append(refs, held.token)
			*witness = held
		}
		return nil
	}
	var originalBaseline, atChange, atRevocation, atRegrant, originalAtRestore cursorCase
	if err := open(a, &originalBaseline, &atChange, &atRevocation, &atRegrant, &originalAtRestore); err != nil {
		return fmt.Errorf("open original masking witnesses: %w", err)
	}
	if err := s.resume(ctx, a, originalBaseline); err != nil {
		return fmt.Errorf("original cursor must resume before changing masking rules: %w", err)
	}
	var unaffectedAtChange, unaffectedAtRestore cursorCase
	if err := open(b, &unaffectedAtChange, &unaffectedAtRestore); err != nil {
		return fmt.Errorf("open unaffected stream witnesses: %w", err)
	}
	modified := modifyToken(atChange.token, refs)
	refs = append(refs, modified)
	invalid := rejection{status: http.StatusBadRequest}
	if err := s.rejectRevisionQueries(ctx, a, &invalid, refs, []string{modified}); err != nil {
		return err
	}
	denied := rejection{status: http.StatusForbidden}
	if err := s.rejectRevisionQueries(ctx, Target{exporter, a.Selector}, &denied, refs, []string{""}); err != nil {
		return err
	}
	if err := s.setMask(ctx, "fixture-masking-v2", []Replacement{{maskedInput, maskedLine}}); err != nil {
		return err
	}
	if err := s.rejectRevisionQueries(ctx, a, &invalid, refs, []string{atChange.token}); err != nil {
		return fmt.Errorf("old masking revision must invalidate authorized cursor: %w", err)
	}
	if err := s.resume(ctx, b, unaffectedAtChange); err != nil {
		return fmt.Errorf("unaffected stream must resume: %w", err)
	}
	if err := s.checkMaskingFilters(ctx, a); err != nil {
		return fmt.Errorf("fresh query must match and page over masked rows: %w", err)
	}
	var currentAtRegrant cursorCase
	if err := open(a, &currentAtRegrant); err != nil {
		return fmt.Errorf("open current masking witness before revocation: %w", err)
	}
	if err := s.setQuery(ctx, a, false); err != nil {
		return err
	}
	if err := s.rejectRevisionQueries(ctx, a, &denied, refs, []string{atRevocation.token, modified}); err != nil {
		return fmt.Errorf("revoked query must deny before masking revision validation: %w", err)
	}
	if err := s.setQuery(ctx, a, true); err != nil {
		return err
	}
	if err := s.rejectRevisionQueries(ctx, a, &invalid, refs, []string{atRegrant.token}); err != nil {
		return fmt.Errorf("regranted query must still reject old masking revision: %w", err)
	}
	if err := s.resume(ctx, a, currentAtRegrant); err != nil {
		return fmt.Errorf("regranted query must resume current masking revision: %w", err)
	}
	var maskedBaseline, maskedAtRestore cursorCase
	if err := open(a, &maskedBaseline, &maskedAtRestore); err != nil {
		return fmt.Errorf("open changed masking witnesses: %w", err)
	}
	if err := s.resume(ctx, a, maskedBaseline); err != nil {
		return fmt.Errorf("masked cursor must resume before restoring rules: %w", err)
	}
	if err := s.setMask(ctx, "fixture-masking-v3", nil); err != nil {
		return err
	}
	for _, probe := range []struct {
		version string
		held    cursorCase
	}{{"original", originalAtRestore}, {"changed", maskedAtRestore}} {
		if err := s.rejectRevisionQueries(ctx, a, &invalid, refs, []string{probe.held.token}); err != nil {
			return fmt.Errorf("restored masking rules must retain a new revision (%s cursor): %w", probe.version, err)
		}
	}
	if err := s.resume(ctx, b, unaffectedAtRestore); err != nil {
		return fmt.Errorf("unaffected stream must resume after restoring rules: %w", err)
	}
	if err := s.checkMaskingFilters(ctx, a); err != nil {
		return fmt.Errorf("fresh query must match and page over restored rows: %w", err)
	}
	return nil
}
