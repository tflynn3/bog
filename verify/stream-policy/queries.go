package streampolicy

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
)

func (s *scenario) query(ctx context.Context, request QueryRequest, status int) (Page, error) {
	request.Start, request.End = s.fixture.QueryStart, s.fixture.QueryEnd
	page, err := s.Query.Query(ctx, request)
	if err := observation(ctx, err); err != nil {
		return Page{}, err
	}
	if err := requireResponse(page.Response, status); err != nil {
		return Page{}, err
	}
	if status != http.StatusOK && (len(page.Rows) != 0 || page.NextCursor != "") {
		return Page{}, fmt.Errorf("%w: rejected query disclosed rows or cursor", ErrViolation)
	}
	cursorHeader, sent := page.Headers["Bog-Next-Cursor"]
	if sent && (page.NextCursor == "" || !slices.Equal(cursorHeader, []string{page.NextCursor})) {
		return Page{}, fmt.Errorf("%w: unexpected cursor header evidence", ErrViolation)
	}
	if page.NextCursor != "" && !sent {
		return Page{}, fmt.Errorf("%w: continuation lacks header evidence", ErrIncomplete)
	}
	if status == http.StatusOK {
		if err := inspectResponse(page.Response, s.scopedMarkers(s.streamForSelector(request.Selector).Scope)); err != nil {
			return Page{}, err
		}
	}
	return page, nil
}

func (s *scenario) checkPageRows(page Page, want []Row) error {
	if len(page.Rows) != 0 && len(page.Body) == 0 {
		return fmt.Errorf("%w: query rows lack response bytes", ErrIncomplete)
	}
	if err := compareRows(page.Rows, want); err != nil {
		return err
	}
	decoded := jsonUnescaped(page.Body)
	for i, row := range want {
		if !containsMarker(page.Body, decoded, row.Line) {
			return fmt.Errorf("%w: query bytes lack retained-field witness for row %d", ErrIncomplete, i+1)
		}
	}
	for _, record := range s.committed {
		retained, _, _ := strings.Cut(record.Row.Line, " ")
		if !slices.Contains(want, record.Row) && containsMarker(page.Body, decoded, retained) {
			return fmt.Errorf("%w: query bytes contain a fixture row outside the expected page", ErrViolation)
		}
	}
	return nil
}

func (s *scenario) terminalRows(page Page, want []Row) error {
	if page.NextCursor != "" {
		return fmt.Errorf("%w: expected a terminal fixture page", ErrViolation)
	}
	return s.checkPageRows(page, want)
}

func (s *scenario) checkFullQueries(ctx context.Context) error {
	filters := append([]string{"", replacement}, s.forbidden...)
	for _, target := range s.targets {
		if !target.grant.Query {
			continue
		}
		_, rows := s.forScope(target.stream.Scope)
		request := QueryRequest{Target: target.grant.Target, Limit: 10}
		for _, filter := range filters {
			request.Contains = filter
			var want []Row
			if filter == "" || filter == replacement {
				want = rows
			}
			page, err := s.query(ctx, request, http.StatusOK)
			if err == nil {
				err = s.terminalRows(page, want)
			}
			if err != nil {
				return fmt.Errorf("authorized query in stream %d (principal %d): %w", target.selectorIndex, target.principalIndex, err)
			}
		}
	}
	return nil
}

type cursorCase struct {
	issuer          matrixTarget
	token, modified string
	remaining       []Row
}

func (s *scenario) issueCursors(ctx context.Context) ([]cursorCase, error) {
	var cursors []cursorCase
	for _, target := range s.targets {
		if !target.grant.Query {
			continue
		}
		_, rows := s.forScope(target.stream.Scope)
		page, err := s.query(ctx, QueryRequest{Target: target.grant.Target, Limit: 1}, http.StatusOK)
		if err == nil {
			err = s.checkPageRows(page, rows[:1])
		}
		if err != nil {
			return nil, fmt.Errorf("issue cursor (%s): %w", target.label(), err)
		}
		if page.NextCursor == "" {
			return nil, fmt.Errorf("%w: opening page has no continuation (%s)", ErrIncomplete, target.label())
		}
		for _, prior := range cursors {
			if prior.token == page.NextCursor {
				return nil, fmt.Errorf("%w: cursor reused across issuers (%s)", ErrViolation, target.label())
			}
		}
		cursors = append(cursors, cursorCase{issuer: target, token: page.NextCursor, remaining: rows[1:]})
	}
	return cursors, nil
}

// invalidTokens changes each token without accidentally choosing another
// issued token. The returned references also cover echoes of another probe.
func invalidTokens(cursors []cursorCase) []string {
	references := make([]string, 0, 2*len(cursors))
	for _, cursor := range cursors {
		references = append(references, cursor.token)
	}
	for i, cursor := range cursors {
		modified := modifyToken(cursor.token, references)
		cursors[i].modified = modified
		references = append(references, modified)
	}
	return references
}

func modifyToken(token string, reserved []string) string {
	// Preserve token length when possible so format validation alone is
	// insufficient to reject the modified token.
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	middle := len(token) / 2
	if strings.IndexByte(alphabet, token[middle]) >= 0 {
		for _, c := range alphabet {
			candidate := token[:middle] + string(c) + token[middle+1:]
			if !slices.Contains(reserved, candidate) {
				return candidate
			}
		}
	}
	for candidate := token + "A"; ; candidate += "A" {
		if !slices.Contains(reserved, candidate) {
			return candidate
		}
	}
}

func (s *scenario) checkQueries(ctx context.Context) error {
	if err := s.checkFullQueries(ctx); err != nil {
		return err
	}
	cursors, err := s.issueCursors(ctx)
	if err != nil {
		return err
	}
	references := invalidTokens(cursors)
	denied := rejection{status: http.StatusForbidden}
	invalid := rejection{status: http.StatusBadRequest}
	for _, target := range s.targets {
		if !target.grant.Query {
			if err := s.rejectQuery(ctx, target, "", &denied, references); err != nil {
				return fmt.Errorf("query authorization before validation (initial, %s): %w", target.label(), err)
			}
		}
	}
	for _, target := range s.targets {
		for _, cursor := range cursors {
			if err := s.checkContinuation(ctx, target, cursor, &denied, &invalid, references); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *scenario) rejectQuery(ctx context.Context, target matrixTarget, token string, expected *rejection, references []string) error {
	page, err := s.query(ctx, QueryRequest{Target: target.grant.Target, Limit: 1, Cursor: token}, expected.status)
	if err != nil {
		return err
	}
	return s.checkRejection(page.Response, expected, references)
}

func (s *scenario) checkContinuation(ctx context.Context, target matrixTarget, cursor cursorCase, denied, invalid *rejection, references []string) error {
	if !target.grant.Query {
		for _, probe := range []string{cursor.token, cursor.modified} {
			if err := s.rejectQuery(ctx, target, probe, denied, references); err != nil {
				return fmt.Errorf("query authorization before validation (source %s, target %s): %w", cursor.issuer.label(), target.label(), err)
			}
		}
		return nil
	}

	if err := s.rejectQuery(ctx, target, cursor.modified, invalid, references); err != nil {
		return fmt.Errorf("modified cursor (source %s, target %s): %w", cursor.issuer.label(), target.label(), err)
	}
	if target.grant.Target != cursor.issuer.grant.Target {
		if err := s.rejectQuery(ctx, target, cursor.token, invalid, references); err != nil {
			return fmt.Errorf("authorized cursor scope mismatch (source %s, target %s): %w", cursor.issuer.label(), target.label(), err)
		}
		return nil
	}

	last, err := s.query(ctx, QueryRequest{Target: target.grant.Target, Limit: 1, Cursor: cursor.token}, http.StatusOK)
	if err == nil {
		err = s.terminalRows(last, cursor.remaining)
	}
	if err != nil {
		return fmt.Errorf("issuer resumes cursor (%s): %w", target.label(), err)
	}
	return nil
}
