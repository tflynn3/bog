package streampolicy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
)

type scenario struct {
	Adapters
	fixture   Fixture
	appends   []appendCase
	forbidden []string
	outcomes  []Response
}

// Run executes the reference scenario on a freshly provisioned target. A nil
// error means that all positive controls and negative probes were observed.
func Run(ctx context.Context, adapters Adapters) error {
	if adapters.Query == nil || adapters.Retry == nil || adapters.Replay == nil || adapters.Export == nil {
		return fmt.Errorf("%w: all four adapters are required", ErrIncomplete)
	}
	fixture := ReferenceFixture()
	s := scenario{
		Adapters: adapters, fixture: fixture, appends: fixtureAppends(fixture),
		forbidden: []string{plain, escaped, failed, uncommitted},
	}
	for _, step := range []struct {
		name string
		run  func(context.Context) error
	}{
		{"retry namespaces and failed append", s.checkIngest},
		{"authorization and cursor scope", s.checkQueries},
		{"committed bytes and replay", s.checkReplay},
		{"reopened query and retry", s.checkReopened},
		{"scoped export", s.checkExports},
		{"final committed bytes and replay", s.checkReplay},
	} {
		if err := observation(ctx, nil); err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
		if err := step.run(ctx); err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
	}
	return observation(ctx, nil)
}

func observation(ctx context.Context, err error) error {
	if cause := ctx.Err(); cause != nil {
		return errors.Join(ErrIncomplete, cause)
	}
	if err != nil {
		// Adapter errors can contain request contents, URLs, or cursor tokens.
		return fmt.Errorf("%w: adapter could not complete observation", ErrIncomplete)
	}
	return nil
}

func (s *scenario) records() []Record {
	records := make([]Record, 0, len(s.appends))
	for _, entry := range s.appends {
		records = append(records, entry.record)
	}
	return records
}

func (s *scenario) forScope(scope Scope) (records []Record, rows []Row) {
	for _, entry := range s.appends {
		if entry.record.Scope == scope {
			records = append(records, entry.record)
			rows = append(rows, entry.record.Row)
		}
	}
	return records, rows
}

func (s *scenario) publicResponse(response Response, status int) error {
	if err := requireResponse(response, status); err != nil {
		return err
	}
	return inspectResponse(response, s.forbidden)
}

func (s *scenario) scopedMarkers(scope Scope) []string {
	forbidden := slices.Clone(s.forbidden)
	for _, entry := range s.appends {
		if entry.record.Scope != scope {
			retained, _, _ := strings.Cut(entry.record.Row.Line, " ")
			forbidden = append(forbidden, retained)
		}
	}
	for _, other := range s.fixture.Streams {
		if other.Scope != scope {
			forbidden = append(forbidden, other.PolicyVersion, other.Selector)
			if other.Scope.Tenant != scope.Tenant {
				forbidden = append(forbidden, other.Scope.Tenant)
			}
			if other.Scope.Stream != scope.Stream {
				forbidden = append(forbidden, other.Scope.Stream)
			}
		}
	}
	return forbidden
}

func (s *scenario) scopedMarkersForSelector(selector string) []string {
	for _, stream := range s.fixture.Streams {
		if stream.Selector == selector {
			return s.scopedMarkers(stream.Scope)
		}
	}
	return s.forbidden
}

func (s *scenario) deniedResponse(response Response, baseline Response, extra ...string) error {
	if err := s.publicResponse(response, baseline.Status); err != nil {
		return err
	}
	markers := append([]string{alice, bob, exporter, key, unknown}, extra...)
	for _, stream := range s.fixture.Streams {
		markers = append(markers, stream.Selector, stream.Scope.Tenant, stream.Scope.Stream, stream.PolicyVersion)
	}
	for _, entry := range s.appends {
		retained, _, _ := strings.Cut(entry.record.Row.Line, " ")
		markers = append(markers, retained)
	}
	if err := inspectResponse(response, markers); err != nil {
		return err
	}
	if !sameResponse(response, baseline) {
		return fmt.Errorf("%w: rejection exposes different response evidence", ErrViolation)
	}
	return nil
}

func (s *scenario) ingest(ctx context.Context, request IngestRequest, status int) (Response, error) {
	resp, err := s.Retry.Ingest(ctx, request)
	if err := observation(ctx, err); err != nil {
		return Response{}, err
	}
	if err := requireResponse(resp, status); err != nil {
		return resp, err
	}
	markers := s.forbidden
	if status != http.StatusForbidden {
		markers = s.scopedMarkersForSelector(request.Selector)
	}
	return resp, inspectResponse(resp, markers)
}

func (s *scenario) checkIngest(ctx context.Context) error {
	for i, entry := range s.appends {
		response, err := s.checkRetry(ctx, entry)
		if err != nil {
			return fmt.Errorf("retry namespace %d: %w", i+1, err)
		}
		s.outcomes = append(s.outcomes, response)
	}
	base := s.appends[0].request
	base.Principal = exporter // Export authority is independent of ingest.
	denied, err := s.ingest(ctx, base, http.StatusForbidden)
	if err != nil {
		return fmt.Errorf("ingest denied: %w", err)
	}
	for _, target := range []Target{base.Target, {bob, s.fixture.Streams[1].Selector}, {bob, unknown}} {
		request := base
		request.Target = target
		resp, err := s.ingest(ctx, request, http.StatusForbidden)
		if err != nil {
			return err
		}
		if err := s.deniedResponse(resp, denied); err != nil {
			return err
		}
	}

	request := s.appends[0].request
	request.Key = "fixture-failed-key"
	const ts = "2026-01-01T00:00:05Z"
	request.Body = ndjson(ts, uncommitted+" "+plain) + ndjson(ts, failed)
	for range 2 {
		if _, err := s.ingest(ctx, request, http.StatusServiceUnavailable); err != nil {
			return fmt.Errorf("redaction must fail the whole batch: %w", err)
		}
	}
	return nil
}

func (s *scenario) checkRetry(ctx context.Context, entry appendCase) (Response, error) {
	first, err := s.ingest(ctx, entry.request, http.StatusNoContent)
	if err != nil {
		return Response{}, fmt.Errorf("first submission: %w", err)
	}
	retry, err := s.ingest(ctx, entry.request, http.StatusNoContent)
	if err != nil {
		return Response{}, fmt.Errorf("identical retry: %w", err)
	}
	if !sameResponse(retry, first) {
		return Response{}, fmt.Errorf("%w: retry response differs from the committed outcome", ErrViolation)
	}
	conflict := entry.request
	conflict.Body += ndjson("2026-01-01T00:00:06Z", uncommitted)
	if _, err := s.ingest(ctx, conflict, http.StatusConflict); err != nil {
		return Response{}, fmt.Errorf("changed retry: %w", err)
	}
	return first, nil
}

func (s *scenario) query(ctx context.Context, request QueryRequest, status int) (Page, error) {
	request.Start, request.End = s.fixture.QueryStart, s.fixture.QueryEnd
	page, err := s.Query.Query(ctx, request)
	if err := observation(ctx, err); err != nil {
		return Page{}, err
	}
	if err := requireResponse(page.Response, status); err != nil {
		return Page{}, err
	}
	markers := s.forbidden
	if status == http.StatusOK {
		markers = s.scopedMarkersForSelector(request.Selector)
	}
	if err := inspectResponse(page.Response, markers); err != nil {
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
	if status == http.StatusOK && len(page.Rows) != 0 && len(page.Body) == 0 {
		return Page{}, fmt.Errorf("%w: query rows lack response bytes", ErrIncomplete)
	}
	return page, nil
}

func (s *scenario) checkQueries(ctx context.Context) error {
	if err := s.checkFullQueries(ctx); err != nil {
		return err
	}
	a, b, c := s.fixture.Streams[0], s.fixture.Streams[1], s.fixture.Streams[2]
	_, rows := s.forScope(a.Scope)
	request := QueryRequest{Target: Target{alice, a.Selector}, Limit: 1}
	first, err := s.query(ctx, request, http.StatusOK)
	if err != nil {
		return fmt.Errorf("issue cursor: %w", err)
	}
	if err := compareRows(first.Rows, rows[:1]); err != nil {
		return err
	}
	if first.NextCursor == "" {
		return fmt.Errorf("%w: opening page has no continuation", ErrIncomplete)
	}
	valid := first.NextCursor
	mid := len(valid) / 2
	tampered := "A"
	if valid[mid] == 'A' {
		tampered = "Z"
	}
	modified := valid[:mid] + tampered + valid[mid+1:]
	denied, err := s.query(ctx, QueryRequest{Target: Target{bob, b.Selector}, Limit: 1}, http.StatusForbidden)
	if err != nil {
		return err
	}
	for _, target := range []Target{{bob, b.Selector}, {bob, c.Selector}, {bob, unknown}, {exporter, a.Selector}} {
		for _, cursor := range []string{"", valid, modified} {
			got, err := s.query(ctx, QueryRequest{Target: target, Limit: 1, Cursor: cursor}, http.StatusForbidden)
			if err != nil {
				return fmt.Errorf("query authorization before validation: %w", err)
			}
			if err := s.deniedResponse(got.Response, denied.Response, valid, modified); err != nil {
				return err
			}
		}
	}

	request.Cursor = modified
	invalid, err := s.query(ctx, request, http.StatusBadRequest)
	if err != nil {
		return fmt.Errorf("modified cursor: %w", err)
	}
	// Each target already has query access to the requested stream. Change
	// one component of the issuer's scope per probe.
	for _, target := range []Target{{alice, b.Selector}, {alice, c.Selector}, {bob, a.Selector}} {
		got, err := s.query(ctx, QueryRequest{Target: target, Limit: 1, Cursor: valid}, http.StatusBadRequest)
		if err != nil {
			return fmt.Errorf("authorized cursor scope mismatch: %w", err)
		}
		if err := s.deniedResponse(got.Response, invalid.Response, valid, modified); err != nil {
			return err
		}
	}
	request.Cursor = valid
	last, err := s.query(ctx, request, http.StatusOK)
	if err != nil {
		return fmt.Errorf("issuer resumes cursor: %w", err)
	}
	return terminalRows(last, rows[1:])
}

func terminalRows(page Page, want []Row) error {
	if page.NextCursor != "" {
		return fmt.Errorf("%w: expected a terminal fixture page", ErrViolation)
	}
	return compareRows(page.Rows, want)
}

func (s *scenario) checkFullQueries(ctx context.Context) error {
	for i, stream := range s.fixture.Streams {
		_, rows := s.forScope(stream.Scope)
		request := QueryRequest{Target: Target{alice, stream.Selector}, Limit: 10}
		page, err := s.query(ctx, request, http.StatusOK)
		if err != nil {
			return fmt.Errorf("authorized query in stream %d: %w", i+1, err)
		}
		if err := terminalRows(page, rows); err != nil {
			return fmt.Errorf("authorized query in stream %d: %w", i+1, err)
		}
		request.Contains = replacement
		page, err = s.query(ctx, request, http.StatusOK)
		if err != nil {
			return fmt.Errorf("query for retained replacement in stream %d: %w", i+1, err)
		}
		if err := terminalRows(page, rows); err != nil {
			return fmt.Errorf("query for retained replacement in stream %d: %w", i+1, err)
		}
		for _, marker := range s.forbidden {
			request.Contains = marker
			page, err := s.query(ctx, request, http.StatusOK)
			if err != nil {
				return fmt.Errorf("query for forbidden marker in stream %d: %w", i+1, err)
			}
			if err := terminalRows(page, nil); err != nil {
				return fmt.Errorf("query for forbidden marker in stream %d: %w", i+1, err)
			}
		}
	}
	bobPage, err := s.query(ctx, QueryRequest{Target: Target{bob, s.fixture.Streams[0].Selector}, Limit: 10}, http.StatusOK)
	if err != nil {
		return fmt.Errorf("second principal's authorized query: %w", err)
	}
	_, rows := s.forScope(s.fixture.Streams[0].Scope)
	return terminalRows(bobPage, rows)
}

func (s *scenario) checkReplay(ctx context.Context) error {
	evidence, err := s.Replay.RestartAndReplay(ctx)
	if err := observation(ctx, err); err != nil {
		return err
	}
	if !evidence.Complete || evidence.Records == nil {
		return ErrIncomplete
	}
	return inspectRecords(evidence.Bytes, evidence.Records, s.records(), s.forbidden)
}

func (s *scenario) checkReopened(ctx context.Context) error {
	if err := s.checkFullQueries(ctx); err != nil {
		return err
	}
	for i, entry := range s.appends {
		response, err := s.ingest(ctx, entry.request, http.StatusNoContent)
		if err != nil {
			return fmt.Errorf("reopened retry namespace %d: %w", i+1, err)
		}
		if !sameResponse(response, s.outcomes[i]) {
			return fmt.Errorf("%w: reopened retry namespace %d returned a different outcome", ErrViolation, i+1)
		}
	}
	return s.checkFullQueries(ctx)
}
