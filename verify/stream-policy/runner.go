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

type scenario struct {
	Adapters
	fixture     Fixture
	matrix      []policyCell
	exportState ExportStateObserver
	appends     []appendCase
	forbidden   []string
	outcomes    []Response
	committed   Committed
}

// Run executes the reference scenario on a freshly provisioned target. A nil
// error means that all positive controls and negative probes were observed.
func Run(ctx context.Context, adapters Adapters) error {
	if adapters.Query == nil || adapters.Retry == nil || adapters.Replay == nil || adapters.Export == nil {
		return fmt.Errorf("%w: all four adapters are required", ErrIncomplete)
	}
	exportState, ok := adapters.Export.(ExportStateObserver)
	if !ok {
		return fmt.Errorf("%w: export state observer is required", ErrIncomplete)
	}
	fixture := ReferenceFixture()
	s := scenario{
		Adapters: adapters, fixture: fixture, appends: fixtureAppends(fixture),
		matrix: policyMatrix(fixture), exportState: exportState,
		forbidden: []string{plain, escaped, failed, uncommitted},
	}
	for _, step := range []struct {
		name string
		run  func(context.Context) error
	}{
		{"retry namespaces and failed append", s.checkIngest},
		{"authorization and cursor scope", s.checkFullQueries},
		{"committed bytes and replay", s.captureCommitted},
		{"reopened query and retry", s.checkReopened},
		{"scoped export and authorization matrix", s.checkMatrix},
		{"committed state after denied operations", s.checkCommittedUnchanged},
		{"reopened controls after denied operations", s.checkReopened},
		{"final committed bytes and replay", s.checkCommittedUnchanged},
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
		records = append(records, entry.records...)
	}
	return records
}

func (s *scenario) forScope(scope Scope) (records []Record, rows []Row) {
	for _, record := range s.records() {
		if record.Scope == scope {
			records = append(records, record)
			rows = append(rows, record.Row)
		}
	}
	return records, rows
}

func (s *scenario) scopedMarkers(scope Scope) []string {
	forbidden := slices.Clone(s.forbidden)
	for _, record := range s.records() {
		if record.Scope != scope {
			retained, _, _ := strings.Cut(record.Row.Line, " ")
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

func (s *scenario) denialMarkers() []string {
	markers := append(slices.Clone(s.forbidden), alice, bob, exporter, key, unknown, freshRetryKey)
	for _, stream := range s.fixture.Streams {
		markers = append(markers, stream.Selector, stream.Scope.Tenant, stream.Scope.Stream, stream.PolicyVersion)
	}
	for _, record := range s.records() {
		retained, _, _ := strings.Cut(record.Row.Line, " ")
		markers = append(markers, retained)
	}
	return markers
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
	conflict := conflictingRetry(entry.request)
	if _, err := s.ingest(ctx, conflict, http.StatusConflict); err != nil {
		return Response{}, fmt.Errorf("changed retry: %w", err)
	}
	return first, nil
}

func conflictingRetry(request IngestRequest) IngestRequest {
	request.Body += ndjson("2026-01-01T00:00:07Z", uncommitted)
	return request
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
	if status == http.StatusOK {
		if err := checkQueryBody(page, request.Limit); err != nil {
			return Page{}, err
		}
	}
	return page, nil
}

// Match the decoded rows to the original query NDJSON. The observer checks
// HTTP framing, while the verifier checks what those bytes actually expose.
func checkQueryBody(page Page, limit int) error {
	if len(page.Rows) > limit {
		return fmt.Errorf("%w: query exceeded its page limit", ErrViolation)
	}
	if len(page.Rows) != 0 && len(page.Body) == 0 {
		return fmt.Errorf("%w: query rows lack response bytes", ErrIncomplete)
	}
	decoder := json.NewDecoder(bytes.NewReader(page.Body))
	decoder.DisallowUnknownFields()
	for i, reported := range page.Rows {
		var row Row
		if err := decoder.Decode(&row); err != nil {
			return fmt.Errorf("%w: invalid query response row %d", ErrViolation, i+1)
		}
		if row != reported {
			return fmt.Errorf("%w: query response bytes differ from observed row %d", ErrViolation, i+1)
		}
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("%w: query response contains unreported bytes", ErrViolation)
	}
	return nil
}

func terminalRows(page Page, want []Row) error {
	if page.NextCursor != "" {
		return fmt.Errorf("%w: expected a terminal fixture page", ErrViolation)
	}
	return compareRows(page.Rows, want)
}

func (s *scenario) checkFullQueries(ctx context.Context) error {
	for _, cell := range s.matrix {
		if !cell.Query {
			continue
		}
		if err := s.checkFullQuery(ctx, cell); err != nil {
			return fmt.Errorf("authorized query in stream %d: %w", cell.streamIndex+1, err)
		}
	}
	return nil
}

func (s *scenario) checkFullQuery(ctx context.Context, cell policyCell) error {
	_, rows := s.forScope(cell.stream.Scope)
	request := QueryRequest{Target: cell.Target, Limit: fixtureQueryLimit}
	filters := append([]string{"", replacement}, s.forbidden...)
	for _, record := range s.records() {
		if record.Scope != cell.stream.Scope {
			filters = append(filters, record.Row.Line)
		}
	}
	for _, filter := range filters {
		request.Contains = filter
		page, err := s.query(ctx, request, http.StatusOK)
		if err != nil {
			return err
		}
		want := rows
		if filter != "" && filter != replacement {
			want = nil
		}
		if err := terminalRows(page, want); err != nil {
			return err
		}
	}
	return nil
}

func (s *scenario) observeCommitted(ctx context.Context) (Committed, error) {
	evidence, err := s.Replay.RestartAndReplay(ctx)
	if err := observation(ctx, err); err != nil {
		return Committed{}, err
	}
	if !evidence.Complete || evidence.Records == nil {
		return Committed{}, ErrIncomplete
	}
	if err := inspectRecords(evidence.Bytes, evidence.Records, s.records(), s.forbidden); err != nil {
		return Committed{}, err
	}
	return evidence, nil
}

func (s *scenario) captureCommitted(ctx context.Context) error {
	evidence, err := s.observeCommitted(ctx)
	if err != nil {
		return err
	}
	s.committed = Committed{Complete: true, Bytes: bytes.Clone(evidence.Bytes), Records: slices.Clone(evidence.Records)}
	return nil
}

func (s *scenario) checkCommittedUnchanged(ctx context.Context) error {
	evidence, err := s.observeCommitted(ctx)
	if err != nil {
		return err
	}
	if !bytes.Equal(evidence.Bytes, s.committed.Bytes) || !slices.Equal(evidence.Records, s.committed.Records) {
		return fmt.Errorf("%w: committed state changed after denied operations and read-only controls", ErrViolation)
	}
	return nil
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
