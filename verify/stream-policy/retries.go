package streampolicy

import (
	"context"
	"fmt"
	"net/http"
)

const (
	unusedKey = "fixture-unused-retry-key"
	failedKey = "fixture-failed-key"
)

func (s *scenario) ingest(ctx context.Context, request IngestRequest, status int) (Response, error) {
	response, err := s.Retry.Ingest(ctx, request)
	if err := observation(ctx, err); err != nil {
		return Response{}, err
	}
	if err := requireResponse(response, status); err != nil {
		return Response{}, err
	}
	markers := s.denialMarkers
	if status == http.StatusNoContent {
		markers = s.scopedMarkers(s.streamForSelector(request.Selector).Scope)
	}
	return response, inspectResponse(response, markers)
}

func (s *scenario) checkIngest(ctx context.Context) error {
	for i, entry := range s.appends {
		response, err := s.ingest(ctx, entry.request, http.StatusNoContent)
		if err != nil {
			return fmt.Errorf("retry namespace %d: first submission: %w", i+1, err)
		}
		if err := s.checkRecordedRetry(ctx, entry.request, response); err != nil {
			return fmt.Errorf("retry namespace %d: %w", i+1, err)
		}
		if err := s.checkConflictingRetry(ctx, entry.request); err != nil {
			return fmt.Errorf("retry namespace %d: %w", i+1, err)
		}
		s.outcomes = append(s.outcomes, response)
	}

	if err := s.checkDeniedRetries(ctx); err != nil {
		return err
	}
	request := s.appends[0].request
	request.Key = failedKey
	const ts = "2026-01-01T00:00:05Z"
	request.Body = ndjson(ts, uncommitted+" "+plain) + ndjson(ts, failed)
	for range 2 {
		if _, err := s.ingest(ctx, request, http.StatusServiceUnavailable); err != nil {
			return fmt.Errorf("redaction must fail the whole batch: %w", err)
		}
	}
	return nil
}

func (s *scenario) checkRecordedRetry(ctx context.Context, request IngestRequest, first Response) error {
	retry, err := s.ingest(ctx, request, http.StatusNoContent)
	if err != nil {
		return fmt.Errorf("identical retry: %w", err)
	}
	if !sameResponse(retry, first) {
		return fmt.Errorf("%w: retry response differs from the committed outcome", ErrViolation)
	}
	return nil
}

func changedRetry(request IngestRequest) IngestRequest {
	request.Body += ndjson("2026-01-01T00:00:06Z", uncommitted)
	return request
}

func (s *scenario) checkConflictingRetry(ctx context.Context, request IngestRequest) error {
	if _, err := s.ingest(ctx, changedRetry(request), http.StatusConflict); err != nil {
		return fmt.Errorf("changed retry: %w", err)
	}
	return nil
}

func (s *scenario) checkDeniedRetries(ctx context.Context) error {
	denied := rejection{status: http.StatusForbidden}
	for _, target := range s.targets {
		if target.grant.Ingest {
			continue
		}
		for i, entry := range s.appends {
			exact := entry.request
			exact.Target = target.grant.Target
			fresh := exact
			fresh.Key = unusedKey
			for _, probe := range []struct {
				name    string
				request IngestRequest
			}{
				{"recorded body", exact},
				{"changed body", changedRetry(exact)},
				{"unused key", fresh},
			} {
				response, err := s.Retry.Ingest(ctx, probe.request)
				if err := observation(ctx, err); err != nil {
					return fmt.Errorf("ingest denied (%s, source %d, %s): %w", target.label(), i+1, probe.name, err)
				}
				if err := s.checkRejection(response, &denied, nil); err != nil {
					return fmt.Errorf("ingest denied (%s, source %d, %s): %w", target.label(), i+1, probe.name, err)
				}
			}
		}
	}
	return nil
}
