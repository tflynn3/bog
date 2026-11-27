package streampolicy

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
)

type scenario struct {
	Adapters
	fixture       Fixture
	appends       []appendCase
	targets       []matrixTarget
	committed     []Record
	denialMarkers []string
	forbidden     []string
	outcomes      []Response
}

// Run executes the reference scenario on a freshly provisioned target. A nil
// error means that all positive controls and negative probes were observed.
func Run(ctx context.Context, adapters Adapters) error {
	if adapters.Query == nil || adapters.Retry == nil || adapters.Replay == nil || adapters.Export == nil {
		return fmt.Errorf("%w: all four adapters are required", ErrIncomplete)
	}
	s := newScenario(adapters)
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

func newScenario(adapters Adapters) scenario {
	fixture := ReferenceFixture()
	s := scenario{
		Adapters: adapters, fixture: fixture, appends: fixtureAppends(fixture), targets: fixture.targets(),
		forbidden: []string{plain, escaped, failed, uncommitted},
	}
	for _, entry := range s.appends {
		for _, row := range entry.rows {
			s.committed = append(s.committed, Record{s.streamForSelector(entry.request.Selector).Scope, row})
		}
	}
	s.denialMarkers = s.rejectionMarkers()
	return s
}

func observation(ctx context.Context, err error) error {
	if cause := ctx.Err(); cause != nil {
		return errors.Join(ErrIncomplete, cause)
	}
	if violation, ok := err.(candidateViolation); ok {
		return violation
	}
	if err != nil {
		// Adapter errors can contain request contents, URLs, or cursor tokens.
		return fmt.Errorf("%w: adapter could not complete observation", ErrIncomplete)
	}
	return nil
}

func (s *scenario) forScope(scope Scope) (records []Record, rows []Row) {
	for _, record := range s.committed {
		if record.Scope == scope {
			records = append(records, record)
			rows = append(rows, record.Row)
		}
	}
	return records, rows
}

func (s *scenario) scopedMarkers(scope Scope) []string {
	forbidden := slices.Clone(s.forbidden)
	for _, record := range s.committed {
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

func (s *scenario) streamForSelector(selector string) Stream {
	for _, stream := range s.fixture.Streams {
		if stream.Selector == selector {
			return stream
		}
	}
	return Stream{}
}

func (s *scenario) rejectionMarkers() []string {
	markers := append(slices.Clone(s.forbidden), key, unusedKey, failedKey, unknown)
	for _, grant := range s.fixture.Grants {
		if !slices.Contains(markers, grant.Principal) {
			markers = append(markers, grant.Principal)
		}
	}
	for _, stream := range s.fixture.Streams {
		markers = append(markers, stream.Selector, stream.Scope.Tenant, stream.Scope.Stream, stream.PolicyVersion)
	}
	for _, record := range s.committed {
		retained, _, _ := strings.Cut(record.Row.Line, " ")
		markers = append(markers, retained)
	}
	return markers
}

func (s *scenario) checkReplay(ctx context.Context) error {
	evidence, err := s.Replay.RestartAndReplay(ctx)
	if err := observation(ctx, err); err != nil {
		return err
	}
	if !evidence.Complete || evidence.Records == nil {
		return ErrIncomplete
	}
	return inspectRecords(evidence.Bytes, evidence.Records, s.committed, s.forbidden)
}

func (s *scenario) checkReopened(ctx context.Context) error {
	if err := s.checkQueries(ctx); err != nil {
		return err
	}
	for i, entry := range s.appends {
		if err := s.checkRecordedRetry(ctx, entry.request, s.outcomes[i]); err != nil {
			return fmt.Errorf("reopened retry namespace %d returned a different outcome: %w", i+1, err)
		}
		if err := s.checkConflictingRetry(ctx, entry.request); err != nil {
			return fmt.Errorf("reopened retry namespace %d: %w", i+1, err)
		}
	}
	if err := s.checkDeniedRetries(ctx); err != nil {
		return err
	}
	return s.checkFullQueries(ctx)
}
